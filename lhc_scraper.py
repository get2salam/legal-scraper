#!/usr/bin/env python3
"""
LHC Scraper - Lahore High Court Reported Judgments
====================================================
Scrapes reported judgments from data.lhc.gov.pk

Two data sources:
  1. AJAX API at data.lhc.gov.pk/dynamic/approved_judgments_result_new.php
     (sitting judges) and approved_judgments_result_former_judges.php (former)
  2. Direct PDF download from sys.lhc.gov.pk/appjudgments/{YEAR}LHC{NUMBER}.pdf

Benches: Lahore (principal), Rawalpindi, Multan, Bahawalpur

Usage:
    python lhc_scraper.py                     # All years, all sources
    python lhc_scraper.py --year 2024         # Specific year
    python lhc_scraper.py --discover          # Discover API endpoints & test connectivity
    python lhc_scraper.py --status            # Show progress
    python lhc_scraper.py --source sitting    # Only sitting judges
    python lhc_scraper.py --source former     # Only former judges
    python lhc_scraper.py --enumerate         # Enumerate PDFs by known URL pattern
    python lhc_scraper.py --limit 10          # Download first N per year
    python lhc_scraper.py --judge "Mr. Justice Muzamil Akhtar Shabir"  # Specific judge

INTERNAL USE ONLY - never push to public GitHub.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import random
import hashlib
import html as html_mod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field, asdict

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests import Session, BrowserType
except ImportError:
    print("ERROR: curl_cffi required. Install: pip install curl_cffi")
    sys.exit(1)

from bs4 import BeautifulSoup, Tag

# PDF text extraction
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# ==============================================================================
# Configuration
# ==============================================================================

# LHC Domains
DATA_DOMAIN = "https://data.lhc.gov.pk"
SYS_DOMAIN = "https://sys.lhc.gov.pk"
MAIN_DOMAIN = "https://lhc.gov.pk"

# AJAX API endpoints (discovered from Wayback Machine analysis)
SITTING_JUDGES_API = f"{DATA_DOMAIN}/dynamic/approved_judgments_result_new.php"
FORMER_JUDGES_API = f"{DATA_DOMAIN}/dynamic/approved_judgments_result_former_judges.php"

# PDF base URL pattern: {YEAR}LHC{NUMBER}.pdf
PDF_BASE = f"{SYS_DOMAIN}/appjudgments"

# Pages
SITTING_PAGE = f"{DATA_DOMAIN}/reported_judgments/judgments_approved_for_reporting"
FORMER_PAGE = f"{DATA_DOMAIN}/reported_judgments/judgments_approved_for_reporting_by_former_judges"

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
COURT_DIR = DATA_ROOT / "court_cases"
LHC_DIR = COURT_DIR / "LHC"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "LHC"
PROGRESS_FILE = COURT_DIR / "lhc_progress.json"
ALL_JSONL = COURT_DIR / "all_court_cases.jsonl"

# Year range (data portal has 2010-2025)
YEAR_START = 2010
YEAR_END = datetime.now().year + 1

# Rate limiting - be respectful to government servers
MIN_DELAY = 5.0
MAX_DELAY = 10.0
PAGE_TIMEOUT = 60
PDF_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF = 30

# LHC Benches
BENCHES = {
    "LHR": ["Lahore", "at Lahore", "Lahore High Court Lahore"],
    "RWP": ["Rawalpindi", "at Rawalpindi", "Rawalpindi Bench"],
    "MUL": ["Multan", "at Multan", "Multan Bench"],
    "BWP": ["Bahawalpur", "at Bahawalpur", "Bahawalpur Bench"],
}

# Logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "lhc_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("lhc_scraper")


# ==============================================================================
# Data Classes
# ==============================================================================

@dataclass
class LHCCase:
    """Represents a single LHC reported judgment."""
    source: str = "LHC"
    source_type: str = ""        # "sitting" or "former" judge
    citation: str = ""           # e.g. "2024 LHC 3881"
    citation_year: str = ""
    citation_number: str = ""
    case_type: str = ""          # e.g. "Writ Petition", "Crl. Appeal"
    case_number: str = ""        # Full case number e.g. "46054/24"
    parties: Dict[str, str] = field(default_factory=dict)
    title: str = ""              # Full case title
    judge: str = ""              # Judge name
    tagline: str = ""            # Summary tagline
    upload_date: str = ""        # When uploaded to portal
    decision_date: str = ""
    bench: str = ""              # LHR, RWP, MUL, BWP
    bench_name: str = ""         # Full bench name
    pdf_url: str = ""            # Direct PDF URL
    pdf_filename: str = ""       # e.g. "2024LHC3881.pdf"
    judgment_text: str = ""      # Extracted text from PDF
    fetched_at: str = ""
    source_url: str = ""
    serial: int = 0              # Serial in listing

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v or isinstance(v, (bool, int))}

    @property
    def file_key(self) -> str:
        """Generate unique file key from citation."""
        if self.citation_year and self.citation_number:
            return f"LHC_{self.citation_year}_{self.citation_number}"
        if self.pdf_filename:
            name = self.pdf_filename.replace(".pdf", "")
            return f"LHC_{name}"
        return f"LHC_unknown_{self.serial}"

    @property
    def year(self) -> str:
        if self.citation_year:
            return self.citation_year
        if self.upload_date:
            # Try extracting year from upload date
            m = re.search(r"\d{4}", self.upload_date)
            if m:
                return m.group()
        return "unknown"


# ==============================================================================
# HTTP Session
# ==============================================================================

class LHCSession:
    """HTTP session with Chrome TLS impersonation for LHC websites."""

    def __init__(self):
        self.session = Session(impersonate="chrome")
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        self.request_count = 0
        self._connectivity = {}  # domain -> bool

    def _delay(self, min_d: float = None, max_d: float = None):
        """Human-like delay between requests."""
        lo = min_d or MIN_DELAY
        hi = max_d or MAX_DELAY
        delay = random.uniform(lo, hi)
        # Occasional longer pauses
        if self.request_count > 0 and self.request_count % 30 == 0:
            delay += random.uniform(10, 30)
            logger.info(f"Breather after {self.request_count} requests ({delay:.1f}s)")
        time.sleep(delay)
        self.request_count += 1

    def check_connectivity(self, domain: str) -> bool:
        """Test if a domain is reachable (not blocked by FortiGuard)."""
        if domain in self._connectivity:
            return self._connectivity[domain]

        try:
            r = self.session.get(domain, timeout=15)
            # FortiGuard returns 403 with specific HTML
            if r.status_code == 403 and "FortiGuard" in r.text:
                logger.warning(f"{domain} blocked by FortiGuard firewall")
                self._connectivity[domain] = False
                return False
            if r.status_code in (200, 301, 302):
                self._connectivity[domain] = True
                return True
            # Check for FortiGuard block page content
            if "fortinet" in r.text.lower() or "fortigate" in r.text.lower():
                logger.warning(f"{domain} blocked by Fortinet firewall")
                self._connectivity[domain] = False
                return False
            self._connectivity[domain] = True
            return True
        except Exception as e:
            logger.warning(f"{domain} unreachable: {e}")
            self._connectivity[domain] = False
            return False

    def get(self, url: str, timeout: int = 30, retries: int = None, **kwargs) -> Optional[Any]:
        """GET with retry logic."""
        max_r = retries if retries is not None else MAX_RETRIES
        for attempt in range(max_r):
            try:
                if attempt > 0:
                    backoff = RETRY_BACKOFF * attempt
                    logger.warning(f"Retry {attempt}/{max_r} after {backoff}s")
                    time.sleep(backoff)

                r = self.session.get(url, timeout=timeout, **kwargs)

                if r.status_code == 200:
                    return r
                elif r.status_code == 403:
                    # Check for firewall block
                    if "fortinet" in r.text.lower() or "fortigate" in r.text.lower() or "FortiGuard" in r.text:
                        logger.error(f"Blocked by firewall: {url}")
                        return None
                    logger.warning(f"HTTP 403 for {url}")
                    return r
                elif r.status_code == 429:
                    logger.warning("Rate limited (429). Backing off 60s...")
                    time.sleep(60)
                elif r.status_code >= 500:
                    logger.warning(f"Server error {r.status_code}. Will retry...")
                else:
                    logger.warning(f"HTTP {r.status_code} for {url}")
                    return r
            except Exception as e:
                logger.error(f"Request failed: {type(e).__name__}: {e}")
                if attempt == max_r - 1:
                    return None
        return None

    def get_ajax(self, url: str, params: Dict, timeout: int = PAGE_TIMEOUT) -> Optional[str]:
        """GET an AJAX endpoint, return HTML content."""
        self._delay()
        # Build URL with params
        param_str = "&".join(f"{k}={v}" for k, v in params.items())
        full_url = f"{url}?{param_str}" if params else url

        # Set AJAX headers
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "text/html, */*; q=0.01",
            "Referer": SITTING_PAGE,
        }

        try:
            r = self.session.get(full_url, timeout=timeout, headers=headers)
            if r and r.status_code == 200:
                return r.text
            else:
                logger.error(f"AJAX failed: {r.status_code if r else 'no response'}")
                return None
        except Exception as e:
            logger.error(f"AJAX error: {e}")
            return None

    def download_pdf(self, url: str, dest: Path) -> bool:
        """Download a PDF file."""
        self._delay(3, 7)
        try:
            r = self.session.get(url, timeout=PDF_TIMEOUT)
            if r and r.status_code == 200:
                ct = r.headers.get("Content-Type", "")
                if "pdf" in ct.lower() or r.content[:5] == b"%PDF-":
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(r.content)
                    logger.info(f"Downloaded PDF: {dest.name} ({len(r.content):,} bytes)")
                    return True
                else:
                    logger.warning(f"Not a PDF: {url} (Content-Type: {ct})")
                    # Could be firewall block
                    if b"fortinet" in r.content.lower() or b"fortigate" in r.content.lower():
                        logger.error("PDF download blocked by firewall")
                    return False
            else:
                logger.warning(f"PDF download failed: {r.status_code if r else 'no response'}")
                return False
        except Exception as e:
            logger.error(f"PDF download error: {e}")
            return False


# ==============================================================================
# HTML Parser for AJAX responses
# ==============================================================================

class LHCParser:
    """Parses HTML responses from LHC AJAX API and listing pages."""

    @staticmethod
    def identify_bench(text: str) -> str:
        """Identify bench from text."""
        text_lower = text.lower()
        for code, patterns in BENCHES.items():
            for pat in patterns:
                if pat.lower() in text_lower:
                    return code
        # Default to Lahore (principal seat)
        if "lahore" in text_lower:
            return "LHR"
        return ""

    @staticmethod
    def parse_upload_date(date_str: str) -> str:
        """Parse upload date like '22-01-2025' to ISO format."""
        date_str = date_str.strip()
        if not date_str:
            return ""
        try:
            dt = datetime.strptime(date_str, "%d-%m-%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        try:
            dt = datetime.strptime(date_str, "%d-%b-%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
        return date_str

    @staticmethod
    def parse_listing_entry(text: str, pdf_url: str = "") -> Optional[LHCCase]:
        """Parse a single listing entry from the AJAX response.

        Format examples:
          1.[Service 46054/24 (Dr Nakshab Choudhary Vs Province of Punjab etc.)
             by Mr. Justice Tariq Saleem Sheikh](PDF_URL)uploaded on: 22-01-2025

          2.[Writ Petition-Regulatory Authorities-... 1987-22 (NASIM HAKIM VS POP ETC)
             by Mr. Justice Asim Hafeez](PDF_URL)Tag Line: Scope of Section 9...
             uploaded on: 21-01-2025
        """
        case = LHCCase()
        text = text.strip()

        # Extract serial number
        m_serial = re.match(r"^(\d+)\.\s*", text)
        if m_serial:
            case.serial = int(m_serial.group(1))
            text = text[m_serial.end():]

        # Extract PDF URL from link
        if pdf_url:
            case.pdf_url = pdf_url
            # Extract filename from URL
            m_fn = re.search(r"(\d{4}LHC\d+\.pdf)", pdf_url)
            if m_fn:
                case.pdf_filename = m_fn.group(1)
                # Extract citation components
                m_cit = re.match(r"(\d{4})LHC(\d+)", case.pdf_filename)
                if m_cit:
                    case.citation_year = m_cit.group(1)
                    case.citation_number = m_cit.group(2)
                    case.citation = f"{case.citation_year} LHC {case.citation_number}"

        # Extract "by Judge" - look for "by Mr. Justice" or "by Justice"
        m_judge = re.search(
            r"\bby\s+((?:Mr\.\s*)?Justice\s+(?:Miss\s+)?[\w\s.]+?)(?:\]|\)|$)",
            text, re.IGNORECASE
        )
        if m_judge:
            case.judge = m_judge.group(1).strip()

        # Extract tagline
        m_tag = re.search(r"Tag\s*Line:\s*(.+?)(?:\s*uploaded\s+on:|$)", text, re.DOTALL | re.IGNORECASE)
        if m_tag:
            case.tagline = m_tag.group(1).strip().rstrip(".")

        # Extract upload date
        m_upload = re.search(r"uploaded\s+on:\s*([\d-]+)", text, re.IGNORECASE)
        if m_upload:
            case.upload_date = LHCParser.parse_upload_date(m_upload.group(1))

        # Extract case type and number
        # Pattern: "Case_Type Number/Year (Parties)" or "Case_Type Number-Year (Parties)"
        # Case types can be complex like "Crl. Misc.-Pre-arrest Bail"
        # Numbers can be "46054/24" or "8741-B-22" or "1505147.1169-13"
        m_case = re.match(
            r"\[?(.+?)\s+([\d][\d/.ABCDEFGH-]*\d)\s*\(([^)]+)\)",
            text
        )
        if not m_case:
            # Try alternate: case type includes special chars then number
            m_case = re.match(
                r"\[?(.+?)\s+([\d]+(?:[/-][\w]+)*(?:[/-]\d{2,4}))\s*\(([^)]+)\)",
                text
            )
        if m_case:
            case.case_type = m_case.group(1).strip()
            case.case_number = m_case.group(2).strip()
            parties_str = m_case.group(3).strip()

            # Parse parties: "Petitioner Vs Respondent" or "PETITIONER VSRESPONDENT"
            # Note: LHC data sometimes has no space around VS, e.g. "KHAN VSSTATE"
            vs_split = re.split(r"\s+(?:Vs|VS|V/S|v/s|vs)\.?\s+", parties_str, maxsplit=1)
            if len(vs_split) < 2:
                # Try with space only before VS: "KHAN VSSTATE"
                vs_split = re.split(r"\s+(?:VS|Vs|V/S)", parties_str, maxsplit=1)
            if len(vs_split) < 2:
                # Try without spaces: "NAMEVSNAME"
                vs_split = re.split(r"(?<=[A-Za-z])(?:VS|Vs|V/S)(?=[A-Z])", parties_str, maxsplit=1)
            if len(vs_split) == 2:
                case.parties = {
                    "petitioner": vs_split[0].strip(),
                    "respondent": vs_split[1].strip(),
                }
            else:
                case.parties = {"title": parties_str}

            case.title = parties_str

        # Determine bench from PDF text (will be enriched later from PDF content)
        return case

    @staticmethod
    def parse_ajax_html(html_content: str, source_type: str = "sitting") -> List[LHCCase]:
        """Parse the AJAX HTML response containing judgment listings.

        The response is HTML with numbered entries containing links to PDFs.
        """
        cases = []

        if not html_content:
            return cases

        soup = BeautifulSoup(html_content, "html.parser")

        # Try to find total count
        total_match = re.search(r"Total\s+Judgments\s*\((\d+)\)", html_content)
        if total_match:
            logger.info(f"Total judgments in response: {total_match.group(1)}")

        # Method 1: Parse <a> tags with PDF links
        links = soup.find_all("a", href=re.compile(r"appjudgments/\d{4}LHC\d+\.pdf"))
        if links:
            for link in links:
                pdf_url = link.get("href", "")
                # Get the surrounding text
                parent = link.parent or link
                full_text = parent.get_text(separator=" ", strip=True)
                link_text = link.get_text(separator=" ", strip=True)

                case = LHCParser.parse_listing_entry(full_text, pdf_url)
                if case:
                    case.source_type = source_type
                    case.source_url = pdf_url
                    cases.append(case)
            return cases

        # Method 2: Parse raw text with regex (for plain HTML responses)
        # Pattern: serial.[link_text](url)...uploaded on: date
        entries = re.findall(
            r'(\d+)\.\s*<a[^>]+href=["\']([^"\']+appjudgments/\d{4}LHC\d+\.pdf)["\'][^>]*>(.*?)</a>(.*?)(?=\d+\.\s*<a|\Z)',
            html_content,
            re.DOTALL
        )

        for serial, pdf_url, link_text, after_text in entries:
            full_text = f"{serial}.{link_text} {after_text}"
            case = LHCParser.parse_listing_entry(full_text, pdf_url)
            if case:
                case.serial = int(serial)
                case.source_type = source_type
                cases.append(case)

        if not cases:
            # Method 3: Try parsing the Wayback/cached listing format
            # Pattern from web archive: "1.[Case info](url)...uploaded on: date"
            pattern = re.compile(
                r'(\d+)\.\[([^\]]+)\]\(([^)]+appjudgments/\d{4}LHC\d+\.pdf)\)(.*?)(?=\d+\.\[|\Z)',
                re.DOTALL
            )
            for m in pattern.finditer(html_content):
                serial = int(m.group(1))
                link_text = m.group(2)
                pdf_url = m.group(3)
                after_text = m.group(4)
                full_text = f"{serial}.{link_text} {after_text}"
                case = LHCParser.parse_listing_entry(full_text, pdf_url)
                if case:
                    case.serial = serial
                    case.source_type = source_type
                    cases.append(case)

        return cases

    @staticmethod
    def parse_wayback_listing(html_content: str) -> List[LHCCase]:
        """Parse the Wayback Machine cached listing page for all judgments."""
        cases = []
        soup = BeautifulSoup(html_content, "html.parser")

        # Find all links to appjudgments PDFs
        links = soup.find_all("a", href=re.compile(r"appjudgments/\d{4}LHC\d+\.pdf"))

        for link in links:
            pdf_url = link.get("href", "")
            # Clean Wayback URL wrapper
            pdf_url = re.sub(r"https?://web\.archive\.org/web/\d+/", "", pdf_url)

            link_text = link.get_text(separator=" ", strip=True)

            # Get surrounding context (parent element text)
            parent = link.parent
            if parent:
                context = parent.get_text(separator=" ", strip=True)
            else:
                context = link_text

            case = LHCParser.parse_listing_entry(context, pdf_url)
            if case:
                cases.append(case)

        return cases


# ==============================================================================
# PDF Text Extraction
# ==============================================================================

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from a PDF file using PyMuPDF or pdfplumber."""
    if HAS_PYMUPDF:
        try:
            doc = fitz.open(str(pdf_path))
            text_parts = []
            for page in doc:
                text_parts.append(page.get_text())
            doc.close()
            return "\n".join(text_parts).strip()
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed for {pdf_path.name}: {e}")

    if HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                text_parts = []
                for page in pdf.pages:
                    text_parts.append(page.extract_text() or "")
                return "\n".join(text_parts).strip()
        except Exception as e:
            logger.warning(f"pdfplumber extraction failed for {pdf_path.name}: {e}")

    logger.warning(f"No PDF extraction library available for {pdf_path.name}")
    return ""


def detect_bench_from_text(text: str) -> Tuple[str, str]:
    """Detect LHC bench from judgment text content."""
    text_upper = text[:2000].upper()  # Check first 2000 chars

    if "RAWALPINDI BENCH" in text_upper or "AT RAWALPINDI" in text_upper:
        return "RWP", "Rawalpindi Bench"
    if "MULTAN BENCH" in text_upper or "AT MULTAN" in text_upper:
        return "MUL", "Multan Bench"
    if "BAHAWALPUR BENCH" in text_upper or "AT BAHAWALPUR" in text_upper:
        return "BWP", "Bahawalpur Bench"
    if "AT LAHORE" in text_upper or "LAHORE HIGH COURT LAHORE" in text_upper:
        return "LHR", "Lahore (Principal Seat)"

    return "LHR", "Lahore (Principal Seat)"  # Default


# ==============================================================================
# HTML Generation
# ==============================================================================

def generate_readable_html(case: LHCCase) -> str:
    """Generate readable HTML from case data."""
    parties = case.parties
    petitioner = parties.get("petitioner", "")
    respondent = parties.get("respondent", "")
    title_display = case.title or f"{petitioner} vs {respondent}" if petitioner else "Unknown"

    text_html = ""
    if case.judgment_text:
        # Convert text to HTML paragraphs
        paragraphs = case.judgment_text.split("\n\n")
        for p in paragraphs:
            p = p.strip()
            if p:
                p = html_mod.escape(p).replace("\n", "<br>")
                text_html += f"<p>{p}</p>\n"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html_mod.escape(case.citation or case.file_key)} - Lahore High Court</title>
    <style>
        body {{ font-family: Georgia, 'Times New Roman', serif; max-width: 900px; margin: 0 auto;
               padding: 20px; line-height: 1.8; color: #333; background: #fafaf8; }}
        .header {{ border-bottom: 3px double #2d5016; padding-bottom: 15px; margin-bottom: 25px; }}
        .court-name {{ text-align: center; font-size: 1.4em; font-weight: bold; color: #2d5016;
                       text-transform: uppercase; letter-spacing: 2px; }}
        .citation {{ text-align: center; font-size: 1.2em; color: #555; margin: 10px 0; }}
        .meta {{ background: #f0f0e8; padding: 15px; border-radius: 5px; margin: 15px 0; }}
        .meta dt {{ font-weight: bold; color: #2d5016; }}
        .meta dd {{ margin-left: 0; margin-bottom: 8px; }}
        .tagline {{ font-style: italic; color: #555; padding: 10px 20px;
                    border-left: 3px solid #2d5016; margin: 15px 0; background: #f8f8f0; }}
        .judgment-text {{ text-align: justify; }}
        .judgment-text p {{ margin: 8px 0; text-indent: 2em; }}
        .footer {{ border-top: 1px solid #ccc; margin-top: 30px; padding-top: 10px;
                   font-size: 0.85em; color: #999; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="court-name">Lahore High Court{' - ' + html_mod.escape(case.bench_name) if case.bench_name else ''}</div>
        <div class="citation">{html_mod.escape(case.citation)}</div>
    </div>

    <dl class="meta">
        <dt>Case</dt>
        <dd>{html_mod.escape(case.case_type)} {html_mod.escape(case.case_number)}</dd>
        <dt>Title</dt>
        <dd>{html_mod.escape(title_display)}</dd>
        <dt>Judge</dt>
        <dd>{html_mod.escape(case.judge)}</dd>
        {'<dt>Bench</dt><dd>' + html_mod.escape(case.bench_name) + '</dd>' if case.bench_name else ''}
        {'<dt>Decision Date</dt><dd>' + html_mod.escape(case.decision_date) + '</dd>' if case.decision_date else ''}
        {'<dt>Upload Date</dt><dd>' + html_mod.escape(case.upload_date) + '</dd>' if case.upload_date else ''}
    </dl>

    {'<div class="tagline">' + html_mod.escape(case.tagline) + '</div>' if case.tagline else ''}

    <div class="judgment-text">
        {text_html if text_html else '<p><em>Full text not available. See original PDF.</em></p>'}
    </div>

    <div class="footer">
        <p>Source: Lahore High Court - {html_mod.escape(case.source_type)} judges</p>
        <p>PDF: <a href="{html_mod.escape(case.pdf_url)}">{html_mod.escape(case.pdf_filename)}</a></p>
        <p>Fetched: {case.fetched_at}</p>
    </div>
</body>
</html>"""


# ==============================================================================
# Progress Tracker
# ==============================================================================

class ProgressTracker:
    """Track scraping progress across sessions."""

    def __init__(self, path: Path = PROGRESS_FILE):
        self.path = path
        self.data = self._load()

    def _load(self) -> Dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return {}
        return {}

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def mark_case_done(self, file_key: str, year: str):
        """Mark a case as fully processed."""
        year_key = f"year_{year}"
        if year_key not in self.data:
            self.data[year_key] = {"completed": [], "failed": [], "total": 0}
        if file_key not in self.data[year_key]["completed"]:
            self.data[year_key]["completed"].append(file_key)
        self.save()

    def mark_case_failed(self, file_key: str, year: str, reason: str = ""):
        """Mark a case as failed."""
        year_key = f"year_{year}"
        if year_key not in self.data:
            self.data[year_key] = {"completed": [], "failed": [], "total": 0}
        entry = {"key": file_key, "reason": reason}
        if entry not in self.data[year_key]["failed"]:
            self.data[year_key]["failed"].append(entry)
        self.save()

    def is_done(self, file_key: str, year: str) -> bool:
        """Check if a case has been processed."""
        year_key = f"year_{year}"
        return file_key in self.data.get(year_key, {}).get("completed", [])

    def set_year_total(self, year: str, total: int):
        year_key = f"year_{year}"
        if year_key not in self.data:
            self.data[year_key] = {"completed": [], "failed": [], "total": 0}
        self.data[year_key]["total"] = total
        self.save()

    def update_meta(self, key: str, value: Any):
        if "meta" not in self.data:
            self.data["meta"] = {}
        self.data["meta"][key] = value
        self.save()

    def get_stats(self) -> Dict:
        """Get overall statistics."""
        stats = {"years": {}, "total_completed": 0, "total_failed": 0, "total_expected": 0}
        for key, val in self.data.items():
            if key.startswith("year_"):
                year = key.replace("year_", "")
                completed = len(val.get("completed", []))
                failed = len(val.get("failed", []))
                total = val.get("total", 0)
                stats["years"][year] = {
                    "completed": completed,
                    "failed": failed,
                    "total": total,
                }
                stats["total_completed"] += completed
                stats["total_failed"] += failed
                stats["total_expected"] += total
        return stats


# ==============================================================================
# File Saver
# ==============================================================================

class FileSaver:
    """Save case data in multiple formats."""

    @staticmethod
    def save_case(case: LHCCase, pdf_content: bytes = None):
        """Save case in all formats: JSON, HTML, PDF, JSONL."""
        year = case.year
        file_key = case.file_key

        # Create directories
        year_dir = LHC_DIR / year
        original_dir = year_dir / "original"
        html_year_dir = HTML_DIR / year
        for d in (year_dir, original_dir, html_year_dir):
            d.mkdir(parents=True, exist_ok=True)

        # 1. Save JSON metadata
        json_path = year_dir / f"{file_key}.json"
        json_path.write_text(
            json.dumps(case.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

        # 2. Save original PDF
        if pdf_content:
            pdf_path = original_dir / (case.pdf_filename or f"{file_key}.pdf")
            pdf_path.write_bytes(pdf_content)

            # Extract text from saved PDF
            if not case.judgment_text:
                case.judgment_text = extract_pdf_text(pdf_path)
                # Update JSON with extracted text
                json_path.write_text(
                    json.dumps(case.to_dict(), indent=2, ensure_ascii=False),
                    encoding="utf-8"
                )

                # Detect bench from judgment text
                if case.judgment_text and not case.bench:
                    bench_code, bench_name = detect_bench_from_text(case.judgment_text)
                    case.bench = bench_code
                    case.bench_name = bench_name

        # 3. Save readable HTML
        html_path = html_year_dir / f"{file_key}.html"
        html_path.write_text(generate_readable_html(case), encoding="utf-8")

        # 4. Append to JSONL
        FileSaver.append_jsonl(case)

        logger.info(f"Saved: {file_key} ({year})")

    @staticmethod
    def append_jsonl(case: LHCCase):
        """Append case to the court-specific JSONL file."""
        jsonl_path = LHC_DIR / "lhc_judgments.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)

        # Also append to all-courts JSONL
        entry = case.to_dict()
        entry["_court"] = "LHC"

        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


# ==============================================================================
# Main Scraper
# ==============================================================================

class LHCScraper:
    """Main scraper orchestrating all components."""

    def __init__(self):
        self.session = LHCSession()
        self.parser = LHCParser()
        self.progress = ProgressTracker()
        self.saver = FileSaver()
        self._direct_blocked = False  # Set True when FortiGuard detected

    def discover(self):
        """Test connectivity and discover available endpoints."""
        print("\n" + "=" * 70)
        print("LHC Scraper - Endpoint Discovery")
        print("=" * 70)

        domains = [
            (DATA_DOMAIN, "Data Portal"),
            (SYS_DOMAIN, "Judgment System"),
            (MAIN_DOMAIN, "Main Website"),
        ]

        for domain, label in domains:
            print(f"\n[{label}] {domain}")
            reachable = self.session.check_connectivity(domain)
            print(f"  Reachable: {'[OK] Yes' if reachable else '[X] No (likely FortiGuard blocked)'}")

        # Test specific endpoints
        print("\n--- API Endpoints ---")
        endpoints = [
            (SITTING_JUDGES_API + "?year=2024", "Sitting Judges API"),
            (FORMER_JUDGES_API + "?year=2024", "Former Judges API"),
            (f"{PDF_BASE}/2024LHC3881.pdf", "Sample PDF"),
        ]

        for url, label in endpoints:
            print(f"\n[{label}] {url}")
            try:
                r = self.session.get(url, timeout=15, retries=1)
                if r:
                    ct = r.headers.get("Content-Type", "?")
                    print(f"  Status: {r.status_code}, Content-Type: {ct}, Size: {len(r.content):,}")
                    if r.status_code == 200 and "pdf" in ct.lower():
                        print("  [OK] PDF accessible!")
                    elif r.status_code == 200 and "html" in ct.lower():
                        # Check if it's actual content or firewall page
                        if "fortinet" in r.text.lower() or "fortigate" in r.text.lower():
                            print("  [X] Blocked by firewall")
                        else:
                            total = re.search(r"Total\s+Judgments\s*\((\d+)\)", r.text)
                            if total:
                                print(f"  [OK] API working! Total judgments: {total.group(1)}")
                            else:
                                print(f"  ? Got HTML response ({len(r.text)} chars)")
                    else:
                        print(f"  ? Unexpected response")
                else:
                    print("  [X] No response (timeout/blocked)")
            except Exception as e:
                print(f"  [X] Error: {e}")

        # Test Wayback Machine CDX API (primary data source)
        print("\n--- Wayback Machine CDX API (Primary Source) ---")
        cdx_url = (
            "https://web.archive.org/cdx/search/cdx?"
            "url=sys.lhc.gov.pk/appjudgments/&matchType=prefix"
            "&output=json&fl=original,timestamp,statuscode"
            "&collapse=urlkey&filter=statuscode:200&limit=100000"
        )
        try:
            r = self.session.get(cdx_url, timeout=60, retries=1)
            if r and r.status_code == 200:
                rows = json.loads(r.text)
                total = len(rows) - 1  # minus header
                # Count by year
                from collections import Counter
                year_counts = Counter()
                for row in rows[1:]:
                    m = re.search(r'/(\d{4})LHC\d+\.pdf', row[0])
                    if m:
                        year_counts[m.group(1)] += 1
                print(f"  [OK] CDX API working! Found {total} archived PDFs")
                print(f"  Year breakdown:")
                for yr in sorted(year_counts.keys()):
                    print(f"    {yr}: {year_counts[yr]} PDFs")
            else:
                print(f"  [X] CDX API failed (status: {r.status_code if r else 'none'})")
        except Exception as e:
            print(f"  [X] CDX API error: {e}")

        # Also test Wayback listing page
        print("\n--- Wayback Listing Page ---")
        wb_url = f"https://web.archive.org/web/2025/{SITTING_PAGE}"
        try:
            r = self.session.get(wb_url, timeout=30, retries=1)
            if r and r.status_code == 200:
                links = re.findall(r"appjudgments/\d{4}LHC\d+\.pdf", r.text)
                print(f"  [OK] Wayback listing cache available ({len(links)} PDF links on page)")
            else:
                print(f"  [X] Wayback listing unavailable")
        except Exception as e:
            print(f"  [X] Wayback listing error: {e}")

        print("\n" + "=" * 70)

    def _fetch_listings_direct(self, year: str, source: str = "sitting") -> List[LHCCase]:
        """Fetch judgment listings from AJAX API directly."""
        api_url = SITTING_JUDGES_API if source == "sitting" else FORMER_JUDGES_API
        params = {
            "year": year,
            "debug": "0",
            "courtName": "All Courts",
            "caseNumber": "",
            "citationTag": "",
            "partyName": "",
            "decisionDate0": "",
            "decisionDate1": "",
            "uploadDate": "",
            "uploadDate1": "",
        }

        logger.info(f"Fetching {source} judge listings for {year} from direct API...")
        html = self.session.get_ajax(api_url, params)

        if html:
            cases = self.parser.parse_ajax_html(html, source)
            logger.info(f"Parsed {len(cases)} cases for {year} ({source})")
            return cases

        logger.warning(f"Direct API failed for {year} ({source})")
        return []

    def _fetch_all_pdfs_cdx(self, year: str = None) -> List[LHCCase]:
        """Use Wayback Machine CDX API to enumerate ALL archived LHC judgment PDFs.
        
        This is the primary discovery method since LHC domains are FortiGuard-blocked.
        CDX API returns URLs of all PDFs ever archived from sys.lhc.gov.pk/appjudgments/.
        """
        cdx_url = (
            "https://web.archive.org/cdx/search/cdx?"
            "url=sys.lhc.gov.pk/appjudgments/&matchType=prefix"
            "&output=json&fl=original,timestamp,statuscode"
            "&collapse=urlkey&filter=statuscode:200&limit=100000"
        )
        logger.info("Fetching full PDF inventory from Wayback CDX API...")
        self.session._delay(2, 4)

        try:
            r = self.session.get(cdx_url, timeout=90)
            if not r or r.status_code != 200:
                logger.error(f"CDX API failed: {r.status_code if r else 'no response'}")
                return []

            rows = json.loads(r.text)
            if len(rows) <= 1:
                logger.warning("CDX API returned no results")
                return []

            logger.info(f"CDX API returned {len(rows) - 1} archived URLs")

        except Exception as e:
            logger.error(f"CDX API error: {e}")
            return []

        # Parse into LHCCase objects
        cases = []
        seen = set()

        for row in rows[1:]:  # Skip header
            url = row[0]
            timestamp = row[1]

            # Extract citation from URL
            m = re.search(r'/(\d{4})LHC(\d+)\.pdf', url)
            if not m:
                continue

            cit_year = m.group(1)
            cit_num = m.group(2)
            pdf_filename = f"{cit_year}LHC{cit_num}.pdf"

            # Filter by year if specified
            if year and cit_year != year:
                continue

            # Deduplicate
            if pdf_filename in seen:
                continue
            seen.add(pdf_filename)

            # Normalize URL to canonical form
            canonical_url = f"https://sys.lhc.gov.pk/appjudgments/{pdf_filename}"

            case = LHCCase(
                citation=f"{cit_year} LHC {cit_num}",
                citation_year=cit_year,
                citation_number=cit_num,
                pdf_url=canonical_url,
                pdf_filename=pdf_filename,
                source_url=canonical_url,
            )
            # Store Wayback timestamp for download
            case._wb_timestamp = timestamp
            cases.append(case)

        logger.info(f"Found {len(cases)} unique PDFs" + (f" for year {year}" if year else ""))
        return cases

    def _enrich_from_wayback_listings(self, cases: List[LHCCase]) -> List[LHCCase]:
        """Try to enrich cases with metadata from Wayback cached listing pages.
        
        This adds judge names, case types, parties, taglines from the HTML listings.
        """
        # Only fetch listing pages if we have cases to enrich
        if not cases:
            return cases

        metadata_map = {}  # pdf_filename -> metadata dict

        for source in ["sitting", "former"]:
            page_url = SITTING_PAGE if source == "sitting" else FORMER_PAGE
            
            # Get most recent snapshot
            cdx_url = (
                f"https://web.archive.org/cdx/search/cdx?"
                f"url={page_url}&output=json&fl=timestamp"
                f"&filter=statuscode:200&limit=5&sort=reverse"
            )
            self.session._delay(2, 4)

            try:
                r = self.session.get(cdx_url, timeout=30)
                if not r or r.status_code != 200:
                    continue
                rows = json.loads(r.text)
                timestamps = [row[0] for row in rows[1:]] if len(rows) > 1 else []
            except Exception:
                timestamps = ["20250123"]

            for ts in timestamps[:3]:
                wb_url = f"https://web.archive.org/web/{ts}/{page_url}"
                logger.info(f"Enriching metadata from Wayback listing ({source}, {ts})...")
                self.session._delay(3, 6)

                try:
                    r = self.session.get(wb_url, timeout=90)
                    if not r or r.status_code != 200:
                        continue

                    listing_cases = self.parser.parse_wayback_listing(r.text)
                    for lc in listing_cases:
                        if lc.pdf_filename:
                            metadata_map[lc.pdf_filename] = {
                                "case_type": lc.case_type,
                                "case_number": lc.case_number,
                                "parties": lc.parties,
                                "title": lc.title,
                                "judge": lc.judge,
                                "tagline": lc.tagline,
                                "upload_date": lc.upload_date,
                                "source_type": source,
                            }
                except Exception as e:
                    logger.warning(f"Listing enrichment failed: {e}")

        # Apply metadata to cases
        enriched = 0
        for case in cases:
            if case.pdf_filename in metadata_map:
                meta = metadata_map[case.pdf_filename]
                for key, val in meta.items():
                    if val and not getattr(case, key, None):
                        setattr(case, key, val)
                enriched += 1

        logger.info(f"Enriched {enriched}/{len(cases)} cases with listing metadata")
        return cases

    def _fetch_listings_wayback(self, source: str = "sitting") -> List[LHCCase]:
        """Legacy method - fetch listings from Wayback cached page."""
        page_url = SITTING_PAGE if source == "sitting" else FORMER_PAGE
        wb_url = f"https://web.archive.org/web/2025/{page_url}"

        logger.info(f"Fetching {source} listings from Wayback Machine...")
        self.session._delay(2, 5)

        r = self.session.get(wb_url, timeout=60)
        if not r or r.status_code != 200:
            logger.warning("Wayback Machine fetch failed")
            return []

        cases = self.parser.parse_wayback_listing(r.text)
        logger.info(f"Parsed {len(cases)} cases from Wayback ({source})")

        for case in cases:
            case.source_type = source

        return cases

    def _fetch_listings(self, year: str = None, source: str = "both") -> List[LHCCase]:
        """Fetch listings using best available method.
        
        Strategy:
        1. Try direct AJAX API (likely blocked by FortiGuard)
        2. Use Wayback CDX API to enumerate all archived PDFs (primary method)
        3. Enrich with metadata from Wayback listing page cache
        """
        all_cases = []

        # Strategy 1: Try direct API (usually blocked)
        if year:
            sources = ["sitting", "former"] if source == "both" else [source]
            for src in sources:
                cases = self._fetch_listings_direct(year, src)
                if cases:
                    all_cases.extend(cases)

        # Strategy 2: Wayback CDX API (primary fallback - finds ALL PDFs)
        if not all_cases:
            logger.info("Direct API unavailable, using Wayback CDX enumeration...")
            cdx_cases = self._fetch_all_pdfs_cdx(year)
            if cdx_cases:
                # Enrich with metadata from listing pages
                cdx_cases = self._enrich_from_wayback_listings(cdx_cases)
                all_cases.extend(cdx_cases)

        # Deduplicate by pdf_filename
        seen = set()
        unique = []
        for case in all_cases:
            key = case.pdf_filename or case.file_key
            if key not in seen:
                seen.add(key)
                unique.append(case)

        # Sort by citation number (ascending)
        unique.sort(key=lambda c: (c.citation_year, int(c.citation_number or 0)))

        logger.info(f"Total unique cases: {len(unique)}")
        return unique

    def _enumerate_pdfs(self, year: int, max_number: int = 10000) -> List[LHCCase]:
        """Enumerate PDFs by trying sequential citation numbers.

        LHC citations follow: {YEAR}LHC{1..N}.pdf
        """
        logger.info(f"Enumerating PDFs for {year} (up to {max_number})...")
        cases = []
        consecutive_misses = 0
        max_consecutive_misses = 50  # Stop after 50 consecutive 404s

        for num in range(1, max_number + 1):
            pdf_filename = f"{year}LHC{num}.pdf"
            pdf_url = f"{PDF_BASE}/{pdf_filename}"

            # Check if already done
            file_key = f"LHC_{year}_{num}"
            if self.progress.is_done(file_key, str(year)):
                consecutive_misses = 0
                continue

            self.session._delay(2, 5)

            try:
                r = self.session.session.head(pdf_url, timeout=15)
                if r.status_code == 200:
                    ct = r.headers.get("Content-Type", "")
                    if "pdf" in ct.lower():
                        case = LHCCase(
                            citation=f"{year} LHC {num}",
                            citation_year=str(year),
                            citation_number=str(num),
                            pdf_url=pdf_url,
                            pdf_filename=pdf_filename,
                        )
                        cases.append(case)
                        consecutive_misses = 0
                        logger.info(f"Found: {pdf_filename}")
                    else:
                        consecutive_misses += 1
                elif r.status_code == 404:
                    consecutive_misses += 1
                elif r.status_code == 403:
                    # Firewall block - can't enumerate
                    logger.error("Firewall block detected during enumeration. Stopping.")
                    break
                else:
                    consecutive_misses += 1

                if consecutive_misses >= max_consecutive_misses:
                    logger.info(f"Stopping enumeration after {max_consecutive_misses} consecutive misses at {num}")
                    break

            except Exception as e:
                logger.warning(f"Enumeration error at {num}: {e}")
                consecutive_misses += 1

        logger.info(f"Enumeration found {len(cases)} PDFs for {year}")
        return cases

    def _download_pdf(self, case: LHCCase) -> Optional[bytes]:
        """Try to download a PDF, with Wayback Machine fallback.
        
        Uses the id_ suffix to get the raw original file from Wayback Machine
        (without the Wayback toolbar injection).
        """
        pdf_url = case.pdf_url
        pdf_filename = case.pdf_filename

        # Strategy 1: Direct download (may be blocked by FortiGuard)
        if not self._direct_blocked:
            self.session._delay(3, 6)
            try:
                r = self.session.get(pdf_url, timeout=PDF_TIMEOUT, retries=1)
                if r and r.status_code == 200:
                    ct = r.headers.get("Content-Type", "")
                    if "pdf" in ct.lower() or r.content[:5] == b"%PDF-":
                        logger.info(f"Direct download OK: {pdf_filename} ({len(r.content):,} bytes)")
                        return r.content
                    elif b"fortinet" in r.content.lower() or b"fortigate" in r.content.lower():
                        logger.warning("Direct downloads blocked by FortiGuard, switching to Wayback")
                        self._direct_blocked = True
                    else:
                        logger.warning(f"Not a PDF from direct: {pdf_url}")
            except Exception as e:
                logger.warning(f"Direct download failed, switching to Wayback: {e}")
                self._direct_blocked = True

        # Strategy 2: Wayback Machine with known timestamp (id_ suffix = raw file)
        wb_timestamp = getattr(case, '_wb_timestamp', None)
        timestamps_to_try = []
        if wb_timestamp:
            timestamps_to_try.append(wb_timestamp)
        # Add some common recent timestamps as fallback
        timestamps_to_try.extend(["", "20250123", "20241215", "20240901", "20240101"])

        for ts in timestamps_to_try:
            if ts:
                wb_url = f"https://web.archive.org/web/{ts}id_/{pdf_url}"
            else:
                wb_url = f"https://web.archive.org/web/id_/{pdf_url}"

            self.session._delay(3, 7)
            try:
                r = self.session.get(wb_url, timeout=PDF_TIMEOUT)
                if r and r.status_code == 200 and r.content[:5] == b"%PDF-":
                    logger.info(f"Wayback download OK: {pdf_filename} ({len(r.content):,} bytes)")
                    return r.content
                elif r and r.status_code == 302:
                    # Follow redirect
                    loc = r.headers.get("Location", "")
                    if loc:
                        self.session._delay(1, 3)
                        r2 = self.session.get(loc, timeout=PDF_TIMEOUT)
                        if r2 and r2.status_code == 200 and r2.content[:5] == b"%PDF-":
                            logger.info(f"Wayback redirect download OK: {pdf_filename}")
                            return r2.content
            except Exception as e:
                if ts == timestamps_to_try[-1]:
                    logger.warning(f"Wayback download failed for {pdf_filename}: {e}")
                continue

        logger.warning(f"All download methods failed for {pdf_filename}")
        return None

    def process_case(self, case: LHCCase) -> bool:
        """Download PDF and save case in all formats."""
        file_key = case.file_key
        year = case.year

        if self.progress.is_done(file_key, year):
            logger.debug(f"Skipping (already done): {file_key}")
            return True

        case.fetched_at = datetime.now(timezone.utc).isoformat()

        # Download PDF
        pdf_content = None
        if case.pdf_url:
            pdf_content = self._download_pdf(case)

        # Save in all formats (even without PDF - metadata is still valuable)
        try:
            self.saver.save_case(case, pdf_content)
            self.progress.mark_case_done(file_key, year)
            return True
        except Exception as e:
            logger.error(f"Save error for {file_key}: {e}")
            self.progress.mark_case_failed(file_key, year, str(e))
            return False

    def scrape_year(self, year: str, source: str = "both", limit: int = None):
        """Scrape all judgments for a specific year."""
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Scraping LHC judgments for {year} ({source})")
        logger.info(f"{'=' * 60}")

        # Fetch listings
        cases = self._fetch_listings(year, source)

        if not cases:
            logger.warning(f"No cases found for {year}. Trying PDF enumeration...")
            cases = self._enumerate_pdfs(int(year))

        if not cases:
            logger.warning(f"No cases found for {year} by any method")
            return

        self.progress.set_year_total(year, len(cases))

        if limit:
            cases = cases[:limit]
            logger.info(f"Limited to {limit} cases")

        success = 0
        failed = 0

        for i, case in enumerate(cases):
            logger.info(f"[{i + 1}/{len(cases)}] Processing: {case.file_key}")
            if self.process_case(case):
                success += 1
            else:
                failed += 1

        logger.info(f"\nYear {year} complete: {success} success, {failed} failed out of {len(cases)}")

    def scrape_all(self, source: str = "both", limit: int = None):
        """Scrape all years."""
        for year in range(YEAR_END - 1, YEAR_START - 1, -1):
            self.scrape_year(str(year), source, limit)

    def show_status(self):
        """Display scraping progress."""
        stats = self.progress.get_stats()
        print("\n" + "=" * 70)
        print("LHC Scraper - Progress Status")
        print("=" * 70)

        if not stats["years"]:
            print("No scraping progress yet.")
            # Count any existing files
            if LHC_DIR.exists():
                total_json = sum(1 for _ in LHC_DIR.rglob("*.json"))
                total_pdf = sum(1 for _ in LHC_DIR.rglob("*.pdf"))
                total_html = sum(1 for _ in HTML_DIR.rglob("*.html")) if HTML_DIR.exists() else 0
                print(f"\nExisting files: {total_json} JSON, {total_pdf} PDF, {total_html} HTML")
            return

        print(f"\n{'Year':<8} {'Done':<8} {'Failed':<8} {'Total':<8} {'Progress':<15}")
        print("-" * 50)

        for year in sorted(stats["years"].keys(), reverse=True):
            ys = stats["years"][year]
            pct = (ys["completed"] / ys["total"] * 100) if ys["total"] else 0
            bar_len = int(pct / 5)
            bar = "#" * bar_len + "." * (20 - bar_len)
            print(f"{year:<8} {ys['completed']:<8} {ys['failed']:<8} {ys['total']:<8} {bar} {pct:.0f}%")

        print("-" * 50)
        print(f"{'Total':<8} {stats['total_completed']:<8} {stats['total_failed']:<8} {stats['total_expected']:<8}")

        # File counts
        if LHC_DIR.exists():
            total_json = sum(1 for _ in LHC_DIR.rglob("LHC_*.json"))
            total_pdf = sum(1 for _ in LHC_DIR.rglob("*.pdf"))
            total_html = sum(1 for _ in HTML_DIR.rglob("*.html")) if HTML_DIR.exists() else 0
            jsonl_size = 0
            jsonl_path = LHC_DIR / "lhc_judgments.jsonl"
            if jsonl_path.exists():
                jsonl_size = jsonl_path.stat().st_size
                jsonl_lines = sum(1 for _ in open(jsonl_path, encoding="utf-8"))
            else:
                jsonl_lines = 0

            print(f"\nFiles on disk:")
            print(f"  JSON:  {total_json}")
            print(f"  PDF:   {total_pdf}")
            print(f"  HTML:  {total_html}")
            print(f"  JSONL: {jsonl_lines} entries ({jsonl_size / 1024:.1f} KB)")

        print("=" * 70)


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="LHC Scraper - Lahore High Court Reported Judgments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lhc_scraper.py                     # Scrape all years
  python lhc_scraper.py --year 2024         # Specific year
  python lhc_scraper.py --discover          # Test endpoints
  python lhc_scraper.py --status            # Show progress
  python lhc_scraper.py --enumerate --year 2024  # Enumerate PDFs
  python lhc_scraper.py --source former     # Only former judges
  python lhc_scraper.py --limit 5           # First 5 per year
        """
    )

    parser.add_argument("--year", type=str, help="Specific year to scrape")
    parser.add_argument("--discover", action="store_true", help="Discover API endpoints")
    parser.add_argument("--status", action="store_true", help="Show progress status")
    parser.add_argument("--enumerate", action="store_true", help="Enumerate PDFs by URL pattern")
    parser.add_argument("--source", choices=["sitting", "former", "both"], default="both",
                        help="Which judge source to scrape")
    parser.add_argument("--limit", type=int, help="Max cases per year")
    parser.add_argument("--judge", type=str, help="Filter by specific judge name")

    args = parser.parse_args()
    scraper = LHCScraper()

    if args.discover:
        scraper.discover()
        return

    if args.status:
        scraper.show_status()
        return

    if args.enumerate:
        year = int(args.year) if args.year else datetime.now().year
        cases = scraper._enumerate_pdfs(year)
        print(f"Found {len(cases)} PDFs for {year}")
        for c in cases[:20]:
            print(f"  {c.citation} - {c.pdf_url}")
        if len(cases) > 20:
            print(f"  ... and {len(cases) - 20} more")
        return

    # Main scraping
    if args.year:
        scraper.scrape_year(args.year, args.source, args.limit)
    else:
        scraper.scrape_all(args.source, args.limit)


if __name__ == "__main__":
    main()
