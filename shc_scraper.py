#!/usr/bin/env python3
"""
SHC Scraper — Sindh High Court Caselaw Database
=================================================
Scrapes the PUBLIC caselaw database at caselaw.shc.gov.pk.
No login required. Downloads judgment PDFs + extracts metadata.

Five benches: KHI (Karachi), HYD (Hyderabad), SUK (Sukkur),
              LAR (Larkana), MIR (Mirpurkhas)

Usage:
    python shc_scraper.py                          # Scrape all benches, all years
    python shc_scraper.py --bench KHI              # Specific bench
    python shc_scraper.py --year 2025              # Specific year
    python shc_scraper.py --bench KHI --year 2026  # Both
    python shc_scraper.py --status                 # Show progress
    python shc_scraper.py --list-only              # Parse homepage, don't download
    python shc_scraper.py --limit 10               # Download only first N cases
"""

import os
import re
import json
import time
import random
import base64
import logging
import argparse
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field, asdict

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup, Tag

# PDF text extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://caselaw.shc.gov.pk/caselaw"
PUBLIC_HOME = f"{BASE_URL}/public/home"
DOWNLOAD_URL = f"{BASE_URL}/download-file.php"
VIEW_URL = f"{BASE_URL}/view-file"

DATA_DIR = Path(__file__).parent / "data_v2"
COURT_DIR = DATA_DIR / "court_cases"
SHC_DIR = COURT_DIR / "SHC"
HTML_DIR = DATA_DIR / "html" / "court_cases" / "SHC"
PROGRESS_FILE = COURT_DIR / "shc_progress.json"
ALL_JSONL = COURT_DIR / "all_court_cases.jsonl"

# Benches and their identifiers from court names in HTML
BENCHES = {
    "KHI": ["Sindh High Court, Karachi", "Sindh High Court Karachi"],
    "HYD": ["Circuit at Hyderabad", "Circuit Court at Hyderabad"],
    "SUK": ["Bench at Sukkur", "Bench Sukkur"],
    "LAR": ["Circuit at Larkana", "Circuit Court at Larkana"],
    "MIR": ["Circuit Court, Mirpur Khas", "Mirpur Khas", "Mirpurkhas"],
}

# Bench name from citation
BENCH_CODES = {"KHI", "HYD", "SUK", "LAR", "MIR", "MPK"}

# Timing — be respectful to government server
MIN_DELAY = 3.0
MAX_DELAY = 8.0
HOMEPAGE_TIMEOUT = 120  # 12.8MB page can be slow
DOWNLOAD_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BACKOFF = 30

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("shc_scraper")


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class SHCCase:
    """Represents a single case from the SHC caselaw database."""
    source: str = "SHC"
    bench: str = ""
    citation: str = ""
    case_number: str = ""
    case_type: str = ""
    parties: Dict[str, str] = field(default_factory=dict)
    matter: str = ""
    judges: List[Dict[str, Any]] = field(default_factory=list)
    advocates: List[Dict[str, str]] = field(default_factory=list)
    tagline: str = ""
    order_date: str = ""
    judgment_text: str = ""
    approved_for_reporting: bool = False
    fetched_at: str = ""
    source_url: str = ""
    download_url: str = ""
    doc_id: str = ""
    shc_citation_id: str = ""
    bench_name: str = ""
    downloads_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # Remove empty fields
        return {k: v for k, v in d.items() if v or isinstance(v, bool)}

    @property
    def file_key(self) -> str:
        """Generate file key from citation e.g. 'SHC_KHI_304'"""
        if self.citation:
            parts = self.citation.split()
            if len(parts) >= 4:
                # "2026 SHC KHI 304" -> "SHC_KHI_304"
                return f"SHC_{parts[2]}_{parts[3]}"
        # Fallback to doc_id
        return f"SHC_{self.bench}_{self.doc_id}" if self.doc_id else f"SHC_{self.bench}_unknown"

    @property
    def year(self) -> str:
        if self.citation:
            parts = self.citation.split()
            if parts and parts[0].isdigit():
                return parts[0]
        # Extract from order_date
        if self.order_date:
            try:
                return self.order_date[:4]
            except Exception:
                pass
        return "unknown"


# ══════════════════════════════════════════════════════════════════════════════
# HTTP Session
# ══════════════════════════════════════════════════════════════════════════════

class SHCSession:
    """HTTP session with Chrome impersonation for SHC website."""

    def __init__(self):
        self.session = Session(impersonate="chrome")
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": PUBLIC_HOME,
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        self.request_count = 0

    def _delay(self, min_d: float = None, max_d: float = None):
        """Human-like delay between requests."""
        lo = min_d or MIN_DELAY
        hi = max_d or MAX_DELAY
        delay = random.uniform(lo, hi)
        # Add occasional longer pauses
        if self.request_count > 0 and self.request_count % 50 == 0:
            delay += random.uniform(10, 30)
            logger.info(f"Taking a breather after {self.request_count} requests ({delay:.1f}s)")
        time.sleep(delay)
        self.request_count += 1

    def get(self, url: str, timeout: int = 30, **kwargs) -> Any:
        """GET with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    backoff = RETRY_BACKOFF * attempt
                    logger.warning(f"Retry {attempt}/{MAX_RETRIES} after {backoff}s backoff")
                    time.sleep(backoff)

                r = self.session.get(url, timeout=timeout, **kwargs)

                if r.status_code == 200:
                    return r
                elif r.status_code == 429:
                    logger.warning(f"Rate limited (429). Backing off 60s...")
                    time.sleep(60)
                elif r.status_code >= 500:
                    logger.warning(f"Server error {r.status_code}. Will retry...")
                else:
                    logger.error(f"HTTP {r.status_code} for {url}")
                    return r
            except Exception as e:
                logger.error(f"Request failed: {e}")
                if attempt == MAX_RETRIES - 1:
                    raise

        return None

    def get_binary(self, url: str, timeout: int = DOWNLOAD_TIMEOUT) -> Optional[bytes]:
        """GET binary content (PDFs)."""
        r = self.get(url, timeout=timeout)
        if r and r.status_code == 200:
            return r.content
        return None


# ══════════════════════════════════════════════════════════════════════════════
# HTML Parser
# ══════════════════════════════════════════════════════════════════════════════

class SHCParser:
    """Parses the SHC public homepage HTML to extract case metadata."""

    @staticmethod
    def identify_bench(court_text: str) -> str:
        """Identify bench code from court name string."""
        court_text_lower = court_text.lower()
        for code, patterns in BENCHES.items():
            for pattern in patterns:
                if pattern.lower() in court_text_lower:
                    return code
        # Fallback: check for known keywords
        if "karachi" in court_text_lower:
            return "KHI"
        if "hyderabad" in court_text_lower:
            return "HYD"
        if "sukkur" in court_text_lower:
            return "SUK"
        if "larkana" in court_text_lower:
            return "LAR"
        if "mirpur" in court_text_lower:
            return "MIR"
        return "KHI"  # Default to Karachi (principal seat)

    @staticmethod
    def identify_bench_from_citation(citation: str) -> str:
        """Extract bench code from citation string like '2026 SHC KHI 304'."""
        parts = citation.strip().split()
        if len(parts) >= 3:
            code = parts[2].upper()
            # Normalize MPK -> MIR
            if code == "MPK":
                return "MIR"
            if code in BENCH_CODES:
                return code
        return ""

    @staticmethod
    def parse_case_title(title_text: str) -> Dict[str, str]:
        """Parse case title to extract case number, type, parties, bench.

        Example: "4.Const. P. 1779/2024 (D.B.) City School (Pvt) Ltd V/S Province of Sindh & Others  Sindh High Court, Karachi"
        """
        result = {
            "serial": "",
            "case_number": "",
            "case_type": "",
            "petitioner": "",
            "respondent": "",
            "bench_name": "",
        }

        title_text = title_text.strip()

        # Remove leading serial number
        m = re.match(r"^\d+\.\s*", title_text)
        if m:
            result["serial"] = m.group().strip(". ")
            title_text = title_text[m.end():]

        # Extract bench type (S.B. / D.B.) from parentheses
        m_bench = re.search(r"\(([SDB.]+)\)", title_text)
        if m_bench:
            result["case_type"] = m_bench.group(1)

        # Split on V/S or v/s to get petitioner and respondent
        vs_split = re.split(r"\s+V/?S\s+", title_text, maxsplit=1, flags=re.IGNORECASE)

        if len(vs_split) == 2:
            left = vs_split[0].strip()
            right = vs_split[1].strip()

            # Left part: case number + petitioner
            # Extract case number: everything before the bench type paren or first name
            # Pattern: "Const. P. 1779/2024 (D.B.) City School (Pvt) Ltd"
            case_match = re.match(
                r"((?:Const\.\s*P\.|Cr\.Bail|Cr\.Rev|R\.A\s*\([^)]+\)|Criminal\s+Miscelleneous|I\.\s*A|II\.A\.|"
                r"Cr\.Misc\.|Spl\.\s*Cr\.\s*Anti|H\.C\.A|F\.A|S\.A|Cr\.Appeal|Cr\.Acs\.\s*Tr|"
                r"C\.P\.|IInd\s+Appeal|II\s+Appeal|Cr\.Acquittal|Cr\.Confirmation|Cr\.JR|"
                r"Suit|R\.C|Cr\.Jail\s+Petition|Tax\s+Ref|Custom\s+Ref|Cr\.Death\s+Ref|"
                r"[A-Za-z.\s]+?)\s*\d+/\d{4})\s*(?:\([^)]+\))?\s*(.*)",
                left,
                re.IGNORECASE,
            )
            if case_match:
                result["case_number"] = case_match.group(1).strip()
                result["petitioner"] = case_match.group(2).strip()
            else:
                # Simpler pattern: try to find NUMBER/YEAR
                num_match = re.search(r"(\S+\s+\d+/\d{4})", left)
                if num_match:
                    idx = num_match.end()
                    result["case_number"] = left[: idx].strip()
                    # Skip bench type paren
                    rest = left[idx:].strip()
                    rest = re.sub(r"^\([^)]+\)\s*", "", rest)
                    result["petitioner"] = rest.strip()
                else:
                    result["petitioner"] = left

            # Right part: respondent + bench name
            # Bench names: "Sindh High Court, Karachi" etc.
            bench_patterns = [
                r"\s+Sindh High Court.*$",
                r"\s+Circuit (?:at|Court).*$",
                r"\s+Bench at.*$",
            ]
            respondent = right
            for bp in bench_patterns:
                m_b = re.search(bp, right, re.IGNORECASE)
                if m_b:
                    respondent = right[: m_b.start()].strip()
                    result["bench_name"] = right[m_b.start():].strip()
                    break

            result["respondent"] = respondent

        return result

    @staticmethod
    def parse_judges(judge_text: str) -> List[Dict[str, Any]]:
        """Parse judge names and author flag.

        Example: "Hon'ble Mr. Justice Yousuf Ali Sayeed, Hon'ble Mr. Justice Abdul Mobeen Lakho(Author)"
        """
        judges = []
        if not judge_text:
            return judges

        # Split on comma (but not within names)
        parts = re.split(r",\s*(?=Hon)", judge_text)
        if len(parts) == 1:
            # Try splitting on "Hon'ble" prefix
            parts = re.split(r"(?<=\))\s*,?\s*(?=Hon)", judge_text)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            is_author = "(Author)" in part
            name = part.replace("(Author)", "").strip()
            # Clean up Hon'ble prefix
            name = re.sub(r"^Hon'ble\s+", "", name).strip()

            if name:
                judges.append({"name": name, "author": is_author})

        return judges

    @staticmethod
    def parse_date(date_str: str) -> str:
        """Parse date string like '16-FEB-26' to ISO format '2026-02-16'."""
        date_str = date_str.strip()
        if not date_str:
            return ""

        try:
            # Try DD-MON-YY format
            dt = datetime.strptime(date_str, "%d-%b-%y")
            # Fix century: if year < 47, assume 2000s; else 1900s
            if dt.year < 1947:
                dt = dt.replace(year=dt.year + 100)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

        try:
            # Try DD-MON-YYYY
            dt = datetime.strptime(date_str, "%d-%b-%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

        return date_str

    @staticmethod
    def parse_reference_text(ref_text: str) -> Dict[str, str]:
        """Parse the hidden reference textarea for extra metadata.

        Contains: case title, CITATION:, SHC Citation:, Tag:, Bench:, Source:, Order:
        """
        result = {}
        if not ref_text:
            return result

        # Extract citation
        m = re.search(r"CITATION:\s*(.+?)(?:\s+SHC|$)", ref_text)
        if m:
            result["citation"] = m.group(1).strip()

        # SHC internal citation
        m = re.search(r"SHC Citation:\s*(SHC-\d+)", ref_text)
        if m:
            result["shc_citation_id"] = m.group(1)

        # Tag/tagline
        m = re.search(r"Tag:\s*(.+?)(?:\s+Bench:|$)", ref_text, re.DOTALL)
        if m:
            result["tagline"] = m.group(1).strip()

        # Bench/judges
        m = re.search(r"Bench:\s*(.+?)(?:\s+Source:|$)", ref_text, re.DOTALL)
        if m:
            result["bench_text"] = m.group(1).strip()

        # Download hash
        m = re.search(r"hash=([A-Za-z0-9=]+)", ref_text)
        if m:
            result["doc_hash"] = m.group(1)

        return result

    def parse_blockquote(self, bq: Tag) -> Optional[SHCCase]:
        """Parse a single <blockquote> element into an SHCCase."""
        case = SHCCase()

        try:
            # 1. Download link + case title
            link = bq.find("a", href=re.compile(r"download-file\.php"))
            if not link:
                return None

            href = link.get("href", "")
            title_text = link.get_text(strip=True)

            # Parse download URL params
            doc_match = re.search(r"doc=([A-Za-z0-9=+/]+)", href)
            citation_match = re.search(r"citation=([^&\"]*)", href)

            if doc_match:
                case.doc_id = doc_match.group(1)
                case.download_url = f"{DOWNLOAD_URL}?doc={case.doc_id}"
                if citation_match and citation_match.group(1):
                    citation_param = citation_match.group(1).replace("+", " ").strip()
                    case.download_url += f"&citation={citation_param}"
                case.source_url = f"{VIEW_URL}/{case.doc_id}"

            # Parse title
            title_info = self.parse_case_title(title_text)
            case.case_number = title_info.get("case_number", "")
            case.case_type = title_info.get("case_type", "")
            case.parties = {
                "petitioner": title_info.get("petitioner", ""),
                "respondent": title_info.get("respondent", ""),
            }
            case.bench_name = title_info.get("bench_name", "")

            # 2. Citation
            cite_tag = bq.find("cite", class_="text-success")
            if cite_tag and not cite_tag.find("i"):
                citation_text = cite_tag.get_text(strip=True)
                if citation_text and not citation_text.startswith("Approved"):
                    case.citation = citation_text

            # 3. Bench identification
            if case.citation:
                bench = self.identify_bench_from_citation(case.citation)
                if bench:
                    case.bench = bench
            if not case.bench and case.bench_name:
                case.bench = self.identify_bench(case.bench_name)
            if not case.bench:
                case.bench = self.identify_bench(title_text)

            # 4. Tagline / headnote
            readmore_div = bq.find("div", class_="readmore")
            if readmore_div:
                case.tagline = readmore_div.get_text(strip=True)

            # 5. Matter category
            matter_match = re.search(r"Matter:-\s*", str(bq))
            if matter_match:
                # Find the <b> tag after "Matter:-"
                b_tag = bq.find("b")
                if b_tag:
                    # Check if it's near the Matter text
                    matter_text = b_tag.get_text(strip=True)
                    if matter_text:
                        case.matter = matter_text

            # 6. Judges
            footer = bq.find("footer")
            if footer:
                judge_span = footer.find("span", class_="text-success")
                if judge_span:
                    judge_text = judge_span.get_text(strip=True)
                    case.judges = self.parse_judges(judge_text)

            # 7. Order date
            date_cite = bq.find("cite", class_="text-danger")
            if date_cite:
                date_text = date_cite.get_text(strip=True)
                date_text = re.sub(r"^.*Order Date:\s*", "", date_text)
                case.order_date = self.parse_date(date_text)

            # 8. Approved for Reporting
            afr_cite = bq.find("cite", title="Approved for Reporting")
            if afr_cite:
                case.approved_for_reporting = True

            # 9. Downloads count
            dl_text = bq.get_text()
            dl_match = re.search(r"Downloads\s+(\d+)", dl_text)
            if dl_match:
                case.downloads_count = int(dl_match.group(1))

            # 10. Reference textarea (extra metadata)
            ref_textarea = bq.find("textarea", class_="reference")
            if ref_textarea:
                ref_text = ref_textarea.get_text()
                ref_data = self.parse_reference_text(ref_text)

                if not case.citation and ref_data.get("citation"):
                    case.citation = ref_data["citation"]
                if ref_data.get("shc_citation_id"):
                    case.shc_citation_id = ref_data["shc_citation_id"]
                if not case.tagline and ref_data.get("tagline"):
                    case.tagline = ref_data["tagline"]

            return case

        except Exception as e:
            logger.error(f"Error parsing blockquote: {e}", exc_info=True)
            return None

    def parse_homepage(self, html_content: str) -> List[SHCCase]:
        """Parse the full homepage HTML and extract all cases."""
        logger.info(f"Parsing homepage HTML ({len(html_content):,} bytes)...")
        soup = BeautifulSoup(html_content, "html.parser")

        # Find the main case listing panel
        blockquotes = soup.find_all("blockquote")
        logger.info(f"Found {len(blockquotes)} blockquotes")

        cases = []
        seen_doc_ids = set()

        for i, bq in enumerate(blockquotes):
            case = self.parse_blockquote(bq)
            if case and case.doc_id:
                # Deduplicate by doc_id
                if case.doc_id not in seen_doc_ids:
                    seen_doc_ids.add(case.doc_id)
                    cases.append(case)

            if (i + 1) % 500 == 0:
                logger.info(f"  Parsed {i + 1}/{len(blockquotes)} blockquotes, {len(cases)} unique cases")

        logger.info(f"Parsed {len(cases)} unique cases from homepage")
        return cases


# ══════════════════════════════════════════════════════════════════════════════
# PDF Text Extraction
# ══════════════════════════════════════════════════════════════════════════════

def detect_content_type(content: bytes) -> str:
    """Detect whether content is PDF, HTML, or DOCX."""
    if content[:4] == b"%PDF":
        return "pdf"
    if b"%PDF" in content[:1024]:
        return "pdf"
    if content[:2] == b"PK":
        return "docx"
    if content[:4] == b"\xd0\xcf\x11\xe0":
        return "doc"
    if content[:50].lower().startswith((b"<html", b"<!doctype", b"<head", b"\r\n<html", b"\n<html")):
        return "html"
    if b"<html" in content[:500].lower():
        return "html"
    return "unknown"


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using available library."""
    if HAS_PDFPLUMBER:
        try:
            import io
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n\n".join(pages)
        except Exception as e:
            logger.warning(f"pdfplumber extraction failed: {e}")

    if HAS_PYMUPDF:
        try:
            import io
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pages = []
            for page in doc:
                pages.append(page.get_text())
            return "\n\n".join(pages)
        except Exception as e:
            logger.warning(f"PyMuPDF extraction failed: {e}")

    logger.warning("No PDF extraction library available (install pdfplumber or PyMuPDF)")
    return ""


def extract_text_from_html(html_bytes: bytes) -> str:
    """Extract text from HTML content (some judgments are HTML, not PDF)."""
    try:
        # Try to decode
        for encoding in ['utf-8', 'windows-1252', 'iso-8859-1']:
            try:
                html_str = html_bytes.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            html_str = html_bytes.decode('utf-8', errors='replace')

        soup = BeautifulSoup(html_str, "html.parser")
        # Remove script and style elements
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return text
    except Exception as e:
        logger.warning(f"HTML text extraction failed: {e}")
        return ""


def extract_judgment_text(content: bytes) -> Tuple[str, str]:
    """Extract text from judgment content. Returns (text, content_type)."""
    ctype = detect_content_type(content)

    if ctype == "pdf":
        return extract_text_from_pdf(content), "pdf"
    elif ctype == "html":
        return extract_text_from_html(content), "html"
    elif ctype in ("doc", "docx"):
        logger.warning(f"DOC/DOCX content — text extraction not supported yet")
        return "", ctype
    else:
        # Try PDF first, then HTML
        text = extract_text_from_pdf(content)
        if text:
            return text, "pdf"
        text = extract_text_from_html(content)
        if text:
            return text, "html"
        return "", "unknown"


# ══════════════════════════════════════════════════════════════════════════════
# Progress Tracking
# ══════════════════════════════════════════════════════════════════════════════

class ProgressTracker:
    """Tracks scraping progress for resume capability."""

    def __init__(self, progress_file: Path = PROGRESS_FILE):
        self.progress_file = progress_file
        self.data = self._load()

    def _load(self) -> Dict:
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "last_updated": "",
            "total_discovered": 0,
            "total_downloaded": 0,
            "total_failed": 0,
            "downloaded": {},  # doc_id -> {citation, bench, status, timestamp}
            "failed": {},     # doc_id -> {error, attempts, last_attempt}
            "stats_by_bench": {},
            "stats_by_year": {},
        }

    def save(self):
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.data["total_downloaded"] = len(self.data["downloaded"])
        self.data["total_failed"] = len(self.data["failed"])

        with open(self.progress_file, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def is_downloaded(self, doc_id: str) -> bool:
        return doc_id in self.data["downloaded"]

    def mark_downloaded(self, case: SHCCase):
        self.data["downloaded"][case.doc_id] = {
            "citation": case.citation,
            "bench": case.bench,
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        # Update stats
        bench = case.bench or "UNKNOWN"
        year = case.year or "unknown"

        if bench not in self.data["stats_by_bench"]:
            self.data["stats_by_bench"][bench] = 0
        self.data["stats_by_bench"][bench] += 1

        if year not in self.data["stats_by_year"]:
            self.data["stats_by_year"][year] = 0
        self.data["stats_by_year"][year] += 1

    def mark_failed(self, doc_id: str, error: str):
        existing = self.data["failed"].get(doc_id, {"attempts": 0})
        self.data["failed"][doc_id] = {
            "error": error,
            "attempts": existing["attempts"] + 1,
            "last_attempt": datetime.now(timezone.utc).isoformat(),
        }

    def show_status(self):
        print("\n" + "=" * 60)
        print("SHC Scraper Progress")
        print("=" * 60)
        print(f"Last updated:     {self.data.get('last_updated', 'never')}")
        print(f"Total discovered: {self.data.get('total_discovered', 0)}")
        print(f"Total downloaded: {len(self.data.get('downloaded', {}))}")
        print(f"Total failed:     {len(self.data.get('failed', {}))}")
        print()

        bench_stats = self.data.get("stats_by_bench", {})
        if bench_stats:
            print("By Bench:")
            for bench in sorted(bench_stats):
                print(f"  {bench}: {bench_stats[bench]}")
            print()

        year_stats = self.data.get("stats_by_year", {})
        if year_stats:
            print("By Year:")
            for year in sorted(year_stats, reverse=True):
                print(f"  {year}: {year_stats[year]}")
        print("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# File Output
# ══════════════════════════════════════════════════════════════════════════════

class FileWriter:
    """Handles 4-format output for each case."""

    @staticmethod
    def _ensure_dirs(bench: str, year: str):
        """Create output directories."""
        (SHC_DIR / bench / year).mkdir(parents=True, exist_ok=True)
        (SHC_DIR / bench / year / "original").mkdir(parents=True, exist_ok=True)
        (HTML_DIR / bench / year).mkdir(parents=True, exist_ok=True)

    @staticmethod
    def save_case(case: SHCCase, content: Optional[bytes] = None, content_type: str = "pdf"):
        """Save case in all 4 formats."""
        bench = case.bench or "UNKNOWN"
        year = case.year or "unknown"
        file_key = case.file_key

        FileWriter._ensure_dirs(bench, year)

        # 1. JSON
        json_path = SHC_DIR / bench / year / f"{file_key}.json"
        case_dict = case.to_dict()
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(case_dict, f, indent=2, ensure_ascii=False)

        # 2. Original file saved as-is (PDF or HTML)
        if content:
            ext = "pdf" if content_type == "pdf" else "html" if content_type == "html" else "bin"
            orig_path = SHC_DIR / bench / year / "original" / f"{file_key}.{ext}"
            with open(orig_path, "wb") as f:
                f.write(content)

        # 3. Readable HTML
        readable_html = FileWriter._generate_readable_html(case)
        html_path = HTML_DIR / bench / year / f"{file_key}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(readable_html)

        # 4. JSONL — bench-year file + all_court_cases.jsonl
        jsonl_line = json.dumps(case_dict, ensure_ascii=False) + "\n"

        bench_jsonl = COURT_DIR / f"SHC_{bench}_{year}.jsonl"
        with open(bench_jsonl, "a", encoding="utf-8") as f:
            f.write(jsonl_line)

        ALL_JSONL.parent.mkdir(parents=True, exist_ok=True)
        with open(ALL_JSONL, "a", encoding="utf-8") as f:
            f.write(jsonl_line)

        return json_path

    @staticmethod
    def _generate_readable_html(case: SHCCase) -> str:
        """Generate a clean, readable HTML page for the case."""
        judges_html = ""
        for j in case.judges:
            author_tag = " <strong>(Author)</strong>" if j.get("author") else ""
            judges_html += f"<li>{html_mod.escape(j.get('name', ''))}{author_tag}</li>\n"

        advocates_html = ""
        for a in case.advocates:
            reg = f" ({a.get('registration', '')})" if a.get("registration") else ""
            advocates_html += f"<li>{html_mod.escape(a.get('name', ''))}{reg}</li>\n"

        afr_badge = '<span style="color:green;font-weight:bold">✓ Approved for Reporting</span>' if case.approved_for_reporting else ""

        judgment_html = html_mod.escape(case.judgment_text).replace("\n", "<br>\n") if case.judgment_text else "<em>No text extracted</em>"

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{html_mod.escape(case.citation or case.case_number)}</title>
    <style>
        body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px; line-height: 1.7; color: #333; }}
        h1 {{ font-size: 1.4em; color: #1a5276; border-bottom: 2px solid #1a5276; padding-bottom: 10px; }}
        .meta {{ background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        .meta dt {{ font-weight: bold; color: #555; }}
        .meta dd {{ margin-left: 0; margin-bottom: 8px; }}
        .tagline {{ font-style: italic; background: #eaf2f8; padding: 10px 15px; border-left: 4px solid #2980b9; margin: 15px 0; }}
        .judgment {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; }}
        .source {{ font-size: 0.85em; color: #888; margin-top: 30px; }}
    </style>
</head>
<body>
    <h1>{html_mod.escape(case.citation or case.case_number)}</h1>
    {afr_badge}

    <div class="meta">
        <dl>
            <dt>Case Number</dt><dd>{html_mod.escape(case.case_number)} ({html_mod.escape(case.case_type)})</dd>
            <dt>Petitioner</dt><dd>{html_mod.escape(case.parties.get('petitioner', ''))}</dd>
            <dt>Respondent</dt><dd>{html_mod.escape(case.parties.get('respondent', ''))}</dd>
            <dt>Bench</dt><dd>{html_mod.escape(case.bench_name or case.bench)}</dd>
            <dt>Matter</dt><dd>{html_mod.escape(case.matter)}</dd>
            <dt>Order Date</dt><dd>{html_mod.escape(case.order_date)}</dd>
        </dl>
    </div>

    {"<div class='tagline'>" + html_mod.escape(case.tagline) + "</div>" if case.tagline else ""}

    <h3>Bench</h3>
    <ul>{judges_html}</ul>

    {"<h3>Advocates</h3><ul>" + advocates_html + "</ul>" if advocates_html else ""}

    <div class="judgment">
        <h2>Judgment</h2>
        {judgment_html}
    </div>

    <div class="source">
        Source: Sindh High Court Caselaw Database<br>
        URL: <a href="{html_mod.escape(case.source_url)}">{html_mod.escape(case.source_url)}</a><br>
        Fetched: {html_mod.escape(case.fetched_at)}
    </div>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════════════════
# Main Scraper
# ══════════════════════════════════════════════════════════════════════════════

class SHCScraper:
    """Main scraper orchestrator."""

    def __init__(self):
        self.session = SHCSession()
        self.parser = SHCParser()
        self.progress = ProgressTracker()
        self.writer = FileWriter()

    def fetch_homepage(self) -> str:
        """Fetch the full homepage HTML (12+ MB)."""
        logger.info("Fetching SHC public homepage (this may take a while — 12+ MB)...")
        r = self.session.get(PUBLIC_HOME, timeout=HOMEPAGE_TIMEOUT)
        if r and r.status_code == 200:
            logger.info(f"Homepage fetched: {len(r.text):,} bytes")
            return r.text
        else:
            status = r.status_code if r else "no response"
            raise RuntimeError(f"Failed to fetch homepage: HTTP {status}")

    def discover_cases(
        self,
        bench_filter: Optional[str] = None,
        year_filter: Optional[str] = None,
    ) -> List[SHCCase]:
        """Discover all cases from the homepage, optionally filtered."""
        html_content = self.fetch_homepage()
        all_cases = self.parser.parse_homepage(html_content)
        self.progress.data["total_discovered"] = len(all_cases)

        # Apply filters
        filtered = all_cases
        if bench_filter:
            bench_filter = bench_filter.upper()
            filtered = [c for c in filtered if c.bench == bench_filter]
            logger.info(f"Filtered to bench {bench_filter}: {len(filtered)} cases")

        if year_filter:
            filtered = [c for c in filtered if c.year == str(year_filter)]
            logger.info(f"Filtered to year {year_filter}: {len(filtered)} cases")

        return filtered

    def download_judgment(self, case: SHCCase) -> Optional[bytes]:
        """Download the judgment file for a case (may be PDF or HTML)."""
        if not case.doc_id:
            return None

        url = case.download_url or f"{DOWNLOAD_URL}?doc={case.doc_id}"
        if case.citation:
            url = f"{DOWNLOAD_URL}?doc={case.doc_id}&citation={case.citation.replace(' ', '+')}"

        self.session._delay()
        content = self.session.get_binary(url, timeout=DOWNLOAD_TIMEOUT)

        if content and len(content) > 100:
            return content
        return None

    def process_case(self, case: SHCCase) -> bool:
        """Download and save a single case. Returns True on success."""
        # Skip if already downloaded
        if self.progress.is_downloaded(case.doc_id):
            return True

        try:
            case.fetched_at = datetime.now(timezone.utc).isoformat()

            # Download judgment (may be PDF or HTML)
            content = self.download_judgment(case)
            content_type = "unknown"
            if content:
                # Extract text + detect type
                case.judgment_text, content_type = extract_judgment_text(content)
                logger.info(
                    f"  Downloaded {case.citation or case.doc_id}: "
                    f"{len(content):,} bytes ({content_type}), "
                    f"{len(case.judgment_text):,} chars text"
                )
            else:
                logger.warning(f"  No content for {case.citation or case.doc_id}")

            # Save in all formats
            json_path = self.writer.save_case(case, content, content_type)

            # Mark progress
            self.progress.mark_downloaded(case)

            return True

        except Exception as e:
            logger.error(f"  Failed to process {case.citation or case.doc_id}: {e}")
            self.progress.mark_failed(case.doc_id, str(e))
            return False

    def run(
        self,
        bench_filter: Optional[str] = None,
        year_filter: Optional[str] = None,
        limit: Optional[int] = None,
        list_only: bool = False,
    ):
        """Main entry point."""
        start_time = time.time()

        # Discover cases
        cases = self.discover_cases(bench_filter, year_filter)

        if not cases:
            logger.warning("No cases found matching filters")
            return

        # Count already downloaded
        already_done = sum(1 for c in cases if self.progress.is_downloaded(c.doc_id))
        to_download = [c for c in cases if not self.progress.is_downloaded(c.doc_id)]

        logger.info(f"\nDiscovered: {len(cases)} cases")
        logger.info(f"Already downloaded: {already_done}")
        logger.info(f"To download: {len(to_download)}")

        if list_only:
            self._print_case_summary(cases)
            self.progress.save()
            return

        if limit:
            to_download = to_download[:limit]
            logger.info(f"Limited to first {limit} cases")

        # Download cases
        success = 0
        failed = 0

        for i, case in enumerate(to_download):
            logger.info(f"\n[{i + 1}/{len(to_download)}] Processing: {case.citation or case.case_number or case.doc_id}")

            if self.process_case(case):
                success += 1
            else:
                failed += 1

            # Save progress periodically
            if (i + 1) % 10 == 0:
                self.progress.save()
                elapsed = time.time() - start_time
                rate = (success + failed) / (elapsed / 60)
                logger.info(
                    f"  Progress: {success} ok, {failed} failed, "
                    f"{rate:.1f} cases/min, "
                    f"elapsed {elapsed / 60:.1f}min"
                )

        # Final save
        self.progress.save()

        elapsed = time.time() - start_time
        logger.info(f"\n{'=' * 60}")
        logger.info(f"SHC Scraper Complete")
        logger.info(f"  Downloaded: {success}")
        logger.info(f"  Failed:     {failed}")
        logger.info(f"  Skipped:    {already_done}")
        logger.info(f"  Time:       {elapsed / 60:.1f} minutes")
        logger.info(f"{'=' * 60}")

    def _print_case_summary(self, cases: List[SHCCase]):
        """Print summary of discovered cases."""
        print(f"\n{'=' * 60}")
        print(f"SHC Cases Discovered: {len(cases)}")
        print(f"{'=' * 60}")

        # Stats by bench
        by_bench: Dict[str, int] = {}
        by_year: Dict[str, int] = {}
        with_citation = 0
        afr = 0

        for c in cases:
            bench = c.bench or "UNKNOWN"
            by_bench[bench] = by_bench.get(bench, 0) + 1

            year = c.year or "unknown"
            by_year[year] = by_year.get(year, 0) + 1

            if c.citation:
                with_citation += 1
            if c.approved_for_reporting:
                afr += 1

        print(f"\nWith citation: {with_citation}")
        print(f"Approved for reporting: {afr}")

        print(f"\nBy Bench:")
        for bench in sorted(by_bench):
            print(f"  {bench}: {by_bench[bench]}")

        print(f"\nBy Year:")
        for year in sorted(by_year, reverse=True):
            print(f"  {year}: {by_year[year]}")

        # Show first 10 cases
        print(f"\nFirst 10 cases:")
        for c in cases[:10]:
            print(f"  {c.citation or 'NO CITATION'} | {c.case_number} | {c.bench} | {c.order_date}")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="SHC Scraper — Sindh High Court Caselaw Database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python shc_scraper.py                          # Scrape all benches, all years
  python shc_scraper.py --bench KHI              # Karachi bench only
  python shc_scraper.py --year 2025              # Year 2025 only
  python shc_scraper.py --bench KHI --year 2026  # Karachi 2026
  python shc_scraper.py --status                 # Show progress
  python shc_scraper.py --list-only              # Parse homepage, show stats
  python shc_scraper.py --limit 10               # Download first 10 only
        """,
    )
    parser.add_argument("--bench", choices=["KHI", "HYD", "SUK", "LAR", "MIR"], help="Filter by bench")
    parser.add_argument("--year", type=str, help="Filter by year (e.g. 2025)")
    parser.add_argument("--status", action="store_true", help="Show progress status")
    parser.add_argument("--list-only", action="store_true", help="List cases without downloading")
    parser.add_argument("--limit", type=int, help="Limit number of downloads")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.status:
        tracker = ProgressTracker()
        tracker.show_status()
        return

    scraper = SHCScraper()
    scraper.run(
        bench_filter=args.bench,
        year_filter=args.year,
        limit=args.limit,
        list_only=args.list_only,
    )


if __name__ == "__main__":
    main()
