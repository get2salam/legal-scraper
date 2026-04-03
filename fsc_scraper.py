#!/usr/bin/env python3
"""
FSC Scraper — Federal Shariat Court of Pakistan
=================================================
Scrapes judgments from https://www.federalshariatcourt.gov.pk

Sources:
  1. Leading Judgments page (/en/leading-judgements/) — 20 landmark cases
  2. All Judgments database (/alljud.php) — ~12,000+ case records, ~825+ with PDFs

The site is a PHP/WordPress hybrid with judgments hosted at:
  https://www.federalshariatcourt.gov.pk/Judgments/{filename}.pdf

When the live site is unreachable, falls back to Wayback Machine cached copies.
Established 1980 — 45 years of shariat/Islamic law judgments.

Storage: data_v2/court_cases/FSC/YEAR/
Formats: JSON + PDF + Readable HTML + JSONL
Progress: data_v2/court_cases/fsc_progress.json

INTERNAL USE ONLY — never push to public GitHub.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from html import unescape
from urllib.parse import quote, unquote, urljoin

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi required. Install: pip install curl_cffi")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
    print("WARNING: PyMuPDF not installed. PDF text extraction disabled.")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.federalshariatcourt.gov.pk"
LEADING_URL = f"{BASE_URL}/en/leading-judgements/"
ALLJUD_URL = f"{BASE_URL}/alljud.php"
JUDGMENTS_PATH = f"{BASE_URL}/Judgments/"

# Wayback Machine fallbacks
WAYBACK_PREFIX = "https://web.archive.org/web"
WAYBACK_LEADING = f"{WAYBACK_PREFIX}/2025/{BASE_URL}/en/leading-judgements/"
WAYBACK_ALLJUD = f"{WAYBACK_PREFIX}/20251206160214/{BASE_URL}/alljud.php"

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
CASES_DIR = DATA_ROOT / "court_cases" / "FSC"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "FSC"
JSONL_DIR = DATA_ROOT / "court_cases"
JSONL_FILE = JSONL_DIR / "fsc_cases.jsonl"
PROGRESS_FILE = DATA_ROOT / "court_cases" / "fsc_progress.json"
LOG_DIR = PROJECT_ROOT / "logs"

MIN_DELAY = 5
MAX_DELAY = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 5
CONNECT_TIMEOUT = 20  # Shorter connect timeout (site is often down)

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "fsc_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("fsc_scraper")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

class FSCSession:
    """Wrapper around curl_cffi with TLS impersonation and throttling."""

    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome")
        self._last_request_time = 0
        self._use_wayback = False  # Auto-fallback to Wayback Machine

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        delay = MIN_DELAY + random.random() * (MAX_DELAY - MIN_DELAY)
        remaining = delay - elapsed
        if remaining > 0:
            log.debug(f"Throttling {remaining:.1f}s")
            time.sleep(remaining)
        self._last_request_time = time.time()

    def get(self, url: str, timeout: int = 60, allow_wayback: bool = True) -> Optional[cffi_requests.Response]:
        """GET with retry logic and Wayback fallback."""
        # Use shorter timeout for the live site (often down)
        live_timeout = min(timeout, CONNECT_TIMEOUT) if not url.startswith(WAYBACK_PREFIX) else timeout
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                resp = self.session.get(url, timeout=live_timeout)
                if resp.status_code == 200:
                    return resp
                log.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt})")
            except Exception as e:
                log.warning(f"Request failed for {url}: {e} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)

        # Fallback to Wayback Machine
        if allow_wayback and not url.startswith(WAYBACK_PREFIX):
            wayback_url = f"{WAYBACK_PREFIX}/2025/{url}"
            log.info(f"Trying Wayback Machine: {wayback_url}")
            try:
                self._throttle()
                resp = self.session.get(wayback_url, timeout=timeout)
                if resp.status_code == 200:
                    self._use_wayback = True
                    return resp
            except Exception as e:
                log.error(f"Wayback also failed: {e}")

        return None

    def download_pdf(self, url: str, dest: Path, timeout: int = 120) -> bool:
        """Download PDF file with retry logic."""
        if dest.exists() and dest.stat().st_size > 1000:
            log.debug(f"PDF already exists: {dest}")
            return True

        dest.parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200 and len(resp.content) > 500:
                    dest.write_bytes(resp.content)
                    log.info(f"Downloaded PDF ({len(resp.content):,} bytes): {dest.name}")
                    return True
                log.warning(f"PDF download HTTP {resp.status_code}, size {len(resp.content)} for {url}")
            except Exception as e:
                log.warning(f"PDF download failed: {e} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)

        # Try Wayback
        if not url.startswith(WAYBACK_PREFIX):
            wayback_url = f"{WAYBACK_PREFIX}/2025/{url}"
            log.info(f"Trying PDF from Wayback: {wayback_url}")
            try:
                self._throttle()
                resp = self.session.get(wayback_url, timeout=timeout)
                if resp.status_code == 200 and len(resp.content) > 500:
                    dest.write_bytes(resp.content)
                    log.info(f"Downloaded PDF from Wayback ({len(resp.content):,} bytes)")
                    return True
            except Exception as e:
                log.error(f"Wayback PDF download also failed: {e}")

        return False


# ---------------------------------------------------------------------------
# Progress Manager
# ---------------------------------------------------------------------------

class ProgressManager:
    """Track scraping progress for resume capability."""

    def __init__(self, path: Path):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                return self._default()
        return self._default()

    def _default(self) -> dict:
        return {
            "court": "FSC",
            "court_name": "Federal Shariat Court of Pakistan",
            "started_at": None,
            "last_updated": None,
            "sources": {
                "leading_judgments": {
                    "discovered": 0,
                    "downloaded": 0,
                    "completed_ids": [],
                },
                "all_judgments": {
                    "discovered": 0,
                    "downloaded": 0,
                    "completed_ids": [],
                    "last_row_processed": 0,
                },
            },
            "total_pdfs_downloaded": 0,
            "total_text_extracted": 0,
            "errors": [],
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def is_completed(self, case_id: str, source: str = "all_judgments") -> bool:
        return case_id in self.data["sources"][source]["completed_ids"]

    def mark_completed(self, case_id: str, source: str = "all_judgments"):
        if case_id not in self.data["sources"][source]["completed_ids"]:
            self.data["sources"][source]["completed_ids"].append(case_id)
            self.data["sources"][source]["downloaded"] += 1

    def log_error(self, error: str):
        self.data["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error[:500],
        })
        # Keep last 100 errors
        self.data["errors"] = self.data["errors"][-100:]


# ---------------------------------------------------------------------------
# HTML Parsing Helpers
# ---------------------------------------------------------------------------

def clean_text(text: str) -> str:
    """Clean HTML text content."""
    text = unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_year_from_text(text: str) -> Optional[str]:
    """Extract a year (1980-2030) from text."""
    matches = re.findall(r'\b(19[89]\d|20[0-3]\d)\b', text)
    if matches:
        # Prefer judgment date year, usually the last year mentioned
        return matches[-1]
    return None


def extract_year_from_case_no(case_no: str) -> Optional[str]:
    """Extract year from case number like 'Cr.A.No.122/I/2008'."""
    matches = re.findall(r'\b(19[789]\d|20[0-3]\d)\b', case_no)
    return matches[-1] if matches else None


def extract_year_from_date(date_str: str) -> Optional[str]:
    """Extract year from date like '1981-05-27' or '19.03.2025'."""
    m = re.search(r'(\d{4})-\d{2}-\d{2}', date_str)
    if m:
        return m.group(1)
    m = re.search(r'\d{2}\.\d{2}\.(\d{4})', date_str)
    if m:
        return m.group(1)
    return extract_year_from_text(date_str)


def make_case_id(case_no: str, sr_no: int) -> str:
    """Create a stable unique ID for a case."""
    slug = re.sub(r'[^a-zA-Z0-9]+', '-', case_no.strip()).strip('-').lower()
    if not slug:
        slug = f"case-{sr_no}"
    return f"FSC-{slug}"


def sanitize_filename(name: str) -> str:
    """Sanitize filename for safe filesystem use."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:200] if name else "unknown"


# ---------------------------------------------------------------------------
# Text Extraction
# ---------------------------------------------------------------------------

def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from PDF using PyMuPDF."""
    if fitz is None:
        return ""
    try:
        doc = fitz.open(str(pdf_path))
        text_parts = []
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        return "\n".join(text_parts).strip()
    except Exception as e:
        log.warning(f"PDF text extraction failed for {pdf_path.name}: {e}")
        return ""


# ---------------------------------------------------------------------------
# Output Generation
# ---------------------------------------------------------------------------

def save_case(case: dict, year_dir: Path, html_year_dir: Path):
    """Save case in JSON + readable HTML formats."""
    case_id = case["case_id"]
    safe_id = sanitize_filename(case_id)

    # JSON
    json_path = year_dir / f"{safe_id}.json"
    json_path.write_text(json.dumps(case, indent=2, ensure_ascii=False), encoding="utf-8")

    # Readable HTML
    html_path = html_year_dir / f"{safe_id}.html"
    html_content = generate_readable_html(case)
    html_path.write_text(html_content, encoding="utf-8")


def generate_readable_html(case: dict) -> str:
    """Generate a clean, readable HTML page for a judgment."""
    title = case.get("case_title", case.get("case_no", "FSC Judgment"))
    case_no = case.get("case_no", "")
    subject = case.get("case_subject", "")
    judge = case.get("author_judge", "")
    date = case.get("judgment_date", "")
    citation = case.get("citation", "")
    sections = case.get("sections", "")
    text = case.get("full_text", "")
    source = case.get("source", "")

    text_html = ""
    if text:
        paragraphs = text.split("\n")
        text_html = "\n".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Federal Shariat Court</title>
    <style>
        body {{ font-family: Georgia, serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; color: #333; }}
        .header {{ border-bottom: 3px solid #006400; padding-bottom: 15px; margin-bottom: 20px; }}
        .court-name {{ color: #006400; font-size: 1.4em; font-weight: bold; }}
        .meta {{ background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .meta dt {{ font-weight: bold; color: #006400; }}
        .meta dd {{ margin: 0 0 10px 0; }}
        .judgment-text {{ text-align: justify; }}
        .footer {{ border-top: 1px solid #ddd; margin-top: 30px; padding-top: 10px; font-size: 0.85em; color: #666; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="court-name">Federal Shariat Court of Pakistan</div>
        <h1>{title}</h1>
    </div>
    <dl class="meta">
        {"<dt>Case Number</dt><dd>" + case_no + "</dd>" if case_no else ""}
        {"<dt>Subject</dt><dd>" + subject + "</dd>" if subject else ""}
        {"<dt>Author Judge</dt><dd>" + judge + "</dd>" if judge else ""}
        {"<dt>Judgment Date</dt><dd>" + date + "</dd>" if date else ""}
        {"<dt>Citation</dt><dd>" + citation + "</dd>" if citation else ""}
        {"<dt>Sections</dt><dd>" + sections + "</dd>" if sections else ""}
        <dt>Source</dt><dd>{source}</dd>
    </dl>
    <div class="judgment-text">
        {text_html if text_html else "<p><em>Full text not available. See PDF.</em></p>"}
    </div>
    <div class="footer">
        <p>Source: Federal Shariat Court of Pakistan | Scraped: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</p>
    </div>
</body>
</html>"""


def append_to_jsonl(case: dict):
    """Append case to JSONL file."""
    JSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(case, ensure_ascii=False)
    with open(JSONL_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# Leading Judgments Parser
# ---------------------------------------------------------------------------

def parse_leading_judgments(html: str) -> List[dict]:
    """Parse the /en/leading-judgements/ page."""
    cases = []

    # Remove Wayback Machine toolbar if present
    html = re.sub(r'<!-- BEGIN WAYBACK TOOLBAR INSERT -->.*?<!-- END WAYBACK TOOLBAR INSERT -->', '', html, flags=re.DOTALL)

    # Find the table
    table_match = re.search(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
    if not table_match:
        log.warning("No table found on leading judgments page")
        return cases

    table_html = table_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) < 4:
            continue

        sr_no_text = clean_text(cells[0])
        if not re.search(r'\d+', sr_no_text):
            continue  # Skip header row

        sr_no = int(re.search(r'\d+', sr_no_text).group())
        title = clean_text(cells[1])
        reference = clean_text(cells[2])
        download_cell = cells[3]

        # Extract PDF link
        pdf_match = re.search(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', download_cell, re.IGNORECASE)
        pdf_url = ""
        if pdf_match:
            pdf_url = pdf_match.group(1)
            # Clean Wayback URL if present
            wb_match = re.search(r'https?://www\.federalshariatcourt\.gov\.pk/(Judgments/[^"\']+)', pdf_url)
            if wb_match:
                pdf_url = f"{BASE_URL}/{wb_match.group(1)}"
            elif not pdf_url.startswith('http'):
                pdf_url = f"{BASE_URL}/{pdf_url}"

        # Extract date and year
        date_match = re.search(r'Date of Decision:\s*(\d{2}\.\d{2}\.\d{4})', reference)
        judgment_date = date_match.group(1) if date_match else ""
        year = extract_year_from_date(judgment_date) if judgment_date else extract_year_from_text(reference)

        # Extract citation
        citation_match = re.search(r'(PLD[- ]\d{4}[- ]FSC[- ]\d+)', reference)
        citation = citation_match.group(1) if citation_match else ""

        case_id = make_case_id(title or f"leading-{sr_no}", sr_no)

        cases.append({
            "case_id": case_id,
            "sr_no": sr_no,
            "case_title": title,
            "case_no": "",
            "case_subject": "",
            "author_judge": "",
            "judgment_date": judgment_date,
            "citation": citation,
            "sections": "",
            "year": year or "unknown",
            "pdf_url": pdf_url,
            "pdf_filename": "",
            "full_text": "",
            "source": "leading_judgments",
            "source_url": LEADING_URL,
            "court": "FSC",
            "court_name": "Federal Shariat Court of Pakistan",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Parsed {len(cases)} leading judgments")
    return cases


# ---------------------------------------------------------------------------
# All Judgments Parser (alljud.php)
# ---------------------------------------------------------------------------

def parse_all_judgments(html: str) -> List[dict]:
    """Parse the /alljud.php page with all judgment records."""
    cases = []

    # Remove Wayback Machine toolbar
    html = re.sub(r'<!-- BEGIN WAYBACK TOOLBAR INSERT -->.*?<!-- END WAYBACK TOOLBAR INSERT -->', '', html, flags=re.DOTALL)

    # Find the inner data table (the one with border="1" containing actual data)
    # The structure is: outer table > td > inner table with case data
    inner_table_match = re.search(r'<table border="1">(.*)', html, re.DOTALL)
    if not inner_table_match:
        log.warning("No inner table found in alljud.php")
        return cases

    table_html = inner_table_match.group(1)
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)

    log.info(f"Found {len(rows)} rows in all judgments table")

    for row_html in rows:
        cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL)
        if len(cells) < 9:
            continue

        sr_no_text = clean_text(cells[0])
        if not re.search(r'^\d+$', sr_no_text.strip()):
            continue  # Skip header/empty rows

        sr_no = int(sr_no_text.strip())
        case_subject = clean_text(cells[1])
        case_no = clean_text(cells[2])
        case_title = clean_text(cells[3])
        author_judge = clean_text(cells[4])
        judgment_date = clean_text(cells[5])
        sections = clean_text(cells[6])
        citation = clean_text(cells[7])
        download_cell = cells[8]

        # Skip test/junk entries
        if case_no in ('fshgjj', 'cr.abc', 'fgdfgdfgdfgdfg') or case_title in ('sajskl', 'dklj', 'fdgdfgdfgdf'):
            continue

        # Extract PDF link
        pdf_match = re.search(r'href=["\']([^"\']*Judgments/[^"\']*\.pdf[^"\']*)["\']', download_cell, re.IGNORECASE)
        pdf_url = ""
        pdf_filename = ""
        if pdf_match:
            raw_url = pdf_match.group(1)
            # Clean Wayback URL prefix if present
            wb_match = re.search(r'(Judgments/.*\.pdf)', raw_url, re.IGNORECASE)
            if wb_match:
                pdf_relative = wb_match.group(1)
                pdf_url = f"{BASE_URL}/{pdf_relative}"
                pdf_filename = unquote(pdf_relative.replace('Judgments/', ''))

        # Determine year
        year = None
        if judgment_date:
            year = extract_year_from_date(judgment_date)
        if not year and case_no:
            year = extract_year_from_case_no(case_no)
        if not year and citation:
            year = extract_year_from_text(citation)
        if not year:
            year = "unknown"

        case_id = make_case_id(case_no or f"row-{sr_no}", sr_no)

        cases.append({
            "case_id": case_id,
            "sr_no": sr_no,
            "case_title": case_title,
            "case_no": case_no,
            "case_subject": case_subject,
            "author_judge": author_judge,
            "judgment_date": judgment_date,
            "citation": citation,
            "sections": sections,
            "year": year,
            "pdf_url": pdf_url,
            "pdf_filename": pdf_filename,
            "full_text": "",
            "source": "all_judgments",
            "source_url": ALLJUD_URL,
            "court": "FSC",
            "court_name": "Federal Shariat Court of Pakistan",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        })

    log.info(f"Parsed {len(cases)} case records from alljud.php ({sum(1 for c in cases if c['pdf_url'])} with PDFs)")
    return cases


# ---------------------------------------------------------------------------
# Scraper Core
# ---------------------------------------------------------------------------

class FSCScraper:
    """Federal Shariat Court judgment scraper."""

    def __init__(self):
        self.session = FSCSession()
        self.progress = ProgressManager(PROGRESS_FILE)

    def discover(self) -> Dict[str, List[dict]]:
        """Discover all available judgments from both sources."""
        results = {"leading": [], "all_judgments": []}

        # 1. Leading Judgments
        log.info("Fetching leading judgments page...")
        resp = self.session.get(LEADING_URL)
        if resp:
            results["leading"] = parse_leading_judgments(resp.text)
            self.progress.data["sources"]["leading_judgments"]["discovered"] = len(results["leading"])
        else:
            log.warning("Could not fetch leading judgments page")

        # 2. All Judgments (alljud.php) — large page, longer timeout
        log.info("Fetching all judgments database (this may take a while)...")
        resp = self.session.get(ALLJUD_URL, timeout=120)
        if resp:
            results["all_judgments"] = parse_all_judgments(resp.text)
            self.progress.data["sources"]["all_judgments"]["discovered"] = len(results["all_judgments"])
        else:
            log.warning("Could not fetch all judgments page")

        self.progress.save()
        return results

    def process_case(self, case: dict, source: str = "all_judgments") -> bool:
        """Download PDF, extract text, save in all formats."""
        case_id = case["case_id"]

        if self.progress.is_completed(case_id, source):
            log.debug(f"Already completed: {case_id}")
            return True

        year = case.get("year", "unknown")
        year_dir = CASES_DIR / year
        year_dir.mkdir(parents=True, exist_ok=True)

        html_year_dir = HTML_DIR / year
        html_year_dir.mkdir(parents=True, exist_ok=True)

        # Download PDF if available
        if case["pdf_url"]:
            safe_filename = sanitize_filename(case.get("pdf_filename") or f"{case_id}.pdf")
            if not safe_filename.endswith('.pdf'):
                safe_filename += '.pdf'
            pdf_path = year_dir / safe_filename

            if self.session.download_pdf(case["pdf_url"], pdf_path):
                case["pdf_local_path"] = str(pdf_path.relative_to(PROJECT_ROOT))
                self.progress.data["total_pdfs_downloaded"] += 1

                # Extract text
                full_text = extract_text_from_pdf(pdf_path)
                if full_text:
                    case["full_text"] = full_text
                    self.progress.data["total_text_extracted"] += 1
            else:
                log.warning(f"Failed to download PDF for {case_id}")
                self.progress.log_error(f"PDF download failed: {case_id} -> {case['pdf_url']}")

        # Save JSON + HTML
        save_case(case, year_dir, html_year_dir)

        # Append to JSONL
        append_to_jsonl(case)

        # Mark completed
        self.progress.mark_completed(case_id, source)
        self.progress.save()

        return True

    def scrape_leading(self, cases: List[dict]):
        """Process leading judgments."""
        log.info(f"Processing {len(cases)} leading judgments...")
        for i, case in enumerate(cases, 1):
            try:
                log.info(f"[Leading {i}/{len(cases)}] {case['case_title'][:60]}...")
                self.process_case(case, source="leading_judgments")
            except Exception as e:
                log.error(f"Error processing leading judgment {case['case_id']}: {e}")
                self.progress.log_error(f"Leading judgment error: {case['case_id']}: {e}")

    def scrape_all_judgments(self, cases: List[dict]):
        """Process all judgments from alljud.php."""
        # Only process cases with PDF links (others have no downloadable content)
        cases_with_pdfs = [c for c in cases if c["pdf_url"]]
        cases_without_pdfs = [c for c in cases if not c["pdf_url"]]

        log.info(f"Processing {len(cases_with_pdfs)} cases with PDFs (skipping {len(cases_without_pdfs)} without PDFs)...")

        for i, case in enumerate(cases_with_pdfs, 1):
            try:
                if self.progress.is_completed(case["case_id"]):
                    continue
                log.info(f"[AllJud {i}/{len(cases_with_pdfs)}] {case['case_no'][:50]} | {case['case_title'][:40]}...")
                self.process_case(case, source="all_judgments")
            except Exception as e:
                log.error(f"Error processing case {case['case_id']}: {e}")
                self.progress.log_error(f"AllJud error: {case['case_id']}: {e}")

        # Also save metadata for cases without PDFs (just JSON, no PDF)
        log.info(f"Saving metadata for {len(cases_without_pdfs)} cases without PDFs...")
        for case in cases_without_pdfs:
            if self.progress.is_completed(case["case_id"]):
                continue
            year = case.get("year", "unknown")
            year_dir = CASES_DIR / year
            year_dir.mkdir(parents=True, exist_ok=True)
            html_year_dir = HTML_DIR / year
            html_year_dir.mkdir(parents=True, exist_ok=True)
            save_case(case, year_dir, html_year_dir)
            append_to_jsonl(case)
            self.progress.mark_completed(case["case_id"])

        self.progress.save()

    def run(self):
        """Full scrape pipeline."""
        self.progress.data["started_at"] = self.progress.data.get("started_at") or datetime.now(timezone.utc).isoformat()

        log.info("=" * 60)
        log.info("FSC Scraper — Federal Shariat Court of Pakistan")
        log.info("=" * 60)

        # Discover
        discovered = self.discover()

        # Process leading judgments first
        if discovered["leading"]:
            self.scrape_leading(discovered["leading"])

        # Process all judgments
        if discovered["all_judgments"]:
            self.scrape_all_judgments(discovered["all_judgments"])

        # Final summary
        self._print_summary()

    def _print_summary(self):
        p = self.progress.data
        log.info("=" * 60)
        log.info("FSC Scraper Summary")
        log.info("=" * 60)
        log.info(f"Leading Judgments: {p['sources']['leading_judgments']['downloaded']}/{p['sources']['leading_judgments']['discovered']}")
        log.info(f"All Judgments:     {p['sources']['all_judgments']['downloaded']}/{p['sources']['all_judgments']['discovered']}")
        log.info(f"PDFs Downloaded:   {p['total_pdfs_downloaded']}")
        log.info(f"Text Extracted:    {p['total_text_extracted']}")
        log.info(f"Errors:            {len(p['errors'])}")
        log.info("=" * 60)

    def status(self):
        """Print current scraping status."""
        p = self.progress.data
        print("\n" + "=" * 50)
        print("[STATUS] FSC Scraper Status")
        print("=" * 50)
        print(f"Court:           Federal Shariat Court of Pakistan")
        print(f"Started:         {p.get('started_at', 'Not started')}")
        print(f"Last Updated:    {p.get('last_updated', 'Never')}")
        print(f"\nLeading Judgments:")
        print(f"  Discovered:    {p['sources']['leading_judgments']['discovered']}")
        print(f"  Downloaded:    {p['sources']['leading_judgments']['downloaded']}")
        print(f"\nAll Judgments (alljud.php):")
        print(f"  Discovered:    {p['sources']['all_judgments']['discovered']}")
        print(f"  Downloaded:    {p['sources']['all_judgments']['downloaded']}")
        print(f"\nPDFs Downloaded: {p['total_pdfs_downloaded']}")
        print(f"Text Extracted:  {p['total_text_extracted']}")
        print(f"Errors:          {len(p['errors'])}")
        if p['errors']:
            print(f"\nLast 3 errors:")
            for err in p['errors'][-3:]:
                print(f"  [{err['time'][:19]}] {err['error'][:80]}")
        print("=" * 50 + "\n")

    def discover_only(self):
        """Only discover and show stats, don't download."""
        discovered = self.discover()
        print("\n" + "=" * 50)
        print("[DISCOVERY] FSC Discovery Results")
        print("=" * 50)

        if discovered["leading"]:
            print(f"\nLeading Judgments: {len(discovered['leading'])}")
            for c in discovered["leading"][:5]:
                print(f"  • {c['case_title'][:60]}")
            if len(discovered["leading"]) > 5:
                print(f"  ... and {len(discovered['leading']) - 5} more")

        if discovered["all_judgments"]:
            with_pdf = sum(1 for c in discovered["all_judgments"] if c["pdf_url"])
            print(f"\nAll Judgments: {len(discovered['all_judgments'])} total records")
            print(f"  With PDFs:  {with_pdf}")
            print(f"  Without:    {len(discovered['all_judgments']) - with_pdf}")

            # Year breakdown
            year_counts = {}
            for c in discovered["all_judgments"]:
                y = c.get("year", "unknown")
                year_counts[y] = year_counts.get(y, 0) + 1
            print(f"\n  Year distribution:")
            for y in sorted(year_counts.keys()):
                print(f"    {y}: {year_counts[y]}")

        print("=" * 50 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="FSC Scraper — Federal Shariat Court of Pakistan",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--status", action="store_true", help="Show current scraping status")
    parser.add_argument("--discover", action="store_true", help="Discover available judgments without downloading")
    parser.add_argument("--leading-only", action="store_true", help="Only scrape leading judgments")
    parser.add_argument("--all-only", action="store_true", help="Only scrape from alljud.php")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    scraper = FSCScraper()

    if args.status:
        scraper.status()
    elif args.discover:
        scraper.discover_only()
    elif args.leading_only:
        discovered = scraper.discover()
        if discovered["leading"]:
            scraper.scrape_leading(discovered["leading"])
        scraper._print_summary()
    elif args.all_only:
        discovered = scraper.discover()
        if discovered["all_judgments"]:
            scraper.scrape_all_judgments(discovered["all_judgments"])
        scraper._print_summary()
    else:
        scraper.run()


if __name__ == "__main__":
    main()
