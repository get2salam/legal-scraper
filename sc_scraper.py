#!/usr/bin/env python3
"""
Supreme Court of Pakistan — Judgment Scraper
=============================================

Scrapes judgments from https://www.supremecourt.gov.pk/judgement-search/

API: POST /wp-content/plugins/my-plugin/online_judgments.php
     Returns JSON array directly — no pagination, no auth, no captcha.

Parameters: case_type, case_number, case_year, author_judge, doa,
            keywords, parties_name, tagline, citation, SCCitation, reported

Response fields per record:
    caseNumber, caseSubject, caseTitle, caseFileName, authorJudge,
    judgmentText, tagline, citation, SCPCitation, reported,
    dateOfAnnouncement, dateCreated, fileSizeInBytes

PDFs at: /downloads_judgements/{caseFileName}

INTERNAL USE ONLY — never push to public GitHub.
"""

import argparse
import json
import logging
import os
import re
import sys
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi required. Install: pip install curl_cffi")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
    print("WARNING: PyMuPDF not installed. PDF text extraction disabled. Install: pip install PyMuPDF")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = "https://www.supremecourt.gov.pk"
SEARCH_API = f"{BASE_URL}/wp-content/plugins/my-plugin/online_judgments.php"
PDF_BASE = f"{BASE_URL}/downloads_judgements"

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
CASES_DIR = DATA_ROOT / "court_cases" / "SC"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "SC"
JSONL_DIR = DATA_ROOT / "court_cases"
PROGRESS_FILE = DATA_ROOT / "court_cases" / "sc_progress.json"

# Rate limiting — be respectful
MIN_DELAY = 5  # seconds between requests
MAX_DELAY = 10

# Case types from the SC website dropdown
CASE_TYPES = [
    "C.A.", "C.M.A.", "C.M.Appeal.", "C.P.L.A.", "C.R.P.",
    "C.Sh.A.", "C.Sh.P.", "C.Sh.R.P.", "C.P.",
    "Crl.A.", "Crl.M.A.", "Crl.M.Appeal.", "Crl.O.P.",
    "Crl.P.L.A.", "Crl.R.P.", "Crl.S.M.R.P.", "Crl.S.M.Sh.R.P.",
    "Crl.Sh.A.", "Crl.Sh.P.", "Crl.Sh.R.P.",
    "D.S.A.", "H.R.C.", "H.R.M.A.", "I.C.A.",
    "J.P.", "J.Sh.P.", "Reference.", "S.M.C.", "S.M.R.P.", "C.U.O."
]

# Year range
YEAR_START = 1980
YEAR_END = datetime.now().year + 1  # Include current year

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / "logs" / "sc_scraper.log", encoding="utf-8"),
    ]
)
log = logging.getLogger("sc_scraper")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

class SCSession:
    """Wrapper around curl_cffi for SC website requests."""

    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome")
        self._last_request_time = 0

    def _throttle(self):
        """Ensure minimum delay between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < MIN_DELAY:
            delay = MIN_DELAY + (MAX_DELAY - MIN_DELAY) * 0.5  # avg 7.5s
            remaining = delay - elapsed
            if remaining > 0:
                log.debug(f"Throttling {remaining:.1f}s")
                time.sleep(remaining)
        self._last_request_time = time.time()

    def search(self, case_year: str = "", case_type: str = "", case_number: str = "",
               author_judge: str = "", doa: str = "", keywords: str = "",
               parties_name: str = "", tagline: str = "", citation: str = "",
               sc_citation: str = "", reported: str = "",
               retries: int = 3) -> list:
        """
        Search judgments via the SC API.
        Returns list of judgment dicts.
        """
        self._throttle()

        data = {
            "case_type": case_type,
            "case_number": case_number,
            "case_year": case_year,
            "author_judge": author_judge,
            "doa": doa,
            "keywords": keywords,
            "parties_name": parties_name,
            "tagline": tagline,
            "citation": citation,
            "SCCitation": sc_citation,
            "reported": reported,
        }

        for attempt in range(1, retries + 1):
            try:
                r = self.session.post(SEARCH_API, data=data, timeout=120)
                r.raise_for_status()
                result = r.json()
                if isinstance(result, dict) and result.get("error"):
                    log.warning(f"API returned error: {result}")
                    return []
                if isinstance(result, list):
                    return result
                log.warning(f"Unexpected response type: {type(result)}")
                return []
            except Exception as e:
                log.warning(f"Search attempt {attempt}/{retries} failed: {e}")
                if attempt < retries:
                    time.sleep(10 * attempt)
                else:
                    log.error(f"Search failed after {retries} attempts")
                    return []

    def download_pdf(self, filename: str, dest_path: Path, retries: int = 3) -> bool:
        """Download a judgment PDF."""
        if not filename or filename.strip() == "":
            return False

        self._throttle()
        url = f"{PDF_BASE}/{filename}"

        for attempt in range(1, retries + 1):
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code == 404:
                    log.warning(f"PDF not found: {filename}")
                    return False
                r.raise_for_status()
                if len(r.content) < 100:
                    log.warning(f"PDF too small ({len(r.content)} bytes): {filename}")
                    return False
                if not r.content[:4] == b"%PDF":
                    log.warning(f"Not a PDF file: {filename}")
                    return False

                dest_path.parent.mkdir(parents=True, exist_ok=True)
                dest_path.write_bytes(r.content)
                log.debug(f"Downloaded PDF: {filename} ({len(r.content)} bytes)")
                return True
            except Exception as e:
                log.warning(f"PDF download attempt {attempt}/{retries} failed for {filename}: {e}")
                if attempt < retries:
                    time.sleep(10 * attempt)

        return False


# ---------------------------------------------------------------------------
# PDF Text Extraction
# ---------------------------------------------------------------------------

def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from a PDF using PyMuPDF."""
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
# Data Normalization
# ---------------------------------------------------------------------------

def normalize_date(date_str: str) -> str:
    """Convert dd-mm-yyyy to yyyy-mm-dd."""
    if not date_str or date_str.strip() == "" or date_str == "N/A":
        return ""
    date_str = date_str.strip()
    # Try dd-mm-yyyy
    try:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    # Try yyyy-mm-dd already
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return date_str
    except ValueError:
        pass
    return date_str


def make_safe_filename(case_number: str) -> str:
    """Convert a case number to a safe filename."""
    # e.g., "C.P.L.A.3661/2025" -> "CPLA_3661_2025"
    s = case_number.strip()
    s = re.sub(r'[./\\]', '_', s)
    s = re.sub(r'[-]', '_', s)
    s = re.sub(r'[^A-Za-z0-9_]', '', s)
    s = re.sub(r'_+', '_', s)
    s = s.strip('_')
    return s if s else hashlib.md5(case_number.encode()).hexdigest()[:12]


def extract_year_from_case(case_number: str) -> Optional[int]:
    """Extract year from case number like 'C.P.L.A.3661/2025'."""
    m = re.search(r'/(\d{4})$', case_number.strip())
    if m:
        return int(m.group(1))
    m = re.search(r'(\d{4})$', case_number.strip())
    if m:
        return int(m.group(1))
    return None


def parse_citations(citation_str: str) -> list:
    """Parse citation string into list of individual citations."""
    if not citation_str or citation_str.strip() in ("", "N/A"):
        return []
    # Citations often separated by <br>, commas, or semicolons
    parts = re.split(r'<br\s*/?>|;|\n', citation_str)
    citations = []
    for p in parts:
        p = p.strip()
        if p and p != "N/A":
            citations.append(p)
    return citations


def build_case_json(record: dict, pdf_text: str = "", pdf_url: str = "") -> dict:
    """Build the standardized JSON from an API record."""
    case_number = (record.get("caseNumber") or "").strip()
    citation = (record.get("citation") or "").strip()
    sc_citation = (record.get("SCPCitation") or "").strip()

    return {
        "source": "SC",
        "citation": citation if citation else None,
        "sc_citation": sc_citation if sc_citation else None,
        "case_number": case_number,
        "case_subject": (record.get("caseSubject") or "").strip() or None,
        "case_title": (record.get("caseTitle") or "").strip() or None,
        "author_judge": (record.get("authorJudge") or "").strip() or None,
        "judges": [],  # API doesn't separate judges; author_judge is what we have
        "upload_date": normalize_date(record.get("dateCreated", "")),
        "judgment_date": normalize_date(record.get("dateOfAnnouncement", "")),
        "citations": parse_citations(citation),
        "tagline": (record.get("tagline") or "").strip() or None,
        "reported": record.get("reported"),
        "judgment_text": pdf_text if pdf_text else None,
        "judgment_raw": (record.get("judgmentText") or ""),
        "pdf_url": pdf_url if pdf_url else None,
        "pdf_filename": (record.get("caseFileName") or "").strip() or None,
        "file_size_kb": record.get("fileSizeInBytes"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def generate_html(case_data: dict) -> str:
    """Generate readable HTML from case data."""
    title = case_data.get("case_title") or case_data.get("case_number") or "Unknown"
    text = case_data.get("judgment_text") or "(No text extracted)"

    # Convert plain text to HTML paragraphs
    paragraphs = text.split("\n\n") if "\n\n" in text else text.split("\n")
    body_html = "\n".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - Supreme Court of Pakistan</title>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.8; color: #333; }}
        .header {{ border-bottom: 2px solid #1a5276; margin-bottom: 20px; padding-bottom: 10px; }}
        .meta {{ color: #666; font-size: 0.9em; }}
        .meta dt {{ font-weight: bold; float: left; clear: left; width: 150px; }}
        .meta dd {{ margin-left: 160px; margin-bottom: 5px; }}
        .tagline {{ background: #f0f8ff; padding: 10px; border-left: 4px solid #1a5276; margin: 15px 0; font-style: italic; }}
        .body {{ text-align: justify; }}
        p {{ margin-bottom: 1em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{title}</h1>
        <h2>{case_data.get('case_number', '')}</h2>
    </div>
    <dl class="meta">
        <dt>Court:</dt><dd>Supreme Court of Pakistan</dd>
        <dt>Case Subject:</dt><dd>{case_data.get('case_subject', 'N/A')}</dd>
        <dt>Author Judge:</dt><dd>{case_data.get('author_judge', 'N/A')}</dd>
        <dt>Judgment Date:</dt><dd>{case_data.get('judgment_date', 'N/A')}</dd>
        <dt>Upload Date:</dt><dd>{case_data.get('upload_date', 'N/A')}</dd>
        <dt>Citation:</dt><dd>{case_data.get('citation', 'N/A')}</dd>
        <dt>SC Citation:</dt><dd>{case_data.get('sc_citation', 'N/A')}</dd>
    </dl>
    {"<div class='tagline'>" + case_data['tagline'] + "</div>" if case_data.get('tagline') else ""}
    <div class="body">
        {body_html}
    </div>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Progress Tracking
# ---------------------------------------------------------------------------

class ProgressTracker:
    """Track scraping progress across sessions."""

    def __init__(self, path: Path = PROGRESS_FILE):
        self.path = path
        self.data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "started_at": None,
            "last_updated": None,
            "years_completed": [],
            "years_in_progress": {},
            "total_cases_found": 0,
            "total_cases_scraped": 0,
            "total_pdfs_downloaded": 0,
            "total_pdfs_failed": 0,
            "errors": [],
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def mark_year_start(self, year: int, total: int):
        if not self.data["started_at"]:
            self.data["started_at"] = datetime.now(timezone.utc).isoformat()
        self.data["years_in_progress"][str(year)] = {
            "total": total,
            "scraped": 0,
            "pdfs_ok": 0,
            "pdfs_fail": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save()

    def mark_case_done(self, year: int, pdf_ok: bool):
        ykey = str(year)
        if ykey in self.data["years_in_progress"]:
            self.data["years_in_progress"][ykey]["scraped"] += 1
            if pdf_ok:
                self.data["years_in_progress"][ykey]["pdfs_ok"] += 1
            else:
                self.data["years_in_progress"][ykey]["pdfs_fail"] += 1
        self.data["total_cases_scraped"] += 1
        if pdf_ok:
            self.data["total_pdfs_downloaded"] += 1
        else:
            self.data["total_pdfs_failed"] += 1

    def mark_year_done(self, year: int):
        ykey = str(year)
        if ykey in self.data["years_in_progress"]:
            del self.data["years_in_progress"][ykey]
        if year not in self.data["years_completed"]:
            self.data["years_completed"].append(year)
            self.data["years_completed"].sort()
        self.save()

    def is_year_done(self, year: int) -> bool:
        return year in self.data["years_completed"]

    def get_year_scraped_count(self, year: int) -> int:
        ykey = str(year)
        if ykey in self.data["years_in_progress"]:
            return self.data["years_in_progress"][ykey]["scraped"]
        return 0

    def add_error(self, error: str):
        self.data["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error,
        })
        # Keep last 100 errors
        self.data["errors"] = self.data["errors"][-100:]

    def print_status(self):
        print("\n" + "=" * 60)
        print("  Supreme Court Scraper — Progress Report")
        print("=" * 60)
        print(f"  Started:          {self.data.get('started_at', 'Never')}")
        print(f"  Last updated:     {self.data.get('last_updated', 'Never')}")
        print(f"  Years completed:  {len(self.data['years_completed'])}")
        print(f"  Cases scraped:    {self.data['total_cases_scraped']}")
        print(f"  PDFs downloaded:  {self.data['total_pdfs_downloaded']}")
        print(f"  PDFs failed:      {self.data['total_pdfs_failed']}")
        print()

        if self.data["years_completed"]:
            completed = sorted(self.data["years_completed"])
            print(f"  Completed years: {', '.join(str(y) for y in completed)}")
            print()

        if self.data["years_in_progress"]:
            print("  In progress:")
            for ykey, info in sorted(self.data["years_in_progress"].items()):
                print(f"    {ykey}: {info['scraped']}/{info['total']} "
                      f"(PDFs: {info['pdfs_ok']} ok, {info['pdfs_fail']} fail)")
            print()

        if self.data["errors"]:
            print(f"  Recent errors ({len(self.data['errors'])}):")
            for err in self.data["errors"][-5:]:
                print(f"    [{err['time'][:19]}] {err['error'][:80]}")

        print("=" * 60 + "\n")


# ---------------------------------------------------------------------------
# JSONL Management
# ---------------------------------------------------------------------------

def append_jsonl(filepath: Path, record: dict):
    """Append a record to a JSONL file."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "a", encoding="utf-8") as f:
        # Write a compact version without judgment_text for JSONL
        compact = {k: v for k, v in record.items() if k != "judgment_text"}
        f.write(json.dumps(compact, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------------------
# Main Scraping Logic
# ---------------------------------------------------------------------------

def scrape_year(session: SCSession, year: int, progress: ProgressTracker,
                skip_existing: bool = True, download_pdfs: bool = True):
    """Scrape all judgments for a given year."""

    if progress.is_year_done(year) and skip_existing:
        log.info(f"Year {year} already completed, skipping")
        return

    log.info(f"{'='*50}")
    log.info(f"Scraping year {year}")
    log.info(f"{'='*50}")

    # Fetch all records for this year
    records = session.search(case_year=str(year))
    if not records:
        log.info(f"No records found for year {year}")
        progress.mark_year_done(year)
        return

    log.info(f"Found {len(records)} records for year {year}")
    progress.data["total_cases_found"] += len(records)
    progress.mark_year_start(year, len(records))

    # JSONL file for this year
    jsonl_year_path = JSONL_DIR / f"SC_{year}.jsonl"
    jsonl_master_path = JSONL_DIR / "SC_master.jsonl"

    # Track which case numbers we've already seen (for dedup within year)
    seen_cases = set()

    # Check existing files to determine resume point
    year_dir = CASES_DIR / str(year)
    existing_files = set()
    if year_dir.exists():
        existing_files = {f.stem for f in year_dir.glob("*.json")}

    scraped_in_session = progress.get_year_scraped_count(year)

    for idx, record in enumerate(records):
        case_number = (record.get("caseNumber") or "").strip()
        if not case_number:
            log.warning(f"Skipping record with empty case number at index {idx}")
            continue

        # Dedup
        if case_number in seen_cases:
            log.debug(f"Duplicate case number: {case_number}")
            continue
        seen_cases.add(case_number)

        safe_name = make_safe_filename(case_number)
        case_year_from_number = extract_year_from_case(case_number) or year

        # Skip if already scraped
        if skip_existing and safe_name in existing_files:
            log.debug(f"Already exists: {safe_name}")
            # Still count for progress
            if scraped_in_session == 0:
                progress.mark_case_done(year, True)
            continue

        log.info(f"[{idx+1}/{len(records)}] {case_number} — {(record.get('caseTitle') or '')[:50]}")

        # Determine PDF info
        case_filename = (record.get("caseFileName") or "").strip()
        pdf_url = f"{PDF_BASE}/{case_filename}" if case_filename else ""
        pdf_text = ""
        pdf_ok = False

        if download_pdfs and case_filename:
            # Download PDF
            pdf_dest = CASES_DIR / str(year) / "original" / f"{safe_name}.pdf"
            if pdf_dest.exists() and pdf_dest.stat().st_size > 100:
                log.debug(f"PDF already exists: {pdf_dest.name}")
                pdf_ok = True
                pdf_text = extract_pdf_text(pdf_dest)
            else:
                pdf_ok = session.download_pdf(case_filename, pdf_dest)
                if pdf_ok:
                    pdf_text = extract_pdf_text(pdf_dest)
                else:
                    progress.add_error(f"PDF download failed: {case_filename}")

        # Build JSON
        case_data = build_case_json(record, pdf_text, pdf_url)

        # Save JSON
        json_path = CASES_DIR / str(year) / f"{safe_name}.json"
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps(case_data, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

        # Generate readable HTML
        html_path = HTML_DIR / str(year) / f"{safe_name}.html"
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_path.write_text(generate_html(case_data), encoding="utf-8")

        # Append to JSONL files
        append_jsonl(jsonl_year_path, case_data)
        append_jsonl(jsonl_master_path, case_data)

        # Update progress
        progress.mark_case_done(year, pdf_ok)

        # Save progress periodically
        if (idx + 1) % 10 == 0:
            progress.save()
            log.info(f"  Progress: {idx+1}/{len(records)} for year {year}")

    progress.mark_year_done(year)
    log.info(f"Completed year {year}: {len(records)} records processed")


# ---------------------------------------------------------------------------
# API Discovery / Debug
# ---------------------------------------------------------------------------

def discover_api(session: SCSession):
    """Print API details for debugging."""
    print("\n" + "=" * 60)
    print("  Supreme Court API Discovery")
    print("=" * 60)
    print()
    print(f"  Search API:  {SEARCH_API}")
    print(f"  Method:      POST")
    print(f"  Auth:        None (no captcha, no nonce)")
    print(f"  PDF Base:    {PDF_BASE}/{{caseFileName}}")
    print()
    print("  Parameters:")
    print("    case_type      — e.g., 'C.A.', 'C.P.L.A.', 'Crl.A.'")
    print("    case_number    — e.g., '3661'")
    print("    case_year      — e.g., '2025' (range: 1980-present)")
    print("    author_judge   — full name, e.g., 'Mr. Justice Yahya Afridi'")
    print("    doa            — date of announcement (dd/mm/yyyy)")
    print("    keywords       — text search")
    print("    parties_name   — party name search")
    print("    tagline        — tagline search")
    print("    citation       — e.g., '2024 SCMR 123'")
    print("    SCCitation     — SC's own citation")
    print("    reported       — 'yes' or 'no'")
    print()
    print("  Response JSON fields:")
    print("    caseNumber, caseSubject, caseTitle, caseFileName,")
    print("    authorJudge, judgmentText, tagline, citation,")
    print("    SCPCitation, reported, dateOfAnnouncement,")
    print("    dateCreated, fileSizeInBytes")
    print()

    print("  Available Case Types:")
    for ct in CASE_TYPES:
        print(f"    {ct}")
    print()

    # Test with a small query
    print("  Testing API with year=2025...")
    records = session.search(case_year="2025")
    print(f"  Result: {len(records)} records")
    if records:
        print(f"  Sample: {records[0].get('caseNumber')} — {records[0].get('caseTitle', '')[:60]}")
        print(f"  PDF:    {PDF_BASE}/{records[0].get('caseFileName', 'N/A')}")

    print()

    # Test year counts
    print("  Checking record counts by year (sampling)...")
    test_years = [2025, 2024, 2023, 2022, 2021, 2020, 2015, 2010, 2005, 2000]
    for y in test_years:
        time.sleep(3)
        recs = session.search(case_year=str(y))
        print(f"    {y}: {len(recs)} records")

    print()
    print("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Supreme Court of Pakistan Judgment Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sc_scraper.py                    # Scrape all years (2026 down to 1980)
  python sc_scraper.py --year 2025        # Scrape specific year
  python sc_scraper.py --year-range 2020 2025  # Scrape year range
  python sc_scraper.py --discover-api     # Print API details
  python sc_scraper.py --status           # Show progress
  python sc_scraper.py --no-pdfs          # Skip PDF downloads
  python sc_scraper.py --force            # Re-scrape even if done
        """
    )

    parser.add_argument("--year", type=int, help="Scrape a specific year")
    parser.add_argument("--year-range", type=int, nargs=2, metavar=("FROM", "TO"),
                        help="Scrape a range of years (inclusive)")
    parser.add_argument("--discover-api", action="store_true",
                        help="Just discover and print API details")
    parser.add_argument("--status", action="store_true", help="Show scraping progress")
    parser.add_argument("--no-pdfs", action="store_true", help="Skip PDF downloads")
    parser.add_argument("--force", action="store_true", help="Re-scrape completed years")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Ensure log directory exists
    (PROJECT_ROOT / "logs").mkdir(exist_ok=True)

    # Status check
    if args.status:
        progress = ProgressTracker()
        progress.print_status()
        return

    # Initialize session
    session = SCSession()

    # API discovery
    if args.discover_api:
        discover_api(session)
        return

    # Determine years to scrape
    if args.year:
        years = [args.year]
    elif args.year_range:
        y1, y2 = args.year_range
        years = list(range(max(y1, y2), min(y1, y2) - 1, -1))
    else:
        # Default: newest to oldest
        years = list(range(YEAR_END, YEAR_START - 1, -1))

    # Scrape
    progress = ProgressTracker()
    skip_existing = not args.force
    download_pdfs = not args.no_pdfs

    log.info(f"Starting SC scraper for years: {years[0]} to {years[-1]}")
    log.info(f"Skip existing: {skip_existing}, Download PDFs: {download_pdfs}")

    try:
        for year in years:
            try:
                scrape_year(session, year, progress,
                            skip_existing=skip_existing,
                            download_pdfs=download_pdfs)
            except KeyboardInterrupt:
                log.info("Interrupted by user")
                progress.save()
                sys.exit(0)
            except Exception as e:
                log.error(f"Error scraping year {year}: {e}", exc_info=True)
                progress.add_error(f"Year {year}: {e}")
                progress.save()
                continue
    finally:
        progress.save()
        log.info("Scraper finished. Final progress:")
        progress.print_status()


if __name__ == "__main__":
    main()
