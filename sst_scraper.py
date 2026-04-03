#!/usr/bin/env python3
"""
SST Scraper — Sindh Service Tribunal
======================================
Scrapes judgments from https://sstsindh.gov.pk/judgements.php

API discovered by reverse-engineering the JavaScript:
  GET https://sstsindh.gov.pk/admin/api/judgements.php
    ?action=getJ
    &start=0
    &noOfRecords=100
    &keyword=
    &j_sort=
    &j_orderby=

Response: { "data": [...], "totalrecords": N, "error": ..., "status": ... }
Each record: { "id", "name" (PDF filename), "appeal", "appealant", "created_at" }

PDFs at: https://sstsindh.gov.pk/admin/upload/judgements/{name}

Also scrapes:
  - /appeals.php — Month/Year Wise Appeals  (via monthlyappeals.php API)
  - /executionappeals.php — Execution Applications

Established 1973 — 50+ years of service tribunal cases.

Storage: data_v2/court_cases/SST/YEAR/
Formats: JSON + PDF + Readable HTML + JSONL
Progress: data_v2/court_cases/sst_progress.json

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
from typing import Optional, List, Dict, Any
from urllib.parse import quote, unquote

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

BASE_URL = "https://sstsindh.gov.pk"
API_URL = f"{BASE_URL}/admin/api/judgements.php"
APPEALS_API_URL = f"{BASE_URL}/admin/api/monthlyappeals.php"
EXEC_APPEALS_API_URL = f"{BASE_URL}/admin/api/monthlyexecutionappeals.php"
PDF_BASE_URL = f"{BASE_URL}/admin/upload/judgements/"

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
CASES_DIR = DATA_ROOT / "court_cases" / "SST"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "SST"
JSONL_DIR = DATA_ROOT / "court_cases"
JSONL_FILE = JSONL_DIR / "sst_cases.jsonl"
PROGRESS_FILE = DATA_ROOT / "court_cases" / "sst_progress.json"
LOG_DIR = PROJECT_ROOT / "logs"

MIN_DELAY = 5
MAX_DELAY = 10
MAX_RETRIES = 3
RETRY_BACKOFF = 15
PAGE_SIZE = 50  # Records per API call

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "sst_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("sst_scraper")


# ---------------------------------------------------------------------------
# HTTP Session
# ---------------------------------------------------------------------------

class SSTSession:
    """Wrapper around curl_cffi with TLS impersonation and throttling."""

    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome")
        self._last_request_time = 0

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        delay = MIN_DELAY + random.random() * (MAX_DELAY - MIN_DELAY)
        remaining = delay - elapsed
        if remaining > 0:
            log.debug(f"Throttling {remaining:.1f}s")
            time.sleep(remaining)
        self._last_request_time = time.time()

    def get_json(self, url: str, params: dict = None, timeout: int = 30) -> Optional[dict]:
        """GET request expecting JSON response."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._throttle()
                resp = self.session.get(
                    url,
                    params=params,
                    timeout=timeout,
                    headers={
                        "Referer": f"{BASE_URL}/judgements.php",
                        "X-Requested-With": "XMLHttpRequest",
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                    },
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("status") == "error":
                        log.warning(f"API error: {data.get('msg', 'unknown')}")
                        return None
                    return data
                log.warning(f"HTTP {resp.status_code} for {url} (attempt {attempt})")
            except Exception as e:
                log.warning(f"Request failed: {e} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
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
                resp = self.session.get(
                    url,
                    timeout=timeout,
                    headers={"Referer": f"{BASE_URL}/judgements.php"},
                )
                if resp.status_code == 200 and len(resp.content) > 500:
                    dest.write_bytes(resp.content)
                    log.info(f"Downloaded PDF ({len(resp.content):,} bytes): {dest.name}")
                    return True
                log.warning(f"PDF download HTTP {resp.status_code}, size {len(resp.content)} (attempt {attempt})")
            except Exception as e:
                log.warning(f"PDF download failed: {e} (attempt {attempt})")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF * attempt)
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
            "court": "SST",
            "court_name": "Sindh Service Tribunal",
            "started_at": None,
            "last_updated": None,
            "sources": {
                "judgments": {
                    "total_on_server": 0,
                    "discovered": 0,
                    "downloaded": 0,
                    "completed_ids": [],
                },
                "appeals": {
                    "total_on_server": 0,
                    "discovered": 0,
                    "downloaded": 0,
                    "completed_ids": [],
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

    def is_completed(self, case_id: str, source: str = "judgments") -> bool:
        return case_id in self.data["sources"][source]["completed_ids"]

    def mark_completed(self, case_id: str, source: str = "judgments"):
        if case_id not in self.data["sources"][source]["completed_ids"]:
            self.data["sources"][source]["completed_ids"].append(case_id)
            self.data["sources"][source]["downloaded"] += 1

    def log_error(self, error: str):
        self.data["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error[:500],
        })
        self.data["errors"] = self.data["errors"][-100:]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def extract_year_from_appeal(appeal: str) -> Optional[str]:
    """Extract year from appeal number like 'Appeal No. 197 & 198 of 2025'."""
    m = re.search(r'of\s+(\d{4})', appeal)
    if m:
        return m.group(1)
    matches = re.findall(r'\b(19[789]\d|20[0-3]\d)\b', appeal)
    return matches[-1] if matches else None


def extract_year_from_date(date_str: str) -> Optional[str]:
    """Extract year from date like '2026-02-16 11:09:59'."""
    m = re.search(r'(\d{4})-\d{2}-\d{2}', date_str)
    return m.group(1) if m else None


def make_case_id(record_id: str, appeal: str) -> str:
    """Create stable unique ID for an SST case."""
    slug = re.sub(r'[^a-zA-Z0-9]+', '-', appeal.strip()).strip('-').lower()
    if not slug:
        slug = f"id-{record_id}"
    return f"SST-{slug}"


def sanitize_filename(name: str) -> str:
    """Make filename safe for filesystem."""
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

def generate_readable_html(case: dict) -> str:
    """Generate a clean, readable HTML page for an SST judgment."""
    appeal = case.get("appeal", "SST Judgment")
    appellant = case.get("appellant", "")
    date = case.get("created_at", "")
    year = case.get("year", "")
    text = case.get("full_text", "")

    text_html = ""
    if text:
        paragraphs = text.split("\n")
        text_html = "\n".join(f"<p>{p.strip()}</p>" for p in paragraphs if p.strip())

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{appeal} - Sindh Service Tribunal</title>
    <style>
        body {{ font-family: Georgia, serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.6; color: #333; }}
        .header {{ border-bottom: 3px solid #16a990; padding-bottom: 15px; margin-bottom: 20px; }}
        .court-name {{ color: #16a990; font-size: 1.4em; font-weight: bold; }}
        .meta {{ background: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
        .meta dt {{ font-weight: bold; color: #16a990; }}
        .meta dd {{ margin: 0 0 10px 0; }}
        .judgment-text {{ text-align: justify; }}
        .footer {{ border-top: 1px solid #ddd; margin-top: 30px; padding-top: 10px; font-size: 0.85em; color: #666; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="court-name">Sindh Service Tribunal</div>
        <h1>{appeal}</h1>
    </div>
    <dl class="meta">
        <dt>Appeal</dt><dd>{appeal}</dd>
        {"<dt>Appellant</dt><dd>" + appellant + "</dd>" if appellant else ""}
        {"<dt>Date</dt><dd>" + date + "</dd>" if date else ""}
        {"<dt>Year</dt><dd>" + year + "</dd>" if year else ""}
        <dt>Source</dt><dd>Sindh Service Tribunal — sstsindh.gov.pk</dd>
    </dl>
    <div class="judgment-text">
        {text_html if text_html else "<p><em>Full text not available. See PDF.</em></p>"}
    </div>
    <div class="footer">
        <p>Source: Sindh Service Tribunal | Scraped: {datetime.now(timezone.utc).strftime('%Y-%m-%d')}</p>
    </div>
</body>
</html>"""


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


def append_to_jsonl(case: dict):
    """Append case to JSONL file."""
    JSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(case, ensure_ascii=False)
    with open(JSONL_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

class SSTApiClient:
    """Client for the SST judgment API."""

    def __init__(self, session: SSTSession):
        self.session = session

    def get_judgments_page(self, start: int = 0, limit: int = PAGE_SIZE,
                          keyword: str = "") -> Optional[dict]:
        """Fetch a page of judgments from the API."""
        params = {
            "action": "getJ",
            "j_sort": "",
            "j_orderby": "",
            "start": start,
            "noOfRecords": limit,
            "keyword": keyword,
        }
        return self.session.get_json(API_URL, params=params)

    def get_all_judgments(self, keyword: str = "") -> List[dict]:
        """Fetch ALL judgments by paginating through the API."""
        all_records = []
        start = 0

        # First request to get total count
        data = self.get_judgments_page(start=0, limit=PAGE_SIZE, keyword=keyword)
        if not data:
            log.error("Failed to fetch first page of judgments")
            return []

        total = data.get("totalrecords", 0)
        log.info(f"SST API reports {total} total judgments")

        if "data" in data and data["data"]:
            all_records.extend(data["data"])

        # Paginate through remaining pages
        start = PAGE_SIZE
        while start < total:
            log.info(f"Fetching judgments {start}-{start + PAGE_SIZE} of {total}...")
            data = self.get_judgments_page(start=start, limit=PAGE_SIZE, keyword=keyword)
            if data and "data" in data and data["data"]:
                all_records.extend(data["data"])
            else:
                log.warning(f"Failed to fetch page at offset {start}")
                break
            start += PAGE_SIZE

        log.info(f"Fetched {len(all_records)} judgment records total")
        return all_records

    def get_appeals_page(self, start: int = 0, limit: int = PAGE_SIZE) -> Optional[dict]:
        """Fetch month/year wise appeals."""
        params = {
            "action": "getMA",
            "ma_sort": "",
            "ma_orderby": "",
            "start": start,
            "noOfRecords": limit,
            "keyword": "",
        }
        return self.session.get_json(APPEALS_API_URL, params=params)

    def get_all_appeals(self) -> List[dict]:
        """Fetch ALL appeals by paginating."""
        all_records = []
        start = 0

        data = self.get_appeals_page(start=0, limit=PAGE_SIZE)
        if not data:
            log.info("No appeals data available (or API error)")
            return []

        total = data.get("totalrecords", 0)
        log.info(f"SST Appeals API reports {total} total appeals")

        if "data" in data and data["data"]:
            all_records.extend(data["data"])

        start = PAGE_SIZE
        while start < total:
            data = self.get_appeals_page(start=start, limit=PAGE_SIZE)
            if data and "data" in data and data["data"]:
                all_records.extend(data["data"])
            else:
                break
            start += PAGE_SIZE

        log.info(f"Fetched {len(all_records)} appeal records total")
        return all_records


# ---------------------------------------------------------------------------
# Record to Case Conversion
# ---------------------------------------------------------------------------

def record_to_case(record: dict) -> dict:
    """Convert an API record to a standardized case dict."""
    record_id = record.get("id", "")
    name = record.get("name", "")  # PDF filename
    appeal = record.get("appeal", "")
    appellant = record.get("appealant", "")
    created_at = record.get("created_at", "")

    # Determine year
    year = extract_year_from_appeal(appeal)
    if not year and created_at:
        year = extract_year_from_date(created_at)
    if not year:
        year = "unknown"

    # Build PDF URL
    pdf_url = ""
    if name:
        # URL-encode the filename (it may contain spaces)
        encoded_name = quote(name, safe='')
        pdf_url = f"{PDF_BASE_URL}{encoded_name}"

    case_id = make_case_id(record_id, appeal)

    return {
        "case_id": case_id,
        "record_id": record_id,
        "appeal": appeal,
        "appellant": appellant,
        "pdf_filename": name,
        "pdf_url": pdf_url,
        "created_at": created_at,
        "year": year,
        "full_text": "",
        "source": "judgments_api",
        "source_url": f"{BASE_URL}/judgements.php",
        "court": "SST",
        "court_name": "Sindh Service Tribunal",
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Scraper Core
# ---------------------------------------------------------------------------

class SSTScraper:
    """Sindh Service Tribunal judgment scraper."""

    def __init__(self):
        self.session = SSTSession()
        self.api = SSTApiClient(self.session)
        self.progress = ProgressManager(PROGRESS_FILE)

    def discover(self) -> Dict[str, List[dict]]:
        """Discover all available judgments and appeals."""
        results = {"judgments": [], "appeals": []}

        # 1. Judgments
        log.info("Discovering judgments via API...")
        raw_judgments = self.api.get_all_judgments()
        cases = [record_to_case(r) for r in raw_judgments]
        results["judgments"] = cases
        self.progress.data["sources"]["judgments"]["total_on_server"] = len(raw_judgments)
        self.progress.data["sources"]["judgments"]["discovered"] = len(cases)

        # 2. Appeals (bonus — may have additional PDF downloads)
        log.info("Discovering appeals via API...")
        raw_appeals = self.api.get_all_appeals()
        results["appeals"] = raw_appeals
        self.progress.data["sources"]["appeals"]["total_on_server"] = len(raw_appeals)
        self.progress.data["sources"]["appeals"]["discovered"] = len(raw_appeals)

        self.progress.save()
        return results

    def process_case(self, case: dict) -> bool:
        """Download PDF, extract text, save in all formats."""
        case_id = case["case_id"]

        if self.progress.is_completed(case_id):
            log.debug(f"Already completed: {case_id}")
            return True

        year = case.get("year", "unknown")
        year_dir = CASES_DIR / year
        year_dir.mkdir(parents=True, exist_ok=True)

        html_year_dir = HTML_DIR / year
        html_year_dir.mkdir(parents=True, exist_ok=True)

        # Download PDF
        if case["pdf_url"]:
            safe_filename = sanitize_filename(case.get("pdf_filename", f"{case_id}.pdf"))
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
        self.progress.mark_completed(case_id)
        self.progress.save()

        return True

    def scrape_judgments(self, cases: List[dict]):
        """Process all judgment cases."""
        pending = [c for c in cases if not self.progress.is_completed(c["case_id"])]
        log.info(f"Processing {len(pending)} judgments ({len(cases) - len(pending)} already done)...")

        for i, case in enumerate(pending, 1):
            try:
                log.info(f"[{i}/{len(pending)}] {case['appeal'][:60]} | {case['appellant'][:30]}...")
                self.process_case(case)
            except Exception as e:
                log.error(f"Error processing case {case['case_id']}: {e}")
                self.progress.log_error(f"Case error: {case['case_id']}: {e}")

    def run(self):
        """Full scrape pipeline."""
        self.progress.data["started_at"] = self.progress.data.get("started_at") or datetime.now(timezone.utc).isoformat()

        log.info("=" * 60)
        log.info("SST Scraper — Sindh Service Tribunal")
        log.info("=" * 60)

        # Discover
        discovered = self.discover()

        # Process judgments
        if discovered["judgments"]:
            self.scrape_judgments(discovered["judgments"])

        # Final summary
        self._print_summary()

    def _print_summary(self):
        p = self.progress.data
        log.info("=" * 60)
        log.info("SST Scraper Summary")
        log.info("=" * 60)
        log.info(f"Judgments:        {p['sources']['judgments']['downloaded']}/{p['sources']['judgments']['discovered']}")
        log.info(f"PDFs Downloaded:  {p['total_pdfs_downloaded']}")
        log.info(f"Text Extracted:   {p['total_text_extracted']}")
        log.info(f"Errors:           {len(p['errors'])}")
        log.info("=" * 60)

    def status(self):
        """Print current scraping status."""
        p = self.progress.data
        print("\n" + "=" * 50)
        print("[STATUS] SST Scraper Status")
        print("=" * 50)
        print(f"Court:           Sindh Service Tribunal")
        print(f"Started:         {p.get('started_at', 'Not started')}")
        print(f"Last Updated:    {p.get('last_updated', 'Never')}")
        print(f"\nJudgments:")
        print(f"  On Server:     {p['sources']['judgments']['total_on_server']}")
        print(f"  Discovered:    {p['sources']['judgments']['discovered']}")
        print(f"  Downloaded:    {p['sources']['judgments']['downloaded']}")
        print(f"\nAppeals:")
        print(f"  On Server:     {p['sources']['appeals']['total_on_server']}")
        print(f"  Discovered:    {p['sources']['appeals']['discovered']}")
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
        print("[DISCOVERY] SST Discovery Results")
        print("=" * 50)

        judgments = discovered["judgments"]
        print(f"\nJudgments: {len(judgments)} total")

        if judgments:
            # Year distribution
            year_counts = {}
            for c in judgments:
                y = c.get("year", "unknown")
                year_counts[y] = year_counts.get(y, 0) + 1

            print(f"\nYear distribution:")
            for y in sorted(year_counts.keys()):
                print(f"  {y}: {year_counts[y]}")

            print(f"\nSample records:")
            for c in judgments[:5]:
                print(f"  • [{c['year']}] {c['appeal']} — {c['appellant'][:40]}")
            if len(judgments) > 5:
                print(f"  ... and {len(judgments) - 5} more")

        appeals = discovered["appeals"]
        if appeals:
            print(f"\nAppeals/Applications: {len(appeals)} records")

        print("=" * 50 + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="SST Scraper — Sindh Service Tribunal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--status", action="store_true", help="Show current scraping status")
    parser.add_argument("--discover", action="store_true", help="Discover available judgments without downloading")
    parser.add_argument("--search", type=str, default="", help="Search keyword (appeal number, etc.)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    scraper = SSTScraper()

    if args.status:
        scraper.status()
    elif args.discover:
        scraper.discover_only()
    else:
        scraper.run()


if __name__ == "__main__":
    main()
