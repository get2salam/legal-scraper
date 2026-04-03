#!/usr/bin/env python3
"""
FCC Scraper — Federal Constitutional Court of Pakistan
========================================================
Placeholder scraper for the newly established Federal Constitutional Court.

Background:
  - Established October 2023 by the 26th Constitutional Amendment
  - May not have a public case database yet
  - Website candidates: fcc.gov.pk, federalconstitutionalcourt.gov.pk, etc.

This scraper:
  1. Probes known/guessed URLs to find the FCC website
  2. If found, attempts to scrape whatever's publicly available
  3. If not found, creates a stub for future activation

Usage:
    python fcc_scraper.py                  # Run scraper (or stub)
    python fcc_scraper.py --discover       # Probe for FCC website
    python fcc_scraper.py --status         # Show progress
    python fcc_scraper.py --no-skip        # Re-scrape existing

INTERNAL USE ONLY — never push to public GitHub.
"""

import argparse
import json
import logging
import os
import re
import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
import time
import random
import hashlib
import html as html_mod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field, asdict
from urllib.parse import urljoin, urlparse

try:
    from curl_cffi import requests as cffi_requests
    from curl_cffi.requests import Session
except ImportError:
    print("ERROR: curl_cffi required. Install: pip install curl_cffi")
    sys.exit(1)

from bs4 import BeautifulSoup, Tag

try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    print("WARNING: PyMuPDF not installed. PDF text extraction disabled.")

# ==============================================================================
# Configuration
# ==============================================================================

# Candidate domains for the FCC
CANDIDATE_DOMAINS = [
    "https://fcc.gov.pk",
    "http://fcc.gov.pk",
    "https://www.fcc.gov.pk",
    "https://federalconstitutionalcourt.gov.pk",
    "http://federalconstitutionalcourt.gov.pk",
    "https://www.federalconstitutionalcourt.gov.pk",
    "https://fcc.judiciary.gov.pk",
    "https://federalcourt.gov.pk",
    "https://fconstitutionalcourt.gov.pk",
    "https://fccp.gov.pk",
    "https://constcourt.gov.pk",
    "https://constitutionalcourt.gov.pk",
    "https://federal-constitutional-court.gov.pk",
]

# If we find the real domain, it goes here
CONFIRMED_DOMAIN = None  # Set after discovery

# Endpoints to probe on any found domain
PROBE_ENDPOINTS = [
    "/",
    "/judgments",
    "/decisions",
    "/cases",
    "/orders",
    "/cause-list",
    "/cause-lists",
    "/judges",
    "/about",
    "/about-court",
    "/resources",
    "/resources/judgments",
    "/api/cases",
    "/api/judgments",
    "/wp-json/wp/v2/posts",
    "/wp-json/wp/v2/pages",
    "/sitemap.xml",
    "/robots.txt",
    "/feed/",
]

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
COURT_DIR = DATA_ROOT / "court_cases"
FCC_DIR = COURT_DIR / "FCC"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "FCC"
PROGRESS_FILE = COURT_DIR / "fcc_progress.json"
JSONL_DIR = COURT_DIR
LOG_DIR = PROJECT_ROOT / "logs"

# Rate limiting
MIN_DELAY = 5.0
MAX_DELAY = 10.0
PAGE_TIMEOUT = 30
PDF_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 15

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "fcc_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("fcc_scraper")


# ==============================================================================
# Data Classes
# ==============================================================================

@dataclass
class FCCCase:
    """Represents a single FCC judgment/decision."""
    source: str = "FCC"
    case_number: str = ""
    case_type: str = ""
    title: str = ""
    parties: Dict[str, str] = field(default_factory=dict)
    judges: List[str] = field(default_factory=list)
    judgment_date: str = ""
    decision_date: str = ""
    citation: str = ""
    headnote: str = ""
    judgment_text: str = ""
    pdf_url: str = ""
    source_url: str = ""
    source_type: str = ""    # "website", "api", "discovered"
    fetched_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v or isinstance(v, (bool, int))}

    @property
    def file_key(self) -> str:
        parts = ["FCC"]
        if self.case_number:
            parts.append(make_safe_filename(self.case_number))
        elif self.title:
            parts.append(make_safe_filename(self.title)[:60])
        else:
            parts.append(hashlib.md5(json.dumps(self.to_dict()).encode()).hexdigest()[:12])
        return "_".join(parts)

    @property
    def year(self) -> str:
        if self.judgment_date:
            m = re.search(r"\d{4}", self.judgment_date)
            if m:
                return m.group()
        if self.decision_date:
            m = re.search(r"\d{4}", self.decision_date)
            if m:
                return m.group()
        return "unknown"


# ==============================================================================
# Utility Functions
# ==============================================================================

def make_safe_filename(text: str) -> str:
    s = re.sub(r"[./\\:*?\"<>|]", "_", text.strip())
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s[:120] if s else hashlib.md5(text.encode()).hexdigest()[:12]


def normalize_date(date_str: str) -> str:
    if not date_str or date_str.strip() in ("", "-", "N/A", "None"):
        return ""
    date_str = date_str.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d-%B-%Y",
                "%Y-%m-%d", "%m/%d/%Y", "%d %b %Y", "%d %B %Y",
                "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def extract_pdf_text(pdf_path: Path) -> str:
    if not HAS_PYMUPDF:
        return ""
    try:
        doc = fitz.open(str(pdf_path))
        parts = [page.get_text() for page in doc]
        doc.close()
        return "\n".join(parts).strip()
    except Exception as e:
        log.warning(f"PDF extraction failed for {pdf_path.name}: {e}")
        return ""


def parse_parties(title: str) -> Dict[str, str]:
    if not title:
        return {}
    parts = re.split(r"\s+(?:VS\.?|vs\.?|Vs\.?|v\.)\s+", title, maxsplit=1)
    result = {"full": title.strip()}
    if len(parts) >= 2:
        result["petitioner"] = parts[0].strip()
        result["respondent"] = parts[1].strip()
    return result


# ==============================================================================
# HTTP Session
# ==============================================================================

class FCCSession:
    """HTTP session with Chrome TLS impersonation."""

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

    def _delay(self, min_d: float = None, max_d: float = None):
        lo = min_d or MIN_DELAY
        hi = max_d or MAX_DELAY
        delay = random.uniform(lo, hi)
        time.sleep(delay)
        self.request_count += 1

    def probe_domain(self, url: str, timeout: int = 15) -> Tuple[bool, int, str]:
        """
        Probe a domain. Returns (reachable, status_code, description).
        Does NOT delay — used for fast scanning.
        """
        try:
            r = self.session.get(url, timeout=timeout, allow_redirects=True)
            status = r.status_code
            ct = r.headers.get("Content-Type", "")
            size = len(r.content)
            text_lower = r.text[:2000].lower() if r.text else ""

            # Check for firewall/parking blocks
            if "fortinet" in text_lower or "fortiguard" in text_lower:
                return False, status, "Blocked by FortiGuard"
            if "this domain" in text_lower and "parked" in text_lower:
                return False, status, "Parked domain"
            if "under construction" in text_lower:
                return True, status, "Under construction"
            if "coming soon" in text_lower:
                return True, status, "Coming soon page"
            if "incapsula" in text_lower or "imperva" in text_lower:
                return True, status, f"Incapsula protected ({size} bytes)"

            # Check for real content
            soup = BeautifulSoup(r.text[:5000], "html.parser")
            title = soup.find("title")
            title_text = title.get_text(strip=True) if title else ""

            if status == 200 and size > 500:
                return True, status, f"OK ({size:,} bytes) title='{title_text[:50]}'"
            elif status in (301, 302):
                loc = r.headers.get("Location", "")
                return True, status, f"Redirect → {loc[:80]}"
            elif status == 403:
                return False, status, "Forbidden"
            elif status == 404:
                return False, status, "Not Found"
            else:
                return False, status, f"HTTP {status} ({size} bytes)"

        except cffi_requests.errors.RequestsError as e:
            err_str = str(e)
            if "resolve" in err_str.lower() or "dns" in err_str.lower():
                return False, 0, "DNS resolution failed"
            elif "timed out" in err_str.lower() or "timeout" in err_str.lower():
                return False, 0, "Connection timed out"
            elif "ssl" in err_str.lower():
                return False, 0, f"SSL error: {err_str[:80]}"
            elif "connection" in err_str.lower():
                return False, 0, f"Connection error: {err_str[:80]}"
            return False, 0, f"Error: {err_str[:80]}"
        except Exception as e:
            return False, 0, f"Error: {type(e).__name__}: {str(e)[:80]}"

    def get(self, url: str, timeout: int = PAGE_TIMEOUT, **kwargs) -> Optional[Any]:
        """GET with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    time.sleep(RETRY_BACKOFF * attempt)
                self._delay()
                r = self.session.get(url, timeout=timeout, **kwargs)
                if r.status_code == 200:
                    return r
                elif r.status_code >= 500:
                    log.warning(f"Server error {r.status_code}, will retry...")
                else:
                    return r
            except Exception as e:
                log.error(f"Request failed: {type(e).__name__}: {e}")
                if attempt == MAX_RETRIES - 1:
                    return None
        return None

    def download_pdf(self, url: str, dest: Path) -> bool:
        """Download a PDF file."""
        self._delay(3, 7)
        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    time.sleep(RETRY_BACKOFF * attempt)
                r = self.session.get(url, timeout=PDF_TIMEOUT)
                if r and r.status_code == 200:
                    ct = r.headers.get("Content-Type", "").lower()
                    if "pdf" in ct or r.content[:5] == b"%PDF-":
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        dest.write_bytes(r.content)
                        log.info(f"Downloaded PDF: {dest.name} ({len(r.content):,} bytes)")
                        return True
                    else:
                        log.warning(f"Not a PDF: {url}")
                        return False
                elif r and r.status_code == 404:
                    return False
            except Exception as e:
                log.error(f"PDF download attempt {attempt+1}/{MAX_RETRIES} failed: {e}")
        return False


# ==============================================================================
# Progress Tracking
# ==============================================================================

class ProgressTracker:
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
            "court": "FCC",
            "court_name": "Federal Constitutional Court of Pakistan",
            "started_at": None,
            "last_updated": None,
            "status": "searching",  # "searching", "found", "active", "stub"
            "confirmed_domain": None,
            "domain_probes": [],
            "endpoints_found": [],
            "total_cases_found": 0,
            "total_cases_scraped": 0,
            "total_pdfs_downloaded": 0,
            "total_pdfs_failed": 0,
            "errors": [],
            "notes": [
                "FCC established by 26th Constitutional Amendment (Oct 2023)",
                "Website may not exist yet — scraper will probe and create stub",
            ],
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def add_probe(self, url: str, reachable: bool, status: int, info: str):
        self.data["domain_probes"].append({
            "url": url,
            "reachable": reachable,
            "status": status,
            "info": info,
            "time": datetime.now(timezone.utc).isoformat(),
        })

    def set_confirmed_domain(self, domain: str):
        self.data["confirmed_domain"] = domain
        self.data["status"] = "found"
        self.save()

    def add_endpoint(self, url: str, status: int, info: str):
        self.data["endpoints_found"].append({
            "url": url, "status": status, "info": info[:200],
            "time": datetime.now(timezone.utc).isoformat(),
        })

    def mark_case_done(self, pdf_ok: bool):
        self.data["total_cases_scraped"] += 1
        if pdf_ok:
            self.data["total_pdfs_downloaded"] += 1
        else:
            self.data["total_pdfs_failed"] += 1

    def add_error(self, error: str):
        self.data["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error[:200],
        })
        self.data["errors"] = self.data["errors"][-100:]

    def print_status(self):
        print("\n" + "=" * 60)
        print("  Federal Constitutional Court Scraper — Status Report")
        print("=" * 60)
        print(f"  Court:             {self.data['court_name']}")
        print(f"  Status:            {self.data['status'].upper()}")
        print(f"  Started:           {self.data.get('started_at', 'Never')}")
        print(f"  Last updated:      {self.data.get('last_updated', 'Never')}")
        print(f"  Confirmed domain:  {self.data.get('confirmed_domain', 'Not found yet')}")
        print(f"  Cases scraped:     {self.data['total_cases_scraped']}")
        print(f"  PDFs downloaded:   {self.data['total_pdfs_downloaded']}")
        print()
        if self.data["domain_probes"]:
            print(f"  Domain probes ({len(self.data['domain_probes'])}):")
            for p in self.data["domain_probes"]:
                symbol = "✓" if p["reachable"] else "✗"
                print(f"    {symbol} {p['url']:<50s} {p['info'][:40]}")
        if self.data["endpoints_found"]:
            working = [e for e in self.data["endpoints_found"] if e["status"] == 200]
            print(f"\n  Endpoints ({len(working)} working / {len(self.data['endpoints_found'])} tested):")
            for e in self.data["endpoints_found"]:
                symbol = "✓" if e["status"] == 200 else "?"
                print(f"    {symbol} {e['status']:3d} {e['url'][:60]}")
        if self.data["notes"]:
            print(f"\n  Notes:")
            for note in self.data["notes"]:
                print(f"    • {note}")
        if self.data["errors"]:
            print(f"\n  Recent errors ({len(self.data['errors'])}):")
            for err in self.data["errors"][-5:]:
                print(f"    [{err['time'][:19]}] {err['error'][:80]}")
        print("=" * 60 + "\n")


# ==============================================================================
# HTML Generation
# ==============================================================================

def generate_html(case: FCCCase) -> str:
    title = case.title or case.case_number or "Unknown Case"
    text = case.judgment_text or "(No text extracted)"
    paragraphs = text.split("\n\n") if "\n\n" in text else text.split("\n")
    body_html = "\n".join(f"<p>{html_mod.escape(p.strip())}</p>" for p in paragraphs if p.strip())

    headnote_html = ""
    if case.headnote:
        headnote_html = f"<div class='headnote'><h3>Head Notes</h3><p>{html_mod.escape(case.headnote)}</p></div>"

    judges_str = ", ".join(case.judges) if case.judges else "N/A"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html_mod.escape(title)} - Federal Constitutional Court</title>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.8; color: #333; }}
        .header {{ border-bottom: 2px solid #006400; margin-bottom: 20px; padding-bottom: 10px; }}
        .meta dt {{ font-weight: bold; float: left; clear: left; width: 180px; }}
        .meta dd {{ margin-left: 190px; margin-bottom: 5px; }}
        .headnote {{ background: #f0fff0; padding: 10px; border-left: 4px solid #006400; margin: 15px 0; }}
        .body {{ text-align: justify; }}
        p {{ margin-bottom: 1em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{html_mod.escape(title)}</h1>
        <h2>{html_mod.escape(case.case_number)}</h2>
    </div>
    <dl class="meta">
        <dt>Court:</dt><dd>Federal Constitutional Court of Pakistan</dd>
        <dt>Case Type:</dt><dd>{html_mod.escape(case.case_type or 'N/A')}</dd>
        <dt>Judges:</dt><dd>{html_mod.escape(judges_str)}</dd>
        <dt>Decision Date:</dt><dd>{html_mod.escape(case.decision_date or case.judgment_date or 'N/A')}</dd>
        <dt>Citation:</dt><dd>{html_mod.escape(case.citation or 'N/A')}</dd>
    </dl>
    {headnote_html}
    <div class="body">
        {body_html}
    </div>
</body>
</html>"""


# ==============================================================================
# JSONL
# ==============================================================================

def append_jsonl(filepath: Path, record: dict):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    compact = {k: v for k, v in record.items() if k not in ("judgment_text",)}
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(compact, ensure_ascii=False) + "\n")


# ==============================================================================
# Parsers
# ==============================================================================

def parse_generic_page(html_text: str, base_url: str) -> List[FCCCase]:
    """Parse any page that might contain case listings."""
    cases = []
    soup = BeautifulSoup(html_text, "html.parser")

    # Strategy 1: Tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            case = FCCCase()
            case.source_type = "website"
            case.source_url = base_url
            case.fetched_at = datetime.now(timezone.utc).isoformat()

            for idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                header = headers[idx] if idx < len(headers) else ""

                if any(k in header for k in ("case", "number", "no")):
                    case.case_number = text
                elif any(k in header for k in ("title", "parties", "name")):
                    case.title = text
                    case.parties = parse_parties(text)
                elif any(k in header for k in ("date", "decision")):
                    case.decision_date = normalize_date(text)
                elif any(k in header for k in ("judge",)):
                    case.judges = [j.strip() for j in text.split(",") if j.strip()]
                elif any(k in header for k in ("type",)):
                    case.case_type = text

                link = cell.find("a", href=True)
                if link:
                    href = link["href"]
                    if href.endswith(".pdf") or "pdf" in href.lower():
                        case.pdf_url = urljoin(base_url, href)

            if case.case_number or case.title:
                cases.append(case)

    # Strategy 2: Article/post listings
    for article in soup.find_all(["article", "div"], class_=re.compile(r"(post|entry|judgment|case|decision|item)")):
        title_el = article.find(["h2", "h3", "h4"])
        if not title_el:
            continue

        case = FCCCase()
        case.source_type = "website"
        case.source_url = base_url
        case.fetched_at = datetime.now(timezone.utc).isoformat()
        case.title = title_el.get_text(strip=True)
        case.parties = parse_parties(case.title)

        # Find PDF link
        for link in article.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".pdf") or "pdf" in href.lower():
                case.pdf_url = urljoin(base_url, href)
                break

        # Find date
        date_el = article.find(class_=re.compile(r"(date|time|posted)"))
        if date_el:
            case.decision_date = normalize_date(date_el.get_text(strip=True))

        if case.title:
            cases.append(case)

    # Strategy 3: Any PDF links
    if not cases:
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if href.endswith(".pdf") and text:
                case = FCCCase()
                case.source_type = "website"
                case.source_url = base_url
                case.fetched_at = datetime.now(timezone.utc).isoformat()
                case.title = text
                case.pdf_url = urljoin(base_url, href)
                case.parties = parse_parties(text)
                cases.append(case)

    return cases


# ==============================================================================
# Discovery Mode
# ==============================================================================

def discover(session: FCCSession, progress: ProgressTracker):
    """Probe candidate domains to find the FCC website."""
    print("\n" + "=" * 70)
    print("  Federal Constitutional Court — Website Discovery")
    print("=" * 70)

    if not progress.data["started_at"]:
        progress.data["started_at"] = datetime.now(timezone.utc).isoformat()

    found_domain = None

    # Phase 1: Probe candidate domains
    print(f"\n  Phase 1: Probing {len(CANDIDATE_DOMAINS)} candidate domains...")
    print(f"{'─' * 60}")

    for domain in CANDIDATE_DOMAINS:
        reachable, status, info = session.probe_domain(domain)
        symbol = "✓" if reachable else "✗"
        print(f"  {symbol} {domain:<55s} {info}")
        progress.add_probe(domain, reachable, status, info)

        if reachable and status == 200 and "parked" not in info.lower() and "dns" not in info.lower():
            if not found_domain:
                found_domain = domain
                log.info(f"Found potential FCC website: {domain}")

        time.sleep(1)  # Brief pause between probes

    # Phase 2: If found, probe endpoints
    if found_domain:
        print(f"\n{'─' * 60}")
        print(f"  Phase 2: Probing endpoints on {found_domain}")
        print(f"{'─' * 60}")

        progress.set_confirmed_domain(found_domain)

        for ep in PROBE_ENDPOINTS:
            url = f"{found_domain}{ep}"
            try:
                r = session.session.get(url, timeout=15)
                status = r.status_code
                ct = r.headers.get("Content-Type", "")[:40]
                size = len(r.content)

                is_json = "json" in ct or (r.text and r.text.strip().startswith(("{", "[")))
                is_html = "html" in ct

                symbol = "✓" if status == 200 else "?"
                note = ""
                if is_json:
                    note = "[JSON]"
                elif is_html:
                    # Check for actual content vs error page
                    soup = BeautifulSoup(r.text[:3000], "html.parser")
                    has_table = bool(soup.find("table"))
                    links_count = len(soup.find_all("a"))
                    note = f"[HTML, {links_count} links, {'has table' if has_table else 'no table'}]"

                print(f"  {symbol} {status:3d} {ep:<30s} {size:>8,}B  {ct[:25]} {note}")
                progress.add_endpoint(url, status, f"{ct} | {size}B | {note}")

                if is_json and status == 200:
                    try:
                        data = r.json()
                        if isinstance(data, list):
                            print(f"       → JSON array: {len(data)} items")
                        elif isinstance(data, dict):
                            print(f"       → JSON keys: {list(data.keys())[:5]}")
                    except Exception:
                        pass

            except Exception as e:
                print(f"  ✗ ---  {ep:<30s} ERROR: {e}")

            time.sleep(0.5)

        progress.data["status"] = "found"
    else:
        print(f"\n  No FCC website found. Creating stub for future activation.")
        progress.data["status"] = "stub"
        progress.data["notes"].append(
            f"Discovery run {datetime.now().strftime('%Y-%m-%d')}: No website found. Will retry later."
        )

    progress.save()

    # Create stub file
    create_stub_marker(progress)

    print(f"\n{'=' * 70}")
    print(f"  Discovery complete.")
    print(f"  Status: {progress.data['status'].upper()}")
    if found_domain:
        print(f"  Domain: {found_domain}")
    print(f"{'=' * 70}\n")


def create_stub_marker(progress: ProgressTracker):
    """Create a README in the FCC data directory explaining the court's status."""
    FCC_DIR.mkdir(parents=True, exist_ok=True)

    readme = f"""# Federal Constitutional Court of Pakistan — Data Directory

## Court Status
- **Established:** October 2023 (26th Constitutional Amendment)
- **Scraper Status:** {progress.data['status'].upper()}
- **Last Checked:** {progress.data.get('last_updated', 'Never')}
- **Confirmed Domain:** {progress.data.get('confirmed_domain', 'Not found')}

## Notes
The Federal Constitutional Court (FCC) was established through the 26th
Amendment to the Constitution of Pakistan in October 2023. As a very new
institution, its digital presence may still be under development.

### Domain Probes
"""
    for p in progress.data.get("domain_probes", []):
        symbol = "✓" if p["reachable"] else "✗"
        readme += f"- {symbol} `{p['url']}` — {p['info']}\n"

    if progress.data.get("endpoints_found"):
        readme += "\n### Working Endpoints\n"
        for e in progress.data["endpoints_found"]:
            if e["status"] == 200:
                readme += f"- `{e['url']}` — {e['info'][:60]}\n"

    readme += f"""
## How to Re-check
Run: `python fcc_scraper.py --discover`

This will probe all known/guessed URLs and update this status.

## Data Format
When data becomes available, it will be stored in:
- `FCC/{{year}}/` — JSON case files
- `FCC/{{year}}/pdf/` — Original judgment PDFs
- `html/court_cases/FCC/{{year}}/` — Readable HTML versions
- `BHC_master.jsonl` / `FCC_{{year}}.jsonl` — JSONL format

---
Auto-generated by fcc_scraper.py on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""

    readme_path = FCC_DIR / "README.md"
    readme_path.write_text(readme, encoding="utf-8")
    log.info(f"Created stub marker: {readme_path}")


# ==============================================================================
# Scraping Logic
# ==============================================================================

def scrape(session: FCCSession, progress: ProgressTracker,
           skip_existing: bool = True, limit: int = None):
    """Main scrape function — either scrapes found website or creates stub."""
    domain = progress.data.get("confirmed_domain") or CONFIRMED_DOMAIN

    if not domain:
        # Run discovery first
        log.info("No confirmed FCC domain. Running discovery...")
        discover(session, progress)
        domain = progress.data.get("confirmed_domain")

    if not domain:
        log.info("FCC website not yet found. Stub created.")
        log.info("Re-run with --discover periodically to check for new website.")
        return

    log.info(f"\n{'=' * 60}")
    log.info(f"Scraping FCC: {domain}")
    log.info(f"{'=' * 60}")

    if not progress.data["started_at"]:
        progress.data["started_at"] = datetime.now(timezone.utc).isoformat()

    all_cases: List[FCCCase] = []

    # Probe each endpoint for case data
    for ep in PROBE_ENDPOINTS:
        url = f"{domain}{ep}"
        r = session.get(url)
        if not r or r.status_code != 200:
            continue

        # Try JSON first
        try:
            data = r.json()
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                for key in ("data", "cases", "judgments", "decisions", "results", "d"):
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break

            for item in items:
                if not isinstance(item, dict):
                    continue
                case = FCCCase()
                case.source_type = "api"
                case.source_url = url
                case.fetched_at = datetime.now(timezone.utc).isoformat()

                for fn in ("caseNo", "case_no", "CaseNo", "case_number"):
                    if fn in item:
                        case.case_number = str(item[fn])
                        break
                for fn in ("title", "Title", "parties", "Parties"):
                    if fn in item:
                        case.title = str(item[fn])
                        case.parties = parse_parties(case.title)
                        break
                for fn in ("date", "Date", "decision_date", "judgment_date"):
                    if fn in item:
                        case.decision_date = normalize_date(str(item[fn]))
                        break
                for fn in ("pdf_url", "pdfUrl", "attachment", "link"):
                    if fn in item:
                        val = str(item[fn])
                        case.pdf_url = urljoin(url, val) if val.startswith("/") else val
                        break

                if case.case_number or case.title:
                    all_cases.append(case)

            if items:
                log.info(f"Got {len(items)} items from API: {ep}")
                continue
        except Exception:
            pass

        # Try HTML parsing
        cases = parse_generic_page(r.text, url)
        if cases:
            log.info(f"Got {len(cases)} cases from HTML: {ep}")
            all_cases.extend(cases)

    # Deduplicate
    seen = set()
    unique = []
    for c in all_cases:
        key = c.case_number or c.title or c.pdf_url
        if key and key not in seen:
            seen.add(key)
            unique.append(c)

    log.info(f"Total unique cases found: {len(unique)}")
    progress.data["total_cases_found"] = len(unique)

    if not unique:
        log.info("No cases found on FCC website yet.")
        progress.data["status"] = "found"  # Domain exists but no cases
        progress.data["notes"].append(
            f"Scrape {datetime.now().strftime('%Y-%m-%d')}: Domain found but no case data available yet."
        )
        progress.save()
        create_stub_marker(progress)
        return

    if limit:
        unique = unique[:limit]

    progress.data["status"] = "active"

    for i, case in enumerate(unique):
        log.info(f"  [{i+1}/{len(unique)}] {case.case_number or case.title[:60]}")

        # Download PDF
        pdf_ok = False
        if case.pdf_url:
            year = case.year
            pdf_dir = FCC_DIR / year / "pdf"
            pdf_dir.mkdir(parents=True, exist_ok=True)
            pdf_path = pdf_dir / f"{case.file_key}.pdf"

            if pdf_path.exists():
                pdf_ok = True
                if not case.judgment_text:
                    case.judgment_text = extract_pdf_text(pdf_path)
            else:
                pdf_ok = session.download_pdf(case.pdf_url, pdf_path)
                if pdf_ok:
                    case.judgment_text = extract_pdf_text(pdf_path)

        # Save all formats
        year = case.year
        file_key = case.file_key
        year_dir = FCC_DIR / year
        html_dir_y = HTML_DIR / year
        year_dir.mkdir(parents=True, exist_ok=True)
        html_dir_y.mkdir(parents=True, exist_ok=True)

        # JSON
        json_path = year_dir / f"{file_key}.json"
        if not (skip_existing and json_path.exists()):
            json_path.write_text(json.dumps(case.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")

        # HTML
        html_path = html_dir_y / f"{file_key}.html"
        html_path.write_text(generate_html(case), encoding="utf-8")

        # JSONL
        append_jsonl(JSONL_DIR / f"FCC_{year}.jsonl", case.to_dict())
        append_jsonl(JSONL_DIR / "FCC_master.jsonl", case.to_dict())

        progress.mark_case_done(pdf_ok)

        if (i + 1) % 10 == 0:
            progress.save()

    progress.save()
    create_stub_marker(progress)
    log.info(f"FCC scrape complete: {len(unique)} cases processed")


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="FCC — Federal Constitutional Court of Pakistan Scraper")
    parser.add_argument("--discover", action="store_true",
                        help="Probe for FCC website and discover endpoints")
    parser.add_argument("--status", action="store_true", help="Show status report")
    parser.add_argument("--no-skip", action="store_true", help="Re-scrape existing")
    parser.add_argument("--limit", type=int, help="Limit cases to download")
    args = parser.parse_args()

    progress = ProgressTracker()

    if args.status:
        progress.print_status()
        return

    session = FCCSession()

    if args.discover:
        discover(session, progress)
        return

    skip = not args.no_skip

    try:
        scrape(session, progress, skip_existing=skip, limit=args.limit)
    except KeyboardInterrupt:
        log.info("Interrupted by user")
        progress.save()
    except Exception as e:
        log.error(f"FCC scraper error: {e}")
        progress.add_error(str(e))
        progress.save()

    progress.save()
    progress.print_status()


if __name__ == "__main__":
    main()
