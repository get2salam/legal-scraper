#!/usr/bin/env python3
"""
BHC Scraper — Balochistan High Court Judgments
================================================
Scrapes judgments from bhc.gov.pk organized by judge name.

Sources:
  1. Judge listing:  bhc.gov.pk/resources/judgments
  2. Per-judge page:  bhc.gov.pk/resources/judgments/justice-{name}
  3. Portal:          portal.bhc.gov.pk/case-status/

Benches: Quetta (principal), Sibi, Turbat

Note: bhc.gov.pk is protected by Incapsula/Imperva CDN.
      Uses curl_cffi with Chrome TLS impersonation to bypass.

Usage:
    python bhc_scraper.py                       # Scrape all judges
    python bhc_scraper.py --discover            # Discover judges & endpoints
    python bhc_scraper.py --status              # Show progress
    python bhc_scraper.py --judge "justice-name" # Specific judge slug
    python bhc_scraper.py --limit 20            # Limit judgments per judge
    python bhc_scraper.py --no-skip             # Re-scrape existing

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
from urllib.parse import urljoin, urlparse, quote

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

BHC_DOMAIN = "https://bhc.gov.pk"
JUDGMENTS_INDEX = f"{BHC_DOMAIN}/resources/judgments"
PORTAL_DOMAIN = "https://portal.bhc.gov.pk"
CASE_STATUS_URL = f"{PORTAL_DOMAIN}/case-status/"

# Benches
BENCHES = {
    "QTA": {"name": "Quetta (Principal Seat)", "aliases": ["Quetta", "at Quetta"]},
    "SBI": {"name": "Sibi Bench", "aliases": ["Sibi", "at Sibi"]},
    "TBT": {"name": "Turbat Bench", "aliases": ["Turbat", "at Turbat"]},
}

# Discovery endpoints to probe
DISCOVERY_ENDPOINTS = [
    "/resources/judgments",
    "/resources/orders",
    "/resources/case-law",
    "/resources/cause-lists",
    "/about/judges",
    "/about/current-judges",
    "/about/former-judges",
    "/api/judgments",
    "/api/cases",
    "/wp-json/wp/v2/posts",
    "/wp-json/wp/v2/pages",
    "/wp-json/wp/v2/categories",
    "/feed/",
    "/sitemap.xml",
    "/robots.txt",
]

PORTAL_ENDPOINTS = [
    "/case-status/",
    "/cause-list/",
    "/daily-orders/",
    "/judgments/",
    "/api/cases",
    "/api/judgments",
]

# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = PROJECT_ROOT / "data_v2"
COURT_DIR = DATA_ROOT / "court_cases"
BHC_DIR = COURT_DIR / "BHC"
HTML_DIR = DATA_ROOT / "html" / "court_cases" / "BHC"
PROGRESS_FILE = COURT_DIR / "bhc_progress.json"
JSONL_DIR = COURT_DIR
LOG_DIR = PROJECT_ROOT / "logs"

# Rate limiting
MIN_DELAY = 5.0
MAX_DELAY = 10.0
PAGE_TIMEOUT = 60
PDF_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 20

# Logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "bhc_scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("bhc_scraper")


# ==============================================================================
# Data Classes
# ==============================================================================

@dataclass
class BHCJudgment:
    """Represents a single BHC judgment."""
    source: str = "BHC"
    judge_name: str = ""
    judge_slug: str = ""
    bench: str = ""          # QTA, SBI, TBT
    bench_name: str = ""
    case_number: str = ""
    case_type: str = ""
    title: str = ""
    parties: Dict[str, str] = field(default_factory=dict)
    judgment_date: str = ""
    citation: str = ""
    statutes: List[str] = field(default_factory=list)
    headnote: str = ""
    judgment_text: str = ""
    pdf_url: str = ""
    pdf_filename: str = ""
    source_url: str = ""
    source_type: str = ""    # "judge_page", "portal", "api"
    fetched_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return {k: v for k, v in d.items() if v or isinstance(v, (bool, int))}

    @property
    def file_key(self) -> str:
        parts = ["BHC"]
        if self.judge_slug:
            parts.append(self.judge_slug[:30])
        if self.case_number:
            parts.append(make_safe_filename(self.case_number))
        elif self.title:
            parts.append(make_safe_filename(self.title)[:50])
        else:
            parts.append(hashlib.md5(json.dumps(self.to_dict()).encode()).hexdigest()[:12])
        return "_".join(parts)

    @property
    def year(self) -> str:
        if self.judgment_date:
            m = re.search(r"\d{4}", self.judgment_date)
            if m:
                return m.group()
        if self.case_number:
            m = re.search(r"[-/](\d{4})", self.case_number)
            if m:
                return m.group(1)
        return "unknown"


# ==============================================================================
# Utility Functions
# ==============================================================================

def make_safe_filename(text: str) -> str:
    """Convert to filesystem-safe filename."""
    s = re.sub(r"[./\\:*?\"<>|]", "_", text.strip())
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s[:120] if s else hashlib.md5(text.encode()).hexdigest()[:12]


def normalize_date(date_str: str) -> str:
    """Convert various date formats to YYYY-MM-DD."""
    if not date_str or date_str.strip() in ("", "-", "N/A", "None"):
        return ""
    date_str = date_str.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d-%B-%Y",
                "%Y-%m-%d", "%m/%d/%Y", "%d %b %Y", "%d %B %Y",
                "%b %d, %Y", "%B %d, %Y", "%d.%m.%Y",
                "%d %b, %Y", "%d %B, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def extract_pdf_text(pdf_path: Path) -> str:
    """Extract text from PDF using PyMuPDF."""
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
    """Parse party names from case title."""
    if not title:
        return {}
    parts = re.split(r"\s+(?:VS\.?|vs\.?|Vs\.?|v\.)\s+", title, maxsplit=1)
    result = {"full": title.strip()}
    if len(parts) >= 2:
        result["petitioner"] = parts[0].strip()
        result["respondent"] = parts[1].strip()
    return result


def detect_bench(text: str) -> Tuple[str, str]:
    """Detect bench from text. Returns (code, name)."""
    text_lower = text.lower()
    for code, info in BENCHES.items():
        for alias in info["aliases"]:
            if alias.lower() in text_lower:
                return code, info["name"]
    return "QTA", "Quetta (Principal Seat)"  # Default to Quetta


# ==============================================================================
# HTTP Session with Incapsula Bypass
# ==============================================================================

class BHCSession:
    """HTTP session with Chrome TLS impersonation for BHC (Incapsula-protected)."""

    def __init__(self):
        self.session = Session(impersonate="chrome")
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        })
        self.request_count = 0
        self._incapsula_cookie_set = False
        self._connectivity: Dict[str, Tuple[bool, str]] = {}

    def _delay(self, min_d: float = None, max_d: float = None):
        lo = min_d or MIN_DELAY
        hi = max_d or MAX_DELAY
        delay = random.uniform(lo, hi)
        if self.request_count > 0 and self.request_count % 20 == 0:
            delay += random.uniform(15, 40)
            log.info(f"Breather after {self.request_count} requests ({delay:.1f}s)")
        time.sleep(delay)
        self.request_count += 1

    def _handle_incapsula(self, response) -> bool:
        """Check if response is an Incapsula challenge and try to handle it."""
        if not response or not response.text:
            return False

        text = response.text[:3000].lower()
        if "incapsula" in text or "imperva" in text or "_incap_" in text:
            log.warning("Incapsula challenge detected")

            # Extract and set any Incapsula cookies
            cookies = response.cookies
            for name, value in cookies.items():
                if "incap" in name.lower() or "visid" in name.lower():
                    self.session.cookies.set(name, value)
                    log.debug(f"Set Incapsula cookie: {name}")

            # Try to extract and execute the Incapsula JavaScript redirect
            # Look for meta refresh or JS redirect
            meta_match = re.search(r'<meta[^>]*http-equiv=["\']refresh["\'][^>]*content=["\'](\d+);?\s*url=([^"\']+)',
                                   response.text, re.IGNORECASE)
            if meta_match:
                redirect_url = meta_match.group(2)
                if redirect_url:
                    log.info(f"Following Incapsula meta-refresh redirect: {redirect_url}")
                    time.sleep(2)
                    r2 = self.session.get(redirect_url, timeout=PAGE_TIMEOUT)
                    if r2 and r2.status_code == 200:
                        self._incapsula_cookie_set = True
                        return True

            # Try waiting and retrying (sometimes Incapsula just needs cookies)
            time.sleep(5)
            self._incapsula_cookie_set = True
            return True

        return False

    def check_connectivity(self, domain: str) -> Tuple[bool, str]:
        """Test if a domain is reachable through Incapsula."""
        if domain in self._connectivity:
            return self._connectivity[domain]
        try:
            r = self.session.get(domain, timeout=20)
            text = r.text[:3000].lower() if r.text else ""

            if r.status_code == 403 and ("fortiguard" in text or "fortinet" in text):
                result = (False, "Blocked by FortiGuard")
                self._connectivity[domain] = result
                return result

            if "incapsula" in text or "imperva" in text:
                self._handle_incapsula(r)
                # Retry after handling challenge
                time.sleep(3)
                r2 = self.session.get(domain, timeout=20)
                if r2 and r2.status_code == 200:
                    result = (True, f"HTTP {r2.status_code} (Incapsula bypassed)")
                else:
                    result = (True, f"HTTP {r.status_code} (Incapsula challenge — may need browser)")
                self._connectivity[domain] = result
                return result

            if r.status_code in (200, 301, 302):
                result = (True, f"HTTP {r.status_code} OK")
                self._connectivity[domain] = result
                return result

            result = (True, f"HTTP {r.status_code}")
            self._connectivity[domain] = result
            return result
        except Exception as e:
            result = (False, f"Error: {e}")
            self._connectivity[domain] = result
            return result

    def get(self, url: str, timeout: int = PAGE_TIMEOUT, **kwargs) -> Optional[Any]:
        """GET with retry logic and Incapsula handling."""
        for attempt in range(MAX_RETRIES):
            try:
                if attempt > 0:
                    backoff = RETRY_BACKOFF * (attempt + 1)
                    log.warning(f"Retry {attempt}/{MAX_RETRIES} after {backoff}s")
                    time.sleep(backoff)

                self._delay()
                r = self.session.get(url, timeout=timeout, **kwargs)

                # Handle Incapsula challenge
                if r and r.text:
                    text_lower = r.text[:3000].lower()
                    if "incapsula" in text_lower or "imperva" in text_lower:
                        log.warning(f"Incapsula challenge on attempt {attempt+1}")
                        self._handle_incapsula(r)
                        # Retry
                        time.sleep(5)
                        r = self.session.get(url, timeout=timeout, **kwargs)

                if r and r.status_code == 200:
                    return r
                elif r and r.status_code == 403:
                    text = r.text[:1000].lower() if r.text else ""
                    if "fortinet" in text or "fortiguard" in text:
                        log.error(f"Blocked by firewall: {url}")
                        return None
                    log.warning(f"HTTP 403 for {url}")
                    return r
                elif r and r.status_code == 429:
                    log.warning("Rate limited (429). Backing off 90s...")
                    time.sleep(90)
                elif r and r.status_code >= 500:
                    log.warning(f"Server error {r.status_code}, will retry...")
                else:
                    status = r.status_code if r else "no response"
                    log.warning(f"HTTP {status} for {url}")
                    return r
            except Exception as e:
                log.error(f"Request failed: {type(e).__name__}: {e}")
                if attempt == MAX_RETRIES - 1:
                    return None
        return None

    def download_pdf(self, url: str, dest: Path) -> bool:
        """Download a PDF file with Incapsula handling."""
        self._delay(3, 8)
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
                    elif "html" in ct:
                        # Might be Incapsula challenge
                        text = r.text[:1000].lower() if r.text else ""
                        if "incapsula" in text or "imperva" in text:
                            log.warning("Incapsula challenge on PDF download")
                            self._handle_incapsula(r)
                            continue  # Retry
                        log.warning(f"Got HTML instead of PDF: {url}")
                        return False
                    else:
                        log.warning(f"Not a PDF: {url} (Content-Type: {ct})")
                        return False
                elif r and r.status_code == 404:
                    log.warning(f"PDF not found (404): {url}")
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
            "court": "BHC",
            "started_at": None,
            "last_updated": None,
            "judges_discovered": [],
            "judges_completed": [],
            "judges_in_progress": {},
            "total_judgments_found": 0,
            "total_judgments_scraped": 0,
            "total_pdfs_downloaded": 0,
            "total_pdfs_failed": 0,
            "incapsula_challenges": 0,
            "discovered_endpoints": {},
            "errors": [],
        }

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.data, indent=2, ensure_ascii=False), encoding="utf-8")

    def mark_judge_start(self, slug: str, total: int):
        if not self.data["started_at"]:
            self.data["started_at"] = datetime.now(timezone.utc).isoformat()
        self.data["judges_in_progress"][slug] = {
            "total": total, "scraped": 0, "pdfs_ok": 0, "pdfs_fail": 0,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        self.save()

    def mark_judgment_done(self, slug: str, pdf_ok: bool):
        if slug in self.data["judges_in_progress"]:
            self.data["judges_in_progress"][slug]["scraped"] += 1
            if pdf_ok:
                self.data["judges_in_progress"][slug]["pdfs_ok"] += 1
            else:
                self.data["judges_in_progress"][slug]["pdfs_fail"] += 1
        self.data["total_judgments_scraped"] += 1
        if pdf_ok:
            self.data["total_pdfs_downloaded"] += 1
        else:
            self.data["total_pdfs_failed"] += 1

    def mark_judge_done(self, slug: str):
        self.data["judges_in_progress"].pop(slug, None)
        if slug not in self.data["judges_completed"]:
            self.data["judges_completed"].append(slug)
        self.save()

    def is_judge_done(self, slug: str) -> bool:
        return slug in self.data["judges_completed"]

    def add_discovered_judge(self, slug: str, name: str, url: str):
        entry = {"slug": slug, "name": name, "url": url}
        # Don't add duplicates
        if not any(j["slug"] == slug for j in self.data["judges_discovered"]):
            self.data["judges_discovered"].append(entry)

    def add_discovered_endpoint(self, domain: str, url: str, status: int, info: str):
        if domain not in self.data["discovered_endpoints"]:
            self.data["discovered_endpoints"][domain] = []
        self.data["discovered_endpoints"][domain].append({
            "url": url, "status": status, "info": info[:200],
            "time": datetime.now(timezone.utc).isoformat(),
        })

    def add_error(self, error: str):
        self.data["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "error": error[:200],
        })
        self.data["errors"] = self.data["errors"][-100:]

    def print_status(self):
        print("\n" + "=" * 60)
        print("  Balochistan High Court Scraper — Progress Report")
        print("=" * 60)
        print(f"  Started:            {self.data.get('started_at', 'Never')}")
        print(f"  Last updated:       {self.data.get('last_updated', 'Never')}")
        print(f"  Judges discovered:  {len(self.data['judges_discovered'])}")
        print(f"  Judges completed:   {len(self.data['judges_completed'])}")
        print(f"  Judgments scraped:  {self.data['total_judgments_scraped']}")
        print(f"  PDFs downloaded:    {self.data['total_pdfs_downloaded']}")
        print(f"  PDFs failed:        {self.data['total_pdfs_failed']}")
        print(f"  Incapsula hits:     {self.data['incapsula_challenges']}")
        print()
        if self.data["judges_discovered"]:
            print(f"  Discovered judges ({len(self.data['judges_discovered'])}):")
            for j in self.data["judges_discovered"]:
                done = "✓" if j["slug"] in self.data["judges_completed"] else " "
                print(f"    [{done}] {j['name']:<45s} ({j['slug']})")
        if self.data["judges_in_progress"]:
            print("\n  In progress:")
            for slug, info in self.data["judges_in_progress"].items():
                print(f"    {slug}: {info['scraped']}/{info['total']} "
                      f"(PDFs: {info['pdfs_ok']} ok, {info['pdfs_fail']} fail)")
        if self.data["errors"]:
            print(f"\n  Recent errors ({len(self.data['errors'])}):")
            for err in self.data["errors"][-5:]:
                print(f"    [{err['time'][:19]}] {err['error'][:80]}")
        print("=" * 60 + "\n")


# ==============================================================================
# HTML Generation
# ==============================================================================

def generate_html(judgment: BHCJudgment) -> str:
    """Generate readable HTML from judgment data."""
    title = judgment.title or judgment.case_number or "Unknown Judgment"
    text = judgment.judgment_text or "(No text extracted)"
    paragraphs = text.split("\n\n") if "\n\n" in text else text.split("\n")
    body_html = "\n".join(f"<p>{html_mod.escape(p.strip())}</p>" for p in paragraphs if p.strip())

    headnote_html = ""
    if judgment.headnote and judgment.headnote.strip() not in ("", "-"):
        headnote_html = f"<div class='headnote'><h3>Head Notes</h3><p>{html_mod.escape(judgment.headnote)}</p></div>"

    statutes_str = ", ".join(judgment.statutes) if judgment.statutes else "N/A"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html_mod.escape(title)} - Balochistan High Court</title>
    <style>
        body {{ font-family: 'Georgia', serif; max-width: 800px; margin: 0 auto; padding: 20px; line-height: 1.8; color: #333; }}
        .header {{ border-bottom: 2px solid #8b0000; margin-bottom: 20px; padding-bottom: 10px; }}
        .meta dt {{ font-weight: bold; float: left; clear: left; width: 180px; }}
        .meta dd {{ margin-left: 190px; margin-bottom: 5px; }}
        .headnote {{ background: #fff0f0; padding: 10px; border-left: 4px solid #8b0000; margin: 15px 0; }}
        .body {{ text-align: justify; }}
        p {{ margin-bottom: 1em; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{html_mod.escape(title)}</h1>
        <h2>{html_mod.escape(judgment.case_number)}</h2>
    </div>
    <dl class="meta">
        <dt>Court:</dt><dd>Balochistan High Court</dd>
        <dt>Bench:</dt><dd>{html_mod.escape(judgment.bench_name or judgment.bench or 'N/A')}</dd>
        <dt>Judge:</dt><dd>{html_mod.escape(judgment.judge_name or 'N/A')}</dd>
        <dt>Case Type:</dt><dd>{html_mod.escape(judgment.case_type or 'N/A')}</dd>
        <dt>Judgment Date:</dt><dd>{html_mod.escape(judgment.judgment_date or 'N/A')}</dd>
        <dt>Citation:</dt><dd>{html_mod.escape(judgment.citation or 'N/A')}</dd>
        <dt>Statutes:</dt><dd>{html_mod.escape(statutes_str)}</dd>
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

def parse_judges_index(html_text: str) -> List[Dict[str, str]]:
    """Parse the /resources/judgments page to extract judge links."""
    judges = []
    soup = BeautifulSoup(html_text, "html.parser")

    # Look for links to individual judge pages
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)

        # Match judge page URLs like /resources/judgments/justice-name-here
        if "/resources/judgments/" in href and "justice" in href.lower():
            slug = href.rstrip("/").split("/")[-1]
            if slug and slug != "judgments":
                full_url = urljoin(BHC_DOMAIN, href)
                judges.append({
                    "name": text or slug.replace("-", " ").title(),
                    "slug": slug,
                    "url": full_url,
                })

    # Also look for list items / cards with judge names
    for item in soup.find_all(["li", "div", "article"], class_=re.compile(r"(judge|justice|item|card|entry)")):
        link = item.find("a", href=True)
        if link:
            href = link["href"]
            text = link.get_text(strip=True) or item.get_text(strip=True)[:100]
            if "/resources/judgments/" in href:
                slug = href.rstrip("/").split("/")[-1]
                if slug and slug != "judgments":
                    full_url = urljoin(BHC_DOMAIN, href)
                    if not any(j["slug"] == slug for j in judges):
                        judges.append({
                            "name": text or slug.replace("-", " ").title(),
                            "slug": slug,
                            "url": full_url,
                        })

    # Deduplicate
    seen = set()
    unique = []
    for j in judges:
        if j["slug"] not in seen:
            seen.add(j["slug"])
            unique.append(j)

    return unique


def parse_judge_page(html_text: str, judge_info: Dict[str, str]) -> List[BHCJudgment]:
    """Parse an individual judge's judgment page."""
    judgments = []
    soup = BeautifulSoup(html_text, "html.parser")
    judge_name = judge_info.get("name", "")
    judge_slug = judge_info.get("slug", "")
    judge_url = judge_info.get("url", "")

    # Strategy 1: Look for tables
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            j = BHCJudgment()
            j.judge_name = judge_name
            j.judge_slug = judge_slug
            j.source_type = "judge_page"
            j.source_url = judge_url
            j.fetched_at = datetime.now(timezone.utc).isoformat()

            for idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                header = headers[idx] if idx < len(headers) else ""

                if any(k in header for k in ("sr", "#", "no", "s.")):
                    pass  # Serial number
                elif any(k in header for k in ("case", "number")):
                    j.case_number = text
                elif any(k in header for k in ("title", "parties", "name", "description")):
                    j.title = text
                    j.parties = parse_parties(text)
                elif any(k in header for k in ("date", "decision")):
                    j.judgment_date = normalize_date(text)
                elif any(k in header for k in ("type",)):
                    j.case_type = text
                elif any(k in header for k in ("citation", "report")):
                    j.citation = text

                # Check for PDF link
                link = cell.find("a", href=True)
                if link:
                    href = link["href"]
                    if href.endswith(".pdf") or "pdf" in href.lower() or "download" in href.lower():
                        j.pdf_url = urljoin(judge_url, href)
                        j.pdf_filename = href.split("/")[-1] if "/" in href else href

            # Detect bench from title or case details
            if j.title:
                bench_code, bench_name = detect_bench(j.title)
                j.bench = bench_code
                j.bench_name = bench_name

            if j.case_number or j.title or j.pdf_url:
                judgments.append(j)

    # Strategy 2: Look for article/post listings (WordPress-style)
    for article in soup.find_all(["article", "div"], class_=re.compile(r"(post|entry|judgment|item|wp-block)")):
        title_el = article.find(["h2", "h3", "h4", "a"])
        if not title_el:
            continue

        j = BHCJudgment()
        j.judge_name = judge_name
        j.judge_slug = judge_slug
        j.source_type = "judge_page"
        j.source_url = judge_url
        j.fetched_at = datetime.now(timezone.utc).isoformat()

        text = title_el.get_text(strip=True)
        j.title = text
        j.parties = parse_parties(text)

        # Extract case number from title
        m = re.search(r"((?:W\.?P\.?|Cr\.?\s*A\.?|C\.?R\.?|C\.?A\.?|C\.?P\.?|H\.?R\.?C\.?)\s*(?:No\.?)?\s*\d+[-/]\d+)", text)
        if m:
            j.case_number = m.group(1)

        # Find PDF link
        for link in article.find_all("a", href=True):
            href = link["href"]
            if href.endswith(".pdf") or "pdf" in href.lower() or "download" in href.lower():
                j.pdf_url = urljoin(judge_url, href)
                j.pdf_filename = href.split("/")[-1] if "/" in href else ""
                break

        # Find date
        date_el = article.find(class_=re.compile(r"(date|time|posted)"))
        if date_el:
            j.judgment_date = normalize_date(date_el.get_text(strip=True))
        else:
            dm = re.search(r"(\d{1,2}[-/.\s]\w+[-/.\s]\d{2,4})", text)
            if dm:
                j.judgment_date = normalize_date(dm.group(1))

        if j.title:
            bench_code, bench_name = detect_bench(j.title)
            j.bench = bench_code
            j.bench_name = bench_name

        if j.case_number or j.pdf_url or (j.title and len(j.title) > 15):
            judgments.append(j)

    # Strategy 3: Plain links to PDFs
    if not judgments:
        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            if href.endswith(".pdf") or ("judgment" in href.lower() and "pdf" in href.lower()):
                j = BHCJudgment()
                j.judge_name = judge_name
                j.judge_slug = judge_slug
                j.source_type = "judge_page"
                j.source_url = judge_url
                j.fetched_at = datetime.now(timezone.utc).isoformat()
                j.title = text or href.split("/")[-1]
                j.pdf_url = urljoin(judge_url, href)
                j.pdf_filename = href.split("/")[-1]
                j.parties = parse_parties(j.title)

                m = re.search(r"((?:W\.?P\.?|Cr\.?\s*A\.?|C\.?R\.?|C\.?A\.?|C\.?P\.?)\s*(?:No\.?)?\s*\d+[-/]\d+)", text)
                if m:
                    j.case_number = m.group(1)

                judgments.append(j)

    # Deduplicate by PDF URL or title
    seen = set()
    unique = []
    for j in judgments:
        key = j.pdf_url or j.title or j.case_number
        if key and key not in seen:
            seen.add(key)
            unique.append(j)

    return unique


def parse_portal_page(html_text: str) -> List[BHCJudgment]:
    """Parse the portal.bhc.gov.pk case status / judgments page."""
    judgments = []
    soup = BeautifulSoup(html_text, "html.parser")

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]

        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            j = BHCJudgment()
            j.source_type = "portal"
            j.source_url = CASE_STATUS_URL
            j.fetched_at = datetime.now(timezone.utc).isoformat()

            for idx, cell in enumerate(cells):
                text = cell.get_text(strip=True)
                header = headers[idx] if idx < len(headers) else ""

                if any(k in header for k in ("case", "number", "no")):
                    j.case_number = text
                elif any(k in header for k in ("title", "parties", "name")):
                    j.title = text
                    j.parties = parse_parties(text)
                elif any(k in header for k in ("date",)):
                    j.judgment_date = normalize_date(text)
                elif any(k in header for k in ("judge",)):
                    j.judge_name = text
                elif any(k in header for k in ("type",)):
                    j.case_type = text
                elif any(k in header for k in ("bench", "seat")):
                    j.bench, j.bench_name = detect_bench(text)

                link = cell.find("a", href=True)
                if link:
                    href = link["href"]
                    if "pdf" in href.lower() or "download" in href.lower():
                        j.pdf_url = urljoin(PORTAL_DOMAIN, href)

            if j.case_number or j.title:
                judgments.append(j)

    return judgments


# ==============================================================================
# Discovery Mode
# ==============================================================================

def discover(session: BHCSession, progress: ProgressTracker):
    """Discover available endpoints and judges."""
    print("\n" + "=" * 70)
    print("  BHC Endpoint & Judge Discovery")
    print("=" * 70)

    # --- Main domain ---
    print(f"\n{'─' * 60}")
    print(f"  Main Domain: {BHC_DOMAIN}")
    print(f"{'─' * 60}")

    reachable, info = session.check_connectivity(BHC_DOMAIN)
    print(f"  Connectivity: {'✓' if reachable else '✗'} {info}")

    if reachable:
        # Probe endpoints
        for ep in DISCOVERY_ENDPOINTS:
            url = f"{BHC_DOMAIN}{ep}"
            try:
                r = session.session.get(url, timeout=15)
                status = r.status_code
                ct = r.headers.get("Content-Type", "")[:40]
                size = len(r.content)
                text_lower = r.text[:2000].lower() if r.text else ""
                is_incap = "incapsula" in text_lower or "imperva" in text_lower
                note = " [INCAPSULA]" if is_incap else ""
                print(f"  {'✓' if status==200 else '?'} {status:3d} {ep:<40s} {size:>8,}B  {ct[:30]}{note}")
                progress.add_discovered_endpoint("bhc.gov.pk", url, status,
                    f"{ct} | {size}B | incapsula={is_incap}")
            except Exception as e:
                print(f"  ✗ ---  {ep:<40s} ERROR: {e}")

        # Try fetching judge list
        print(f"\n  Fetching judge list from {JUDGMENTS_INDEX}...")
        r = session.get(JUDGMENTS_INDEX)
        if r and r.status_code == 200:
            judges = parse_judges_index(r.text)
            print(f"  Found {len(judges)} judges:")
            for j in judges:
                print(f"    - {j['name']:<45s} ({j['slug']})")
                progress.add_discovered_judge(j["slug"], j["name"], j["url"])
        else:
            print(f"  Failed to fetch judge list: {r.status_code if r else 'no response'}")

    # --- Portal domain ---
    print(f"\n{'─' * 60}")
    print(f"  Portal: {PORTAL_DOMAIN}")
    print(f"{'─' * 60}")

    reachable2, info2 = session.check_connectivity(PORTAL_DOMAIN)
    print(f"  Connectivity: {'✓' if reachable2 else '✗'} {info2}")

    if reachable2:
        for ep in PORTAL_ENDPOINTS:
            url = f"{PORTAL_DOMAIN}{ep}"
            try:
                r = session.session.get(url, timeout=15)
                status = r.status_code
                ct = r.headers.get("Content-Type", "")[:40]
                size = len(r.content)
                print(f"  {'✓' if status==200 else '?'} {status:3d} {ep:<40s} {size:>8,}B  {ct[:30]}")
                progress.add_discovered_endpoint("portal.bhc.gov.pk", url, status, f"{ct} | {size}B")
            except Exception as e:
                print(f"  ✗ ---  {ep:<40s} ERROR: {e}")

    progress.save()
    print(f"\n{'=' * 70}")
    print("  Discovery complete. Results saved to progress file.")
    print(f"{'=' * 70}\n")


# ==============================================================================
# Scraping Logic
# ==============================================================================

def save_judgment(judgment: BHCJudgment, skip_existing: bool = True) -> bool:
    """Save a single judgment in all 4 formats. Returns True if saved (not skipped)."""
    year = judgment.year
    file_key = judgment.file_key

    year_dir = BHC_DIR / year
    pdf_dir = year_dir / "pdf"
    html_dir = HTML_DIR / year
    jsonl_path = JSONL_DIR / f"BHC_{year}.jsonl"

    year_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)

    json_path = year_dir / f"{file_key}.json"
    if skip_existing and json_path.exists():
        log.debug(f"Already exists: {file_key}")
        return False

    # 1. JSON
    case_data = judgment.to_dict()
    json_path.write_text(json.dumps(case_data, indent=2, ensure_ascii=False), encoding="utf-8")

    # 2. Readable HTML
    html_path = html_dir / f"{file_key}.html"
    html_path.write_text(generate_html(judgment), encoding="utf-8")

    # 3. JSONL
    append_jsonl(jsonl_path, case_data)
    append_jsonl(JSONL_DIR / "BHC_master.jsonl", case_data)

    return True


def scrape_judge(session: BHCSession, judge_info: Dict[str, str],
                 progress: ProgressTracker, skip_existing: bool = True,
                 limit: int = None):
    """Scrape all judgments for a specific judge."""
    slug = judge_info["slug"]
    name = judge_info["name"]
    url = judge_info["url"]

    if skip_existing and progress.is_judge_done(slug):
        log.info(f"Judge already completed: {name} ({slug})")
        return

    log.info(f"\n{'─' * 50}")
    log.info(f"Scraping: {name}")
    log.info(f"URL: {url}")
    log.info(f"{'─' * 50}")

    r = session.get(url)
    if not r or r.status_code != 200:
        log.error(f"Failed to fetch judge page: {r.status_code if r else 'no response'}")
        progress.add_error(f"Judge {slug}: HTTP {r.status_code if r else 'N/A'}")
        return

    # Check for pagination
    judgments = parse_judge_page(r.text, judge_info)

    # Try pagination — WordPress-style or custom
    soup = BeautifulSoup(r.text, "html.parser")
    page_links = soup.find_all("a", class_=re.compile(r"(page|next|pagination)"))
    next_links = [a for a in page_links if "next" in (a.get_text(strip=True).lower() + a.get("class", [""])[0].lower())]

    page = 2
    while next_links or page <= 3:  # Try at least a few pages
        # Try common pagination patterns
        page_urls = [
            f"{url}/page/{page}/",
            f"{url}?page={page}",
            f"{url}?paged={page}",
        ]
        found_more = False
        for purl in page_urls:
            pr = session.get(purl)
            if pr and pr.status_code == 200:
                more = parse_judge_page(pr.text, judge_info)
                if more:
                    log.info(f"  Page {page}: found {len(more)} more judgments")
                    judgments.extend(more)
                    found_more = True
                    break
        if not found_more:
            break
        page += 1
        if page > 50:  # Safety limit
            break

    log.info(f"Found {len(judgments)} judgments for {name}")

    if not judgments:
        progress.mark_judge_done(slug)
        return

    if limit:
        judgments = judgments[:limit]
        log.info(f"Limited to {limit} judgments")

    progress.data["total_judgments_found"] += len(judgments)
    progress.mark_judge_start(slug, len(judgments))

    for i, judgment in enumerate(judgments):
        log.info(f"  [{i+1}/{len(judgments)}] {judgment.case_number or judgment.title[:60]}")

        # Download PDF if available
        pdf_ok = False
        if judgment.pdf_url:
            pdf_dir = BHC_DIR / judgment.year / "pdf"
            pdf_dir.mkdir(parents=True, exist_ok=True)
            pdf_path = pdf_dir / f"{judgment.file_key}.pdf"

            if pdf_path.exists():
                pdf_ok = True
                if not judgment.judgment_text:
                    judgment.judgment_text = extract_pdf_text(pdf_path)
            else:
                pdf_ok = session.download_pdf(judgment.pdf_url, pdf_path)
                if pdf_ok:
                    judgment.judgment_text = extract_pdf_text(pdf_path)

        # Save in all formats
        save_judgment(judgment, skip_existing)
        progress.mark_judgment_done(slug, pdf_ok)

        if (i + 1) % 10 == 0:
            progress.save()
            log.info(f"  Progress saved: {i+1}/{len(judgments)}")

    progress.mark_judge_done(slug)
    log.info(f"Judge {name} complete: {len(judgments)} judgments processed")


def scrape_portal(session: BHCSession, progress: ProgressTracker,
                  skip_existing: bool = True, limit: int = None):
    """Try scraping from the BHC portal."""
    log.info(f"\n{'=' * 50}")
    log.info(f"Trying BHC Portal: {PORTAL_DOMAIN}")
    log.info(f"{'=' * 50}")

    reachable, info = session.check_connectivity(PORTAL_DOMAIN)
    if not reachable:
        log.warning(f"Portal unreachable: {info}")
        return

    log.info(f"Portal connectivity: {info}")

    # Try case status page
    r = session.get(CASE_STATUS_URL)
    if r and r.status_code == 200:
        judgments = parse_portal_page(r.text)
        if judgments:
            log.info(f"Found {len(judgments)} cases from portal")
            if limit:
                judgments = judgments[:limit]

            for i, j in enumerate(judgments):
                pdf_ok = False
                if j.pdf_url:
                    pdf_dir = BHC_DIR / j.year / "pdf"
                    pdf_dir.mkdir(parents=True, exist_ok=True)
                    pdf_path = pdf_dir / f"{j.file_key}.pdf"
                    if not pdf_path.exists():
                        pdf_ok = session.download_pdf(j.pdf_url, pdf_path)
                        if pdf_ok:
                            j.judgment_text = extract_pdf_text(pdf_path)
                    else:
                        pdf_ok = True

                save_judgment(j, skip_existing)
                progress.mark_judgment_done("portal", pdf_ok)
        else:
            log.info("No judgments found on portal page")
    else:
        log.warning(f"Portal fetch failed: {r.status_code if r else 'no response'}")


# ==============================================================================
# CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="BHC — Balochistan High Court Judgment Scraper")
    parser.add_argument("--discover", action="store_true",
                        help="Discover judges & endpoints")
    parser.add_argument("--status", action="store_true", help="Show progress report")
    parser.add_argument("--judge", type=str, help="Scrape specific judge by slug (e.g. justice-name)")
    parser.add_argument("--no-skip", action="store_true", help="Re-scrape even if done")
    parser.add_argument("--limit", type=int, help="Limit judgments per judge")
    parser.add_argument("--portal", action="store_true", help="Also try portal.bhc.gov.pk")
    args = parser.parse_args()

    progress = ProgressTracker()

    if args.status:
        progress.print_status()
        return

    session = BHCSession()

    if args.discover:
        discover(session, progress)
        return

    skip = not args.no_skip

    # First, get the judge list
    log.info("Fetching judge list from BHC...")
    r = session.get(JUDGMENTS_INDEX)
    judges = []
    if r and r.status_code == 200:
        judges = parse_judges_index(r.text)
        log.info(f"Found {len(judges)} judges")
        for j in judges:
            progress.add_discovered_judge(j["slug"], j["name"], j["url"])
        progress.save()
    else:
        # Fall back to previously discovered judges
        if progress.data["judges_discovered"]:
            judges = progress.data["judges_discovered"]
            log.info(f"Using {len(judges)} previously discovered judges")
        else:
            log.error("Cannot fetch judge list and no cached judges available")
            return

    if args.judge:
        # Find specific judge
        matching = [j for j in judges if j["slug"] == args.judge or args.judge in j["slug"]]
        if not matching:
            log.error(f"Judge '{args.judge}' not found. Available: {[j['slug'] for j in judges]}")
            return
        for j in matching:
            try:
                scrape_judge(session, j, progress, skip_existing=skip, limit=args.limit)
            except Exception as e:
                log.error(f"Error scraping judge {j['slug']}: {e}")
                progress.add_error(f"Judge {j['slug']}: {e}")
    else:
        # Scrape all judges
        for j in judges:
            try:
                scrape_judge(session, j, progress, skip_existing=skip, limit=args.limit)
            except KeyboardInterrupt:
                log.info("Interrupted by user")
                progress.save()
                break
            except Exception as e:
                log.error(f"Error scraping judge {j['slug']}: {e}")
                progress.add_error(f"Judge {j['slug']}: {e}")
                progress.save()

    # Optionally try portal
    if args.portal:
        try:
            scrape_portal(session, progress, skip_existing=skip, limit=args.limit)
        except Exception as e:
            log.error(f"Portal error: {e}")
            progress.add_error(f"Portal: {e}")

    progress.save()
    progress.print_status()


if __name__ == "__main__":
    main()
