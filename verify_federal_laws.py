#!/usr/bin/env python3
"""
Independent Federal Laws Verifier
==================================
Re-crawls pakistancode.gov.pk to get the ground truth count of federal laws,
then compares against what the scraper has actually downloaded.

This is an INDEPENDENT check — it doesn't trust the scraper's progress.json.
It re-fetches the listing page and counts everything from scratch.

Author: Pakistan Legislation Scraper Project
"""

import io
import os
import re
import sys
import json
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any, Tuple, Set
from collections import Counter

# ── Windows UTF-8 fix ────────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Try requests first, fall back to curl_cffi
USE_CURL_CFFI = False
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    requests = None

try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    curl_requests = None

from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://pakistancode.gov.pk"
ENGLISH_URL = f"{BASE_URL}/english"

# Main listing page for federal laws (the actual working URL)
LIST_PAGE_URL = f"{ENGLISH_URL}/sHyuRiF.php"

# Alternative URLs to try
ALT_LIST_URLS = [
    f"{ENGLISH_URL}/UY2FqaJw2-apaUY2Fqa-apb-sg-jjjjjjjjjjjjj",
    f"{ENGLISH_URL}/LGu0xAD.php",   # Alphabetical order
    f"{ENGLISH_URL}/LGu0xBD.php",   # Chronological order
    f"{ENGLISH_URL}/LGu0xVD.php",   # Category wise
]

# Directories (relative to this script)
PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data_v2" / "federal_laws"
ACTS_DIR = DATA_DIR / "acts"
ACTS_PDF_DIR = ACTS_DIR / "pdfs"
ORDINANCES_DIR = DATA_DIR / "ordinances"
ORDINANCES_PDF_DIR = ORDINANCES_DIR / "pdfs"
HTML_DIR = DATA_DIR / "html"
PROGRESS_FILE = DATA_DIR / "progress.json"
INDEX_FILE = DATA_DIR / "index.json"
JSONL_FILE = DATA_DIR / "all_federal_laws.jsonl"
REPORT_FILE = DATA_DIR / "verification_report.json"

# Polite scraping
MIN_DELAY = 1.5
MAX_DELAY = 3.0
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": ENGLISH_URL + "/",
}


# ══════════════════════════════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stderr)],
)
logger = logging.getLogger("verifier")


# ══════════════════════════════════════════════════════════════════════════════
# HTTP Client
# ══════════════════════════════════════════════════════════════════════════════

def create_session():
    """Create an HTTP session with retry logic."""
    global USE_CURL_CFFI

    if requests and not USE_CURL_CFFI:
        session = requests.Session()
        session.headers.update(HEADERS)
        retry_strategy = Retry(
            total=MAX_RETRIES,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=3, pool_maxsize=3)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
    elif HAS_CURL_CFFI:
        USE_CURL_CFFI = True
        return None  # curl_cffi uses direct calls
    else:
        raise RuntimeError("Neither requests nor curl_cffi available!")


def fetch_url(session, url: str, timeout: int = REQUEST_TIMEOUT) -> Optional[str]:
    """Fetch a URL and return the text content."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if USE_CURL_CFFI:
                resp = curl_requests.get(
                    url, headers=HEADERS, timeout=timeout,
                    impersonate="chrome"
                )
                resp.raise_for_status()
                return resp.text
            else:
                resp = session.get(url, timeout=timeout)
                resp.raise_for_status()
                resp.encoding = 'utf-8'
                return resp.text
        except Exception as e:
            wait = 2.0 * attempt + random.uniform(0.5, 2.0)
            logger.warning(f"Attempt {attempt}/{MAX_RETRIES} failed for {url}: {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"  Retrying in {wait:.1f}s...")
                time.sleep(wait)
            else:
                # If requests fails, try curl_cffi as fallback
                if not USE_CURL_CFFI and HAS_CURL_CFFI and attempt == MAX_RETRIES:
                    logger.info("Falling back to curl_cffi...")
                    try:
                        resp = curl_requests.get(
                            url, headers=HEADERS, timeout=timeout,
                            impersonate="chrome"
                        )
                        resp.raise_for_status()
                        return resp.text
                    except Exception as e2:
                        logger.error(f"curl_cffi also failed: {e2}")
                logger.error(f"All retries exhausted for {url}")
    return None


def polite_delay():
    """Sleep politely between requests."""
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


# ══════════════════════════════════════════════════════════════════════════════
# Helper: Slugify (must match the scraper's slugify)
# ══════════════════════════════════════════════════════════════════════════════

def slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug (matches scraper's logic)."""
    text = text.lower().strip()
    text = text.replace("'", "").replace('"', '').replace(',', '')
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    text = text.strip('-')
    if len(text) > 120:
        text = text[:120].rstrip('-')
    return text


def detect_law_type(title: str) -> str:
    """Detect whether a law is an act, ordinance, order, rules, etc."""
    title_lower = title.lower()
    if 'ordinance' in title_lower:
        return 'ordinance'
    elif 'order' in title_lower and 'act' not in title_lower:
        return 'order'
    elif 'rules' in title_lower and 'act' not in title_lower:
        return 'rules'
    elif 'regulation' in title_lower and 'act' not in title_lower:
        return 'regulation'
    else:
        return 'act'


def extract_year(title: str) -> Optional[int]:
    """Extract year from a title string."""
    m = re.search(r'\b(1[89]\d{2}|20[0-2]\d)\b', title)
    return int(m.group(1)) if m else None


# ══════════════════════════════════════════════════════════════════════════════
# Phase A: Crawl Website — Get Ground Truth
# ══════════════════════════════════════════════════════════════════════════════

def parse_listing_page(session, url: str) -> List[Dict[str, Any]]:
    """Parse a listing page and extract all law entries from accordion sections."""
    html_text = fetch_url(session, url)
    if not html_text:
        logger.error(f"Failed to fetch: {url}")
        return []

    logger.info(f"Fetched listing page: {len(html_text):,} chars from {url}")
    soup = BeautifulSoup(html_text, 'html.parser')

    laws = []

    # Strategy 1: Parse accordion sections in primary-legislation tab
    primary_tab = soup.find('div', id='primary-legislation')
    if primary_tab:
        sections = primary_tab.find_all('div', class_='accordion-section')
        logger.info(f"  Found {len(sections)} entries in primary-legislation tab")
        for i, sec in enumerate(sections):
            law = _parse_accordion_section(sec, i, "primary")
            if law:
                laws.append(law)

    # Strategy 2: Parse accordion sections in secondary-legislation tab
    secondary_tab = soup.find('div', id='secondary-legislation')
    if secondary_tab:
        sections = secondary_tab.find_all('div', class_='accordion-section')
        logger.info(f"  Found {len(sections)} entries in secondary-legislation tab")
        for i, sec in enumerate(sections):
            law = _parse_accordion_section(sec, i, "subordinate")
            if law:
                laws.append(law)

    # Strategy 3: If no accordion found, try generic link patterns
    if not laws:
        logger.info("  No accordion sections found, trying generic link parsing...")
        all_links = soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href', '')
            title = link.get_text(strip=True)
            # Look for law-like links (typical pattern: UY2FqaJw1-...)
            if ('UY2FqaJw1' in href or 'UY2Fqa' in href) and title and len(title) > 10:
                if re.search(r'(act|ordinance|order|rules|regulation)', title, re.IGNORECASE):
                    slug = slugify(title)
                    year = extract_year(title)
                    law_type = detect_law_type(title)
                    source_url = href if href.startswith('http') else f"{ENGLISH_URL}/{href}"
                    laws.append({
                        "title": title,
                        "slug": slug,
                        "type": law_type,
                        "year": year,
                        "source_url": source_url,
                        "tab": "generic",
                    })

    return laws


def _parse_accordion_section(sec, index: int, tab: str) -> Optional[Dict[str, Any]]:
    """Parse a single accordion section into a law dict."""
    try:
        title_div = sec.find('div', class_='accordion-section-title')
        content_div = sec.find('div', class_='accordion-section-content')

        if not title_div:
            return None

        link = title_div.find('a')
        if not link:
            return None

        title = link.get_text(strip=True)
        if not title:
            return None

        relative_url = link.get('href', '')
        source_url = f"{ENGLISH_URL}/{relative_url}" if relative_url else ''

        # Extract metadata from content
        category = ""
        act_number = ""
        promulgation_date = ""
        review_status = "unknown"

        if content_div:
            content_text = content_div.get_text(strip=True)

            # Parse category (before first |)
            cat_match = re.match(r'^([^|]+)\|', content_text)
            if cat_match:
                category = cat_match.group(1).strip()

            # Parse act number (between first and second |)
            act_match = re.search(r'\|\s*(.+?)\s*\|', content_text)
            if act_match:
                act_number = act_match.group(1).strip()

            # Parse promulgation date
            date_match = re.search(r'Promulgation Date:\s*(.+?)(?:\.|$)', content_text)
            if date_match:
                promulgation_date = date_match.group(1).strip()

            # Review status
            if 'Under Final Review' in content_text:
                review_status = "under_final_review"
            elif 'Under Review' in content_text:
                review_status = "under_review"
            elif 'Certified Authentic' in content_text:
                review_status = "certified_authentic"

        slug = slugify(title)
        year = extract_year(title)
        law_type = detect_law_type(title)

        return {
            "title": title,
            "slug": slug,
            "type": law_type,
            "category": category,
            "act_number": act_number,
            "year": year,
            "promulgation_date": promulgation_date,
            "review_status": review_status,
            "source_url": source_url,
            "tab": tab,
            "list_index": index + 1,
        }

    except Exception as e:
        logger.warning(f"Error parsing accordion section {index + 1}: {e}")
        return None


def crawl_all_laws(session) -> Dict[str, Any]:
    """Crawl the website and return all law entries found."""
    logger.info("=" * 60)
    logger.info("PHASE A: Crawling pakistancode.gov.pk for ground truth")
    logger.info("=" * 60)

    all_laws = []
    seen_slugs: Set[str] = set()

    # 1. Main listing page
    logger.info(f"\n[1/5] Fetching main listing page: {LIST_PAGE_URL}")
    main_laws = parse_listing_page(session, LIST_PAGE_URL)
    for law in main_laws:
        if law["slug"] not in seen_slugs:
            seen_slugs.add(law["slug"])
            all_laws.append(law)
    logger.info(f"  -> {len(main_laws)} laws found, {len(all_laws)} unique so far")

    # 2. Try alternative URLs (to cross-check or find more)
    for i, alt_url in enumerate(ALT_LIST_URLS):
        polite_delay()
        logger.info(f"\n[{i+2}/5] Trying alternate URL: {alt_url}")
        alt_laws = parse_listing_page(session, alt_url)
        new_count = 0
        for law in alt_laws:
            if law["slug"] not in seen_slugs:
                seen_slugs.add(law["slug"])
                all_laws.append(law)
                new_count += 1
        logger.info(f"  -> {len(alt_laws)} laws found, {new_count} new unique entries")

    # Summarize by type
    type_counts = Counter(law["type"] for law in all_laws)
    logger.info(f"\nGround truth from website: {len(all_laws)} total unique laws")
    for law_type, count in sorted(type_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {law_type}: {count}")

    return {
        "total": len(all_laws),
        "laws": all_laws,
        "type_counts": dict(type_counts),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Phase B: Check Local Data
# ══════════════════════════════════════════════════════════════════════════════

def check_local_data() -> Dict[str, Any]:
    """Check what we have on disk."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE B: Checking local data on disk")
    logger.info("=" * 60)

    result: Dict[str, Any] = {
        "progress_json": None,
        "index_json": None,
        "files_on_disk": {},
    }

    # 1. Read progress.json
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress = json.load(f)
            result["progress_json"] = {
                "laws_parsed": progress.get("laws_parsed", 0),
                "pdf_urls_found": progress.get("pdf_urls_found", 0),
                "pdfs_downloaded": progress.get("pdfs_downloaded", 0),
                "texts_extracted": progress.get("texts_extracted", 0),
                "htmls_generated": progress.get("htmls_generated", 0),
                "phase1_complete": progress.get("phase1_complete", False),
                "phase2_complete": progress.get("phase2_complete", False),
                "phase3_complete": progress.get("phase3_complete", False),
                "completed_slugs": progress.get("completed_slugs", []),
                "pdf_url_slugs": progress.get("pdf_url_slugs", []),
                "failures_count": len(progress.get("failures", [])),
                "failures": progress.get("failures", []),
            }
            logger.info(f"  progress.json: {result['progress_json']['laws_parsed']} parsed, "
                        f"{result['progress_json']['pdfs_downloaded']} downloaded")
        except Exception as e:
            logger.error(f"  Error reading progress.json: {e}")
    else:
        logger.warning("  progress.json NOT FOUND")

    # 2. Read index.json
    if INDEX_FILE.exists():
        try:
            with open(INDEX_FILE, 'r', encoding='utf-8') as f:
                index = json.load(f)
            index_laws = index.get("laws", [])
            result["index_json"] = {
                "total_laws": index.get("total_laws", 0),
                "types": index.get("types", {}),
                "categories": index.get("categories", {}),
                "law_slugs": [law.get("slug", "") for law in index_laws],
                "laws_with_pdf_url": sum(1 for law in index_laws if law.get("pdf_url")),
                "laws_with_scraped_at": sum(1 for law in index_laws if law.get("scraped_at")),
            }
            logger.info(f"  index.json: {result['index_json']['total_laws']} total, "
                        f"{result['index_json']['laws_with_pdf_url']} with PDF URLs")
        except Exception as e:
            logger.error(f"  Error reading index.json: {e}")
    else:
        logger.warning("  index.json NOT FOUND")

    # 3. Count files on disk
    file_counts = {}

    # Acts - JSON files
    acts_jsons = set()
    if ACTS_DIR.exists():
        for f in ACTS_DIR.glob("*.json"):
            acts_jsons.add(f.stem)
    file_counts["acts_json"] = len(acts_jsons)

    # Acts - PDF files
    acts_pdfs = set()
    if ACTS_PDF_DIR.exists():
        for f in ACTS_PDF_DIR.glob("*.pdf"):
            acts_pdfs.add(f.stem)
    file_counts["acts_pdf"] = len(acts_pdfs)

    # Acts - TXT files
    acts_txts = set()
    if ACTS_DIR.exists():
        for f in ACTS_DIR.glob("*.txt"):
            acts_txts.add(f.stem)
    file_counts["acts_txt"] = len(acts_txts)

    # Ordinances - JSON files
    ord_jsons = set()
    if ORDINANCES_DIR.exists():
        for f in ORDINANCES_DIR.glob("*.json"):
            ord_jsons.add(f.stem)
    file_counts["ordinances_json"] = len(ord_jsons)

    # Ordinances - PDF files
    ord_pdfs = set()
    if ORDINANCES_PDF_DIR.exists():
        for f in ORDINANCES_PDF_DIR.glob("*.pdf"):
            ord_pdfs.add(f.stem)
    file_counts["ordinances_pdf"] = len(ord_pdfs)

    # Ordinances - TXT files
    ord_txts = set()
    if ORDINANCES_DIR.exists():
        for f in ORDINANCES_DIR.glob("*.txt"):
            ord_txts.add(f.stem)
    file_counts["ordinances_txt"] = len(ord_txts)

    # HTML files
    html_files = set()
    if HTML_DIR.exists():
        for f in HTML_DIR.glob("*.html"):
            html_files.add(f.stem)
    file_counts["html"] = len(html_files)

    # JSONL file
    jsonl_count = 0
    jsonl_slugs = set()
    if JSONL_FILE.exists():
        try:
            with open(JSONL_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entry = json.loads(line)
                            jsonl_count += 1
                            slug = entry.get("slug", "")
                            if slug:
                                jsonl_slugs.add(slug)
                        except json.JSONDecodeError:
                            pass
        except Exception:
            pass
    file_counts["jsonl_entries"] = jsonl_count

    # Combine all on-disk slugs
    all_disk_slugs = acts_jsons | ord_jsons
    file_counts["total_json_on_disk"] = len(all_disk_slugs)
    file_counts["total_pdf_on_disk"] = len(acts_pdfs | ord_pdfs)
    file_counts["total_txt_on_disk"] = len(acts_txts | ord_txts)

    result["files_on_disk"] = file_counts
    result["disk_slugs"] = {
        "acts_json": sorted(acts_jsons),
        "acts_pdf": sorted(acts_pdfs),
        "acts_txt": sorted(acts_txts),
        "ordinances_json": sorted(ord_jsons),
        "ordinances_pdf": sorted(ord_pdfs),
        "ordinances_txt": sorted(ord_txts),
        "html": sorted(html_files),
        "jsonl": sorted(jsonl_slugs),
        "all_json": sorted(all_disk_slugs),
        "all_pdf": sorted(acts_pdfs | ord_pdfs),
    }

    logger.info(f"  Files on disk:")
    logger.info(f"    Acts:       {file_counts['acts_json']} JSON, {file_counts['acts_pdf']} PDF, {file_counts['acts_txt']} TXT")
    logger.info(f"    Ordinances: {file_counts['ordinances_json']} JSON, {file_counts['ordinances_pdf']} PDF, {file_counts['ordinances_txt']} TXT")
    logger.info(f"    HTML:       {file_counts['html']}")
    logger.info(f"    JSONL:      {file_counts['jsonl_entries']} entries")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# Phase C: Compare & Generate Report
# ══════════════════════════════════════════════════════════════════════════════

def compare_and_report(
    website_data: Dict[str, Any],
    local_data: Dict[str, Any],
) -> Dict[str, Any]:
    """Compare website ground truth against local data and generate report."""
    logger.info("\n" + "=" * 60)
    logger.info("PHASE C: Comparing website vs local data")
    logger.info("=" * 60)

    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary": {},
        "counts": {},
        "missing_laws": [],
        "extra_laws": [],
        "failed_downloads": [],
        "format_gaps": [],
        "website_laws": [],
    }

    # Website counts
    website_slugs = set(law["slug"] for law in website_data["laws"])
    website_total = website_data["total"]

    # Progress.json counts
    progress = local_data.get("progress_json") or {}
    progress_parsed = progress.get("laws_parsed", 0)
    progress_downloaded = progress.get("pdfs_downloaded", 0)
    progress_completed_slugs = set(progress.get("completed_slugs", []))
    progress_pdf_url_slugs = set(progress.get("pdf_url_slugs", []))

    # Index.json counts
    index = local_data.get("index_json") or {}
    index_total = index.get("total_laws", 0)
    index_slugs = set(index.get("law_slugs", []))

    # Disk counts
    files = local_data.get("files_on_disk", {})
    disk_slugs = local_data.get("disk_slugs", {})
    disk_all_json = set(disk_slugs.get("all_json", []))
    disk_all_pdf = set(disk_slugs.get("all_pdf", []))
    disk_html = set(disk_slugs.get("html", []))
    disk_jsonl = set(disk_slugs.get("jsonl", []))

    # ── Counts Summary ────────────────────────────────────────────────────
    report["counts"] = {
        "website_total": website_total,
        "website_by_type": website_data.get("type_counts", {}),
        "progress_parsed": progress_parsed,
        "progress_pdf_urls": len(progress_pdf_url_slugs),
        "progress_downloaded": progress_downloaded,
        "index_total": index_total,
        "disk_json": files.get("total_json_on_disk", 0),
        "disk_pdf": files.get("total_pdf_on_disk", 0),
        "disk_txt": files.get("total_txt_on_disk", 0),
        "disk_html": files.get("html", 0),
        "disk_jsonl": files.get("jsonl_entries", 0),
    }

    # ── Missing Laws (on website but NOT downloaded) ──────────────────────
    missing_from_disk = website_slugs - disk_all_json
    missing_laws = []
    for law in website_data["laws"]:
        if law["slug"] in missing_from_disk:
            status = "not_started"
            if law["slug"] in progress_pdf_url_slugs:
                status = "pdf_url_found_but_not_downloaded"
            elif law["slug"] in index_slugs:
                status = "indexed_but_not_downloaded"
            missing_laws.append({
                "title": law["title"],
                "slug": law["slug"],
                "type": law["type"],
                "year": law.get("year"),
                "status": status,
            })
    report["missing_laws"] = sorted(missing_laws, key=lambda x: (x.get("year") or 9999, x["title"]))

    # ── Extra Laws (on disk but NOT on website) ───────────────────────────
    extra_on_disk = disk_all_json - website_slugs
    report["extra_laws"] = sorted(extra_on_disk)

    # ── Failed Downloads (in progress.json completed_slugs but missing on disk) ──
    claimed_completed = progress_completed_slugs - disk_all_json
    report["failed_downloads"] = sorted(claimed_completed)

    # ── Format Gaps (JSON exists but some other format missing) ───────────
    format_gaps = []
    for slug in sorted(disk_all_json):
        has_json = slug in disk_all_json
        has_pdf = slug in disk_all_pdf
        has_html = slug in disk_html
        missing_formats = []
        if has_json and not has_pdf:
            missing_formats.append("pdf")
        if has_json and not has_html:
            missing_formats.append("html")
        if missing_formats:
            format_gaps.append({
                "slug": slug,
                "has_json": has_json,
                "has_pdf": has_pdf,
                "has_html": has_html,
                "missing": missing_formats,
            })
    report["format_gaps"] = format_gaps

    # ── Summary ───────────────────────────────────────────────────────────
    download_pct = (files.get("total_json_on_disk", 0) / website_total * 100) if website_total > 0 else 0
    report["summary"] = {
        "website_total_laws": website_total,
        "scraper_thinks_parsed": progress_parsed,
        "actually_on_disk": files.get("total_json_on_disk", 0),
        "download_percentage": round(download_pct, 1),
        "missing_count": len(missing_laws),
        "extra_count": len(extra_on_disk),
        "failed_count": len(claimed_completed),
        "format_gap_count": len(format_gaps),
        "scraper_phase1_complete": progress.get("phase1_complete", False),
        "scraper_phase2_complete": progress.get("phase2_complete", False),
        "scraper_phase3_complete": progress.get("phase3_complete", False),
    }

    # Store compact website laws list
    report["website_laws"] = [
        {"title": law["title"], "slug": law["slug"], "type": law["type"],
         "year": law.get("year"), "tab": law.get("tab")}
        for law in website_data["laws"]
    ]

    return report


# ══════════════════════════════════════════════════════════════════════════════
# Output
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(report: Dict[str, Any]) -> None:
    """Print a clean human-readable summary to stdout."""
    s = report["summary"]
    c = report["counts"]

    print()
    print("=" * 70)
    print("  FEDERAL LAWS VERIFICATION REPORT")
    print(f"  {report['timestamp']}")
    print("=" * 70)

    print()
    print("  COUNTS COMPARISON")
    print("  " + "-" * 50)
    print(f"  Website (ground truth):       {c['website_total']:>6}")
    if c.get("website_by_type"):
        for t, cnt in sorted(c["website_by_type"].items(), key=lambda x: -x[1]):
            print(f"    - {t:25s}  {cnt:>5}")
    print(f"  progress.json (parsed):       {c['progress_parsed']:>6}")
    print(f"  progress.json (PDF URLs):     {c['progress_pdf_urls']:>6}")
    print(f"  progress.json (downloaded):   {c['progress_downloaded']:>6}")
    print(f"  index.json (total):           {c['index_total']:>6}")
    print(f"  Disk — JSON files:            {c['disk_json']:>6}")
    print(f"  Disk — PDF files:             {c['disk_pdf']:>6}")
    print(f"  Disk — TXT files:             {c['disk_txt']:>6}")
    print(f"  Disk — HTML files:            {c['disk_html']:>6}")
    print(f"  Disk — JSONL entries:         {c['disk_jsonl']:>6}")

    print()
    print("  STATUS")
    print("  " + "-" * 50)
    print(f"  Download Progress:  {s['actually_on_disk']}/{s['website_total_laws']} "
          f"({s['download_percentage']:.1f}%)")
    print(f"  Missing Laws:       {s['missing_count']}")
    print(f"  Extra Laws:         {s['extra_count']}")
    print(f"  Failed Downloads:   {s['failed_count']}")
    print(f"  Format Gaps:        {s['format_gap_count']}")

    print()
    print("  SCRAPER PHASE STATUS")
    print("  " + "-" * 50)
    print(f"  Phase 1 (parse list):     {'✓ Complete' if s['scraper_phase1_complete'] else '✗ Incomplete'}")
    print(f"  Phase 2 (get PDF URLs):   {'✓ Complete' if s['scraper_phase2_complete'] else '✗ Incomplete'}")
    print(f"  Phase 3 (download all):   {'✓ Complete' if s['scraper_phase3_complete'] else '✗ Incomplete'}")

    if report["missing_laws"]:
        print()
        print(f"  TOP MISSING LAWS (showing first 20 of {len(report['missing_laws'])})")
        print("  " + "-" * 50)
        for i, law in enumerate(report["missing_laws"][:20]):
            year_str = str(law.get("year", "????"))
            print(f"  {i+1:4d}. [{year_str}] {law['title'][:60]}")
            print(f"        Status: {law['status']}")

    if report["format_gaps"]:
        print()
        print(f"  FORMAT GAPS (showing first 10 of {len(report['format_gaps'])})")
        print("  " + "-" * 50)
        for gap in report["format_gaps"][:10]:
            print(f"    {gap['slug'][:50]:50s} missing: {', '.join(gap['missing'])}")

    print()
    print("=" * 70)
    print(f"  Report saved to: {REPORT_FILE}")
    print("=" * 70)
    print()


def save_report(report: Dict[str, Any]) -> None:
    """Save the full report to JSON."""
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    logger.info(f"Report saved to {REPORT_FILE}")


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    start_time = time.time()
    logger.info("Federal Laws Independent Verifier starting...")

    # Create HTTP session
    session = create_session()

    # Phase A: Crawl website
    website_data = crawl_all_laws(session)

    # Phase B: Check local data
    local_data = check_local_data()

    # Phase C: Compare and report
    report = compare_and_report(website_data, local_data)

    # Add timing
    elapsed = time.time() - start_time
    report["elapsed_seconds"] = round(elapsed, 1)

    # Save and display
    save_report(report)
    print_summary(report)

    logger.info(f"Verification complete in {elapsed:.1f}s")
    return report


if __name__ == "__main__":
    main()
