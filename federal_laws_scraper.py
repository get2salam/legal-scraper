#!/usr/bin/env python3
"""
Pakistan Code Federal Laws Scraper
====================================
Scrapes all federal laws from pakistancode.gov.pk — the official Pakistan Code
maintained by the Ministry of Law and Justice.

Source: https://pakistancode.gov.pk/english/sHyuRiF.php

Outputs 5 formats per law:
  1. JSON  — federal_laws/acts/{slug}.json  (structured metadata + extracted text)
  2. PDF   — federal_laws/acts/pdfs/{slug}.pdf  (original from source)
  3. TXT   — federal_laws/acts/{slug}.txt  (plain text extracted from PDF)
  4. HTML  — federal_laws/html/{slug}.html  (styled readable HTML)
  5. JSONL — federal_laws/all_federal_laws.jsonl  (append-mode master file)

Also generates:
  - federal_laws/index.json  — master index of all laws
  - federal_laws/progress.json  — resume tracking
  - federal_laws/constitution/constitution.pdf  — Constitution PDF

Three-phase approach:
  Phase 1: Parse list page → extract all law metadata + URLs
  Phase 2: Visit each law page → extract PDF URL
  Phase 3: Download PDFs, extract text, generate all 5 formats

Author: Pakistan Legislation Scraper Project
"""

import io
import os
import re
import sys
import json
import time
import random
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List, Any, Tuple

# ── Windows UTF-8 fix ────────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

# PDF extraction — try pdfplumber first (better quality), fall back to PyPDF2
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False


# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://pakistancode.gov.pk"
ENGLISH_URL = f"{BASE_URL}/english"
LIST_PAGE_URL = f"{ENGLISH_URL}/sHyuRiF.php"
CONSTITUTION_PDF_URL = f"{BASE_URL}/pdffiles/constitution.pdf"

# Directories
PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data_v2" / "federal_laws"
ACTS_DIR = DATA_DIR / "acts"
ACTS_PDF_DIR = ACTS_DIR / "pdfs"
ORDINANCES_DIR = DATA_DIR / "ordinances"
ORDINANCES_PDF_DIR = ORDINANCES_DIR / "pdfs"
HTML_DIR = DATA_DIR / "html"
CONSTITUTION_DIR = DATA_DIR / "constitution"
LOG_DIR = DATA_DIR / "logs"

# Files
PROGRESS_FILE = DATA_DIR / "progress.json"
INDEX_FILE = DATA_DIR / "index.json"
JSONL_FILE = DATA_DIR / "all_federal_laws.jsonl"

# Rate limiting — be respectful to .gov.pk
MIN_DELAY = 2.0      # Min seconds between requests
MAX_DELAY = 5.0      # Max seconds between requests
PDF_DELAY = 3.0       # Extra delay for PDF downloads
MAX_RETRIES = 3        # Retry count per request
RETRY_BACKOFF = 5.0    # Exponential backoff base (seconds)
REQUEST_TIMEOUT = 60   # Timeout per request (seconds)
PDF_TIMEOUT = 120      # Timeout for PDF downloads

# User agent — polite identification
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
# Roman Numeral Conversion
# ══════════════════════════════════════════════════════════════════════════════

ROMAN_VALUES = {
    'I': 1, 'V': 5, 'X': 10, 'L': 50,
    'C': 100, 'D': 500, 'M': 1000,
}


def roman_to_int(roman: str) -> Optional[int]:
    """Convert Roman numeral string to integer. Returns None on failure."""
    if not roman or not isinstance(roman, str):
        return None
    roman = roman.strip().upper()
    if not roman:
        return None
    # Check if it's already a number
    try:
        return int(roman)
    except ValueError:
        pass
    # Validate characters
    if not all(c in ROMAN_VALUES for c in roman):
        return None
    total = 0
    prev_value = 0
    for char in reversed(roman):
        value = ROMAN_VALUES[char]
        if value < prev_value:
            total -= value
        else:
            total += value
        prev_value = value
    return total


def slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    text = text.lower().strip()
    # Replace common special chars
    text = text.replace("'", "").replace('"', '').replace(',', '')
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '-', text)
    text = text.strip('-')
    # Truncate to reasonable length
    if len(text) > 120:
        text = text[:120].rstrip('-')
    return text


# ══════════════════════════════════════════════════════════════════════════════
# Logging Setup
# ══════════════════════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    """Configure dual logging to file and stderr."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("federal_laws")
    logger.setLevel(logging.DEBUG)

    # File handler
    fh = logging.FileHandler(
        LOG_DIR / "federal_laws.log",
        encoding='utf-8',
        mode='a',
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    ))
    logger.addHandler(fh)

    # Stderr handler
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%H:%M:%S',
    ))
    logger.addHandler(sh)

    return logger


logger = setup_logging()


# ══════════════════════════════════════════════════════════════════════════════
# HTTP Session with Retry
# ══════════════════════════════════════════════════════════════════════════════

def create_session() -> requests.Session:
    """Create a requests session with retry logic and connection pooling."""
    session = requests.Session()
    session.headers.update(HEADERS)

    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
    )
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=5,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def polite_delay(min_s: float = MIN_DELAY, max_s: float = MAX_DELAY) -> None:
    """Sleep for a random duration to be respectful to the server."""
    delay = random.uniform(min_s, max_s)
    time.sleep(delay)


def fetch_with_retry(
    session: requests.Session,
    url: str,
    timeout: int = REQUEST_TIMEOUT,
    stream: bool = False,
    max_retries: int = MAX_RETRIES,
) -> Optional[requests.Response]:
    """Fetch a URL with manual retry logic and exponential backoff."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout, stream=stream)
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429:
                wait = RETRY_BACKOFF * (2 ** attempt) + random.uniform(1, 5)
                logger.warning(f"Rate limited (429). Waiting {wait:.0f}s... (attempt {attempt}/{max_retries})")
                time.sleep(wait)
            elif status >= 500:
                wait = RETRY_BACKOFF * attempt
                logger.warning(f"Server error {status} for {url}. Retrying in {wait:.0f}s (attempt {attempt}/{max_retries})")
                time.sleep(wait)
            else:
                logger.error(f"HTTP {status} for {url}: {e}")
                return None
        except requests.exceptions.ConnectionError as e:
            wait = RETRY_BACKOFF * attempt + random.uniform(1, 3)
            logger.warning(f"Connection error for {url}. Retrying in {wait:.0f}s (attempt {attempt}/{max_retries}): {e}")
            time.sleep(wait)
        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF * attempt
            logger.warning(f"Timeout for {url}. Retrying in {wait:.0f}s (attempt {attempt}/{max_retries})")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None

    logger.error(f"All {max_retries} retries exhausted for {url}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
# Progress Tracking
# ══════════════════════════════════════════════════════════════════════════════

class ProgressTracker:
    """Track scraping progress for resume support."""

    def __init__(self, path: Path):
        self.path = path
        self.data: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.path.exists():
            try:
                with open(self.path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.warning("Corrupt progress file, starting fresh")
        return {
            "phase1_complete": False,
            "phase2_complete": False,
            "phase3_complete": False,
            "laws_parsed": 0,
            "pdf_urls_found": 0,
            "pdfs_downloaded": 0,
            "texts_extracted": 0,
            "htmls_generated": 0,
            "failures": [],
            "completed_slugs": [],
            "pdf_url_slugs": [],
            "started_at": datetime.now(timezone.utc).isoformat(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

    def save(self) -> None:
        self.data["last_updated"] = datetime.now(timezone.utc).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def is_completed(self, slug: str) -> bool:
        return slug in self.data.get("completed_slugs", [])

    def has_pdf_url(self, slug: str) -> bool:
        return slug in self.data.get("pdf_url_slugs", [])

    def mark_pdf_url(self, slug: str) -> None:
        if slug not in self.data.get("pdf_url_slugs", []):
            self.data.setdefault("pdf_url_slugs", []).append(slug)
            self.data["pdf_urls_found"] = len(self.data["pdf_url_slugs"])

    def mark_completed(self, slug: str) -> None:
        if slug not in self.data.get("completed_slugs", []):
            self.data.setdefault("completed_slugs", []).append(slug)
            self.data["pdfs_downloaded"] = len(self.data["completed_slugs"])

    def add_failure(self, slug: str, error: str) -> None:
        self.data.setdefault("failures", []).append({
            "slug": slug,
            "error": error,
            "time": datetime.now(timezone.utc).isoformat(),
        })

    @property
    def stats(self) -> str:
        d = self.data
        return (
            f"Phase1={d.get('phase1_complete')}, "
            f"Phase2: {d.get('pdf_urls_found', 0)} PDF URLs, "
            f"Phase3: {d.get('pdfs_downloaded', 0)} downloaded, "
            f"{d.get('texts_extracted', 0)} extracted, "
            f"{d.get('htmls_generated', 0)} HTMLs, "
            f"{len(d.get('failures', []))} failures"
        )


# ══════════════════════════════════════════════════════════════════════════════
# PDF Text Extraction
# ══════════════════════════════════════════════════════════════════════════════

def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a PDF file. Tries pdfplumber first, then PyPDF2."""
    text = ""

    if HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                pages = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        pages.append(page_text)
                text = "\n\n".join(pages)
                if text.strip():
                    return text.strip()
        except Exception as e:
            logger.debug(f"pdfplumber failed for {pdf_path.name}: {e}")

    if HAS_PYPDF2:
        try:
            with open(pdf_path, 'rb') as f:
                reader = PyPDF2.PdfReader(f)
                pages = []
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        pages.append(page_text)
                text = "\n\n".join(pages)
                if text.strip():
                    return text.strip()
        except Exception as e:
            logger.debug(f"PyPDF2 failed for {pdf_path.name}: {e}")

    if not text.strip():
        logger.warning(f"Could not extract text from {pdf_path.name} (scanned PDF or encrypted?)")
    return text.strip()


# ══════════════════════════════════════════════════════════════════════════════
# Readable HTML Generator
# ══════════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} | Pakistan Code</title>
    <style>
        :root {{
            --bg-primary: #0d1117;
            --bg-secondary: #161b22;
            --bg-tertiary: #21262d;
            --text-primary: #e6edf3;
            --text-secondary: #8b949e;
            --text-muted: #6e7681;
            --accent: #58a6ff;
            --accent-green: #3fb950;
            --accent-red: #f85149;
            --accent-gold: #d29922;
            --border-color: #30363d;
            --border-muted: #21262d;
        }}

        * {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body {{
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            line-height: 1.8;
            padding: 0;
            min-height: 100vh;
        }}

        .container {{
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 24px;
        }}

        /* Header */
        .law-header {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 32px;
            margin-bottom: 32px;
        }}

        .law-header .source-badge {{
            display: inline-block;
            font-size: 0.75rem;
            color: var(--accent);
            background: rgba(88, 166, 255, 0.1);
            border: 1px solid rgba(88, 166, 255, 0.3);
            border-radius: 12px;
            padding: 2px 12px;
            margin-bottom: 16px;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }}

        .law-title {{
            font-size: 1.75rem;
            font-weight: 600;
            color: var(--text-primary);
            margin-bottom: 20px;
            line-height: 1.3;
        }}

        .metadata-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-top: 16px;
        }}

        .meta-item {{
            background: var(--bg-tertiary);
            border-radius: 6px;
            padding: 12px 16px;
        }}

        .meta-label {{
            font-size: 0.75rem;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.05em;
            margin-bottom: 4px;
        }}

        .meta-value {{
            font-size: 0.95rem;
            color: var(--text-primary);
            font-weight: 500;
        }}

        .status-badge {{
            display: inline-block;
            font-size: 0.8rem;
            font-weight: 600;
            padding: 3px 10px;
            border-radius: 12px;
        }}

        .status-certified {{
            background: rgba(63, 185, 80, 0.15);
            color: var(--accent-green);
            border: 1px solid rgba(63, 185, 80, 0.3);
        }}

        .status-review {{
            background: rgba(210, 153, 34, 0.15);
            color: var(--accent-gold);
            border: 1px solid rgba(210, 153, 34, 0.3);
        }}

        .status-final-review {{
            background: rgba(248, 81, 73, 0.15);
            color: var(--accent-red);
            border: 1px solid rgba(248, 81, 73, 0.3);
        }}

        /* Content */
        .law-content {{
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 32px;
        }}

        .law-content h2 {{
            font-size: 1.3rem;
            color: var(--accent);
            margin: 28px 0 16px 0;
            padding-bottom: 8px;
            border-bottom: 1px solid var(--border-muted);
        }}

        .law-content h2:first-child {{
            margin-top: 0;
        }}

        .law-content h3 {{
            font-size: 1.1rem;
            color: var(--text-primary);
            margin: 20px 0 12px 0;
        }}

        .law-content p {{
            margin-bottom: 12px;
            text-align: justify;
        }}

        .section-number {{
            color: var(--accent);
            font-weight: 600;
        }}

        .law-content .empty-notice {{
            color: var(--text-muted);
            font-style: italic;
            text-align: center;
            padding: 40px;
        }}

        /* Footer */
        .law-footer {{
            margin-top: 24px;
            padding: 16px;
            text-align: center;
            color: var(--text-muted);
            font-size: 0.8rem;
        }}

        .law-footer a {{
            color: var(--accent);
            text-decoration: none;
        }}

        .law-footer a:hover {{
            text-decoration: underline;
        }}

        /* PDF link */
        .pdf-link {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            background: rgba(248, 81, 73, 0.1);
            border: 1px solid rgba(248, 81, 73, 0.3);
            color: var(--accent-red);
            padding: 6px 14px;
            border-radius: 6px;
            text-decoration: none;
            font-size: 0.85rem;
            font-weight: 500;
            margin-top: 12px;
        }}

        .pdf-link:hover {{
            background: rgba(248, 81, 73, 0.2);
        }}

        @media (max-width: 640px) {{
            .container {{ padding: 16px 12px; }}
            .law-header, .law-content {{ padding: 20px 16px; }}
            .law-title {{ font-size: 1.3rem; }}
            .metadata-grid {{ grid-template-columns: 1fr; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="law-header">
            <span class="source-badge">Pakistan Code &middot; Ministry of Law &amp; Justice</span>
            <h1 class="law-title">{title}</h1>
            <div class="metadata-grid">
                <div class="meta-item">
                    <div class="meta-label">Category</div>
                    <div class="meta-value">{category}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Act Number</div>
                    <div class="meta-value">{act_number} of {year}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Promulgation Date</div>
                    <div class="meta-value">{promulgation_date}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Type</div>
                    <div class="meta-value">{law_type}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Status</div>
                    <div class="meta-value"><span class="status-badge {status_class}">{review_status_display}</span></div>
                </div>
            </div>
            {pdf_link_html}
        </div>

        <div class="law-content">
            {body_html}
        </div>

        <div class="law-footer">
            Source: <a href="{source_url}" target="_blank" rel="noopener">pakistancode.gov.pk</a>
            &middot; Generated {generated_at}
        </div>
    </div>
</body>
</html>'''


def text_to_formatted_html(text: str) -> str:
    """Convert raw extracted text to structured HTML with detected sections/headings."""
    if not text or not text.strip():
        return '<p class="empty-notice">Text could not be extracted from the PDF. Please refer to the original PDF document.</p>'

    import html as html_mod
    text = html_mod.escape(text)
    lines = text.split('\n')
    html_parts: List[str] = []
    current_para: List[str] = []

    def flush_para() -> None:
        if current_para:
            para_text = ' '.join(current_para).strip()
            if para_text:
                # Highlight section numbers like "1.", "2.", "2A."
                para_text = re.sub(
                    r'^(\d+[A-Z]?\.)\s',
                    r'<span class="section-number">\1</span> ',
                    para_text,
                )
                html_parts.append(f'<p>{para_text}</p>')
            current_para.clear()

    for line in lines:
        stripped = line.strip()

        # Empty line → paragraph break
        if not stripped:
            flush_para()
            continue

        # Detect headings: ALL CAPS lines, "CHAPTER X", "PART II", "SCHEDULE", "PREAMBLE"
        is_heading = False
        upper = stripped.upper()

        # Chapter/Part/Schedule headings
        if re.match(r'^(CHAPTER|PART|SCHEDULE|PREAMBLE|PRELIMINARY|ANNEXURE)\b', upper):
            is_heading = True
        # Short ALL CAPS lines (likely section titles)
        elif stripped == upper and len(stripped) > 3 and len(stripped) < 120 and stripped.isalpha() is False:
            # Check it's mostly uppercase letters
            alpha_chars = [c for c in stripped if c.isalpha()]
            if alpha_chars and sum(1 for c in alpha_chars if c.isupper()) / len(alpha_chars) > 0.75:
                is_heading = True

        if is_heading:
            flush_para()
            heading_text = stripped
            # Use h2 for major parts, h3 for sub-sections
            if re.match(r'^(CHAPTER|PART|SCHEDULE)', upper):
                html_parts.append(f'<h2>{heading_text}</h2>')
            else:
                html_parts.append(f'<h3>{heading_text}</h3>')
        else:
            current_para.append(stripped)

    flush_para()
    return '\n'.join(html_parts)


def generate_readable_html(law: Dict[str, Any]) -> str:
    """Generate a styled HTML page for a law from its metadata and extracted text."""
    import html as html_mod

    title = html_mod.escape(law.get('title', 'Untitled'))
    category = html_mod.escape(law.get('category', 'Unknown'))
    act_number = html_mod.escape(str(law.get('act_number', '')))
    year = html_mod.escape(str(law.get('year', '')))
    promulgation_date = html_mod.escape(law.get('promulgation_date', 'Unknown'))
    law_type = html_mod.escape(law.get('type', 'act').replace('_', ' ').title())
    review_status = law.get('review_status', 'unknown')
    source_url = html_mod.escape(law.get('source_url', ''))
    pdf_url = law.get('pdf_url', '')
    extracted_text = law.get('extracted_text', '')

    # Status display
    status_map = {
        'certified_authentic': ('Certified Authentic', 'status-certified'),
        'under_final_review': ('Under Final Review', 'status-final-review'),
        'under_review': ('Under Review', 'status-review'),
    }
    status_display, status_class = status_map.get(review_status, (review_status.replace('_', ' ').title(), 'status-review'))

    # PDF link
    pdf_link_html = ''
    if pdf_url:
        pdf_link_html = f'<a class="pdf-link" href="{html_mod.escape(pdf_url)}" target="_blank" rel="noopener">&#128196; Download Original PDF</a>'

    body_html = text_to_formatted_html(extracted_text)
    generated_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    return HTML_TEMPLATE.format(
        title=title,
        category=category,
        act_number=act_number,
        year=year,
        promulgation_date=promulgation_date,
        law_type=law_type,
        review_status_display=status_display,
        status_class=status_class,
        source_url=source_url,
        pdf_link_html=pdf_link_html,
        body_html=body_html,
        generated_at=generated_at,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Phase 1: Parse List Page
# ══════════════════════════════════════════════════════════════════════════════

def parse_promulgation_date(date_str: str) -> str:
    """Parse a promulgation date string into ISO format (YYYY-MM-DD).

    Handles formats like:
      - "December 19 2025"
      - "December 19 2025."
      - "December  ."  (partial dates)
      - "December 23rd September, 1958."  (messy data)
    """
    if not date_str:
        return ""
    date_str = date_str.strip().rstrip('.')

    # Try standard parsing
    for fmt in ("%B %d %Y", "%B %d, %Y", "%B %d %Y", "%b %d %Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue

    # Try extracting month + day + year manually
    m = re.search(r'(\w+)\s+(\d{1,2})\s+(\d{4})', date_str)
    if m:
        try:
            month_str, day, year = m.group(1), m.group(2), m.group(3)
            dt = datetime.strptime(f"{month_str} {day} {year}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Try just month + year
    m = re.search(r'(\w+)\s+(\d{4})', date_str)
    if m:
        try:
            month_str, year = m.group(1), m.group(2)
            dt = datetime.strptime(f"{month_str} 1 {year}", "%B %d %Y")
            return dt.strftime("%Y-%m-01")
        except ValueError:
            pass

    # Just year
    m = re.search(r'(\d{4})', date_str)
    if m:
        return f"{m.group(1)}-01-01"

    return date_str  # Return raw if nothing works


def parse_act_number_text(text: str) -> Tuple[str, Optional[int], Optional[int]]:
    """Parse act number text like 'II of 2026' or 'Act No VI of 1937 of 1923'.

    Returns: (act_number_str, act_number_numeric, year)
    """
    text = text.strip()

    # Pattern: "XXXVIII of 2025" or "3 of 2025" or "Act No VI of 1937 of 1923"
    # Try most specific pattern first
    m = re.match(r'^(?:Act\s*(?:No\.?\s*)?)?([IVXLCDM]+|\d+)\s+of\s+(\d{4})', text, re.IGNORECASE)
    if m:
        num_str = m.group(1).strip()
        year = int(m.group(2))
        num_int = roman_to_int(num_str)
        return num_str, num_int, year

    # Pattern: "Act(XIX of 1838) of 1838"
    m = re.match(r'^Act\(([IVXLCDM]+|\d+)\s+of\s+(\d{4})\)', text, re.IGNORECASE)
    if m:
        num_str = m.group(1).strip()
        year = int(m.group(2))
        num_int = roman_to_int(num_str)
        return num_str, num_int, year

    # Pattern: "Ordinance No.L of 1980"
    m = re.match(r'^Ordinance\s*No\.?\s*([IVXLCDM]+|\d+)\s+of\s+(\d{4})', text, re.IGNORECASE)
    if m:
        num_str = m.group(1).strip()
        year = int(m.group(2))
        num_int = roman_to_int(num_str)
        return num_str, num_int, year

    # Fallback: just try to extract any Roman numeral or number
    m = re.search(r'([IVXLCDM]+|\d+)\s+of\s+(\d{4})', text, re.IGNORECASE)
    if m:
        num_str = m.group(1).strip()
        year = int(m.group(2))
        num_int = roman_to_int(num_str)
        return num_str, num_int, year

    # Just a Roman numeral with no year (e.g., "VIII of ")
    m = re.match(r'^(?:Act\s*(?:No\.?\s*)?|Ordinance\s*No\.?\s*)?([IVXLCDM]+|\d+)', text, re.IGNORECASE)
    if m:
        num_str = m.group(1).strip()
        num_int = roman_to_int(num_str)
        return num_str, num_int, None

    return text.strip(), None, None


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


def parse_list_page(session: requests.Session) -> List[Dict[str, Any]]:
    """Phase 1: Parse the main list page to extract all law metadata.

    Returns a list of law dicts with metadata but without PDF URLs yet.
    """
    logger.info("Phase 1: Fetching main list page...")
    resp = fetch_with_retry(session, LIST_PAGE_URL, timeout=120)
    if not resp:
        logger.error("Failed to fetch list page!")
        return []

    html_text = resp.text
    logger.info(f"List page fetched: {len(html_text):,} chars")

    soup = BeautifulSoup(html_text, 'html.parser')
    primary_tab = soup.find('div', id='primary-legislation')

    if not primary_tab:
        logger.error("Could not find primary-legislation tab!")
        return []

    sections = primary_tab.find_all('div', class_='accordion-section')
    logger.info(f"Found {len(sections)} law entries in primary-legislation tab")

    laws: List[Dict[str, Any]] = []

    for i, sec in enumerate(sections):
        try:
            title_div = sec.find('div', class_='accordion-section-title')
            content_div = sec.find('div', class_='accordion-section-content')

            if not title_div or not content_div:
                logger.warning(f"Entry {i+1}: Missing title or content div, skipping")
                continue

            # Extract title and URL
            link = title_div.find('a')
            if not link:
                logger.warning(f"Entry {i+1}: No link found in title div, skipping")
                continue

            title = link.get_text(strip=True)
            relative_url = link.get('href', '')
            source_url = f"{ENGLISH_URL}/{relative_url}" if relative_url else ''

            # Extract content metadata
            content_text = content_div.get_text(strip=True)
            content_html = str(content_div)

            # Parse category — text before the first "|"
            category = ""
            cat_match = re.match(r'^([^|]+)\|', content_text)
            if cat_match:
                category = cat_match.group(1).strip()

            # Parse act number — text between first and second "|"
            act_number_str = ""
            act_number_numeric = None
            year = None
            act_match = re.search(r'\|\s*(.+?)\s*\|', content_text)
            if act_match:
                act_text = act_match.group(1).strip()
                act_text = re.sub(r'^<p>\s*', '', act_text)  # Clean any leftover tags
                act_number_str, act_number_numeric, year = parse_act_number_text(act_text)

            # Parse promulgation date
            promulgation_date = ""
            date_match = re.search(r'Promulgation Date:\s*(.+?)(?:\.|$)', content_text)
            if date_match:
                promulgation_date = parse_promulgation_date(date_match.group(1).strip())

            # If year not found from act number, try from date or title
            if not year:
                year_match = re.search(r'(\d{4})', title)
                if year_match:
                    year = int(year_match.group(1))

            # Detect review status from visible <font> tags
            review_status = "certified_authentic"  # Default — most entries
            visible_fonts = content_div.find_all('font')
            for font in visible_fonts:
                font_text = font.get_text(strip=True)
                if 'Under Final Review' in font_text:
                    review_status = "under_final_review"
                    break
                elif 'Under Review' in font_text:
                    review_status = "under_review"
                    break

            # Detect type (act vs ordinance)
            law_type = detect_law_type(title)

            slug = slugify(title)
            if not slug:
                slug = f"law-{i+1}"

            law = {
                "title": title,
                "slug": slug,
                "type": law_type,
                "category": category,
                "act_number": act_number_str,
                "act_number_numeric": act_number_numeric,
                "year": year,
                "promulgation_date": promulgation_date,
                "review_status": review_status,
                "source_url": source_url,
                "pdf_url": None,  # Will be filled in Phase 2
                "pdf_path": None,
                "extracted_text": None,
                "scraped_at": None,
                "source": "pakistancode.gov.pk",
                "list_index": i + 1,
            }
            laws.append(law)

        except Exception as e:
            logger.error(f"Error parsing entry {i+1}: {e}", exc_info=True)
            continue

    # Deduplicate by slug (some laws might appear twice — federal + subordinate duplicate)
    seen_slugs: Dict[str, int] = {}
    unique_laws: List[Dict[str, Any]] = []
    for law in laws:
        slug = law["slug"]
        if slug in seen_slugs:
            # Append a suffix
            seen_slugs[slug] += 1
            law["slug"] = f"{slug}-{seen_slugs[slug]}"
        else:
            seen_slugs[slug] = 1
        unique_laws.append(law)

    logger.info(f"Phase 1 complete: {len(unique_laws)} laws parsed")
    return unique_laws


# ══════════════════════════════════════════════════════════════════════════════
# Phase 2: Extract PDF URLs from Individual Law Pages
# ══════════════════════════════════════════════════════════════════════════════

def extract_pdf_url(session: requests.Session, law: Dict[str, Any]) -> Optional[str]:
    """Visit a law's detail page and extract the PDF URL from the iframe."""
    source_url = law.get("source_url")
    if not source_url:
        return None

    resp = fetch_with_retry(session, source_url)
    if not resp:
        return None

    html_text = resp.text

    # Pattern 1: iframe with ViewerJS
    # <iframe src="https://pakistancode.gov.pk/ViewerJS/#../pdffiles/administrator<hash>.pdf"
    m = re.search(r'src=["\']([^"\']*ViewerJS[^"\']*\.pdf)["\']', html_text)
    if m:
        viewer_url = m.group(1)
        # Extract the actual PDF URL from ViewerJS URL
        # ViewerJS URL: https://pakistancode.gov.pk/ViewerJS/#../pdffiles/admin...pdf
        pdf_match = re.search(r'#\.\./(.+\.pdf)', viewer_url)
        if pdf_match:
            return f"{BASE_URL}/{pdf_match.group(1)}"
        # Try direct path
        pdf_match = re.search(r'(pdffiles/.+\.pdf)', viewer_url)
        if pdf_match:
            return f"{BASE_URL}/{pdf_match.group(1)}"

    # Pattern 2: Direct PDF link
    m = re.search(r'href=["\']([^"\']*pdffiles[^"\']*\.pdf)["\']', html_text)
    if m:
        url = m.group(1)
        if url.startswith('http'):
            return url
        return f"{BASE_URL}/{url.lstrip('/')}"

    # Pattern 3: Any PDF URL in the page
    m = re.search(r'(https?://[^\s"\'<>]*pdffiles[^\s"\'<>]*\.pdf)', html_text)
    if m:
        return m.group(1)

    logger.warning(f"No PDF URL found for: {law.get('title', 'unknown')}")
    return None


def phase2_extract_pdf_urls(
    session: requests.Session,
    laws: List[Dict[str, Any]],
    progress: ProgressTracker,
) -> None:
    """Phase 2: Visit each law page to extract PDF URLs."""
    total = len(laws)
    found = 0
    skipped = 0

    logger.info(f"Phase 2: Extracting PDF URLs for {total} laws...")

    for i, law in enumerate(laws):
        slug = law["slug"]

        # Skip if already have PDF URL
        if progress.has_pdf_url(slug) and law.get("pdf_url"):
            skipped += 1
            continue

        logger.info(f"[{i+1}/{total}] Getting PDF URL: {law['title'][:60]}...")
        pdf_url = extract_pdf_url(session, law)

        if pdf_url:
            law["pdf_url"] = pdf_url
            found += 1
            progress.mark_pdf_url(slug)
            logger.debug(f"  → PDF: {pdf_url}")
        else:
            progress.add_failure(slug, "No PDF URL found on detail page")
            logger.warning(f"  → No PDF found")

        # Save progress every 20 laws
        if (i + 1) % 20 == 0:
            progress.save()
            # Save intermediate index
            _save_index(laws)
            logger.info(f"  Progress: {found} PDFs found, {skipped} skipped, {i+1}/{total} processed")

        polite_delay()

    progress.data["phase2_complete"] = True
    progress.save()
    logger.info(f"Phase 2 complete: {found} PDF URLs found, {skipped} skipped")


# ══════════════════════════════════════════════════════════════════════════════
# Phase 3: Download PDFs + Extract Text + Generate All Formats
# ══════════════════════════════════════════════════════════════════════════════

def download_pdf(session: requests.Session, pdf_url: str, output_path: Path) -> bool:
    """Download a PDF file with streaming."""
    try:
        resp = fetch_with_retry(session, pdf_url, timeout=PDF_TIMEOUT, stream=True)
        if not resp:
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        file_size = output_path.stat().st_size
        if file_size < 100:
            logger.warning(f"PDF too small ({file_size} bytes), probably invalid: {output_path.name}")
            output_path.unlink(missing_ok=True)
            return False

        logger.debug(f"Downloaded {output_path.name} ({file_size:,} bytes)")
        return True

    except Exception as e:
        logger.error(f"Failed to download PDF {pdf_url}: {e}")
        return False


def phase3_download_and_process(
    session: requests.Session,
    laws: List[Dict[str, Any]],
    progress: ProgressTracker,
) -> None:
    """Phase 3: Download PDFs, extract text, generate all 5 output formats."""
    total = len(laws)
    downloaded = 0
    skipped = 0
    errors = 0

    logger.info(f"Phase 3: Processing {total} laws (download + extract + generate)...")

    for i, law in enumerate(laws):
        slug = law["slug"]
        law_type = law.get("type", "act")

        # Determine output directory based on type
        if law_type == "ordinance":
            type_dir = ORDINANCES_DIR
            pdf_dir = ORDINANCES_PDF_DIR
        else:
            type_dir = ACTS_DIR
            pdf_dir = ACTS_PDF_DIR

        # Output paths
        json_path = type_dir / f"{slug}.json"
        txt_path = type_dir / f"{slug}.txt"
        pdf_path = pdf_dir / f"{slug}.pdf"
        html_path = HTML_DIR / f"{slug}.html"

        # Skip if fully completed
        if progress.is_completed(slug):
            skipped += 1
            continue

        pdf_url = law.get("pdf_url")
        if not pdf_url:
            logger.warning(f"[{i+1}/{total}] No PDF URL for {law['title'][:50]}, generating metadata only")
            # Still save JSON with what we have
            law["scraped_at"] = datetime.now(timezone.utc).isoformat()
            law["extracted_text"] = ""
            law["pdf_path"] = None
            _save_law_all_formats(law, json_path, txt_path, pdf_path, html_path)
            progress.mark_completed(slug)
            progress.save()
            continue

        logger.info(f"[{i+1}/{total}] Processing: {law['title'][:60]}...")

        # Step 1: Download PDF (if not already exists)
        pdf_downloaded = pdf_path.exists() and pdf_path.stat().st_size > 100
        if not pdf_downloaded:
            polite_delay(PDF_DELAY, PDF_DELAY + 2.0)
            pdf_downloaded = download_pdf(session, pdf_url, pdf_path)

        if pdf_downloaded:
            downloaded += 1
            relative_pdf = f"pdfs/{slug}.pdf"
            law["pdf_path"] = relative_pdf

            # Step 2: Extract text from PDF
            try:
                extracted_text = extract_text_from_pdf(pdf_path)
                law["extracted_text"] = extracted_text
                progress.data["texts_extracted"] = progress.data.get("texts_extracted", 0) + 1
            except Exception as e:
                logger.error(f"  Text extraction failed for {slug}: {e}")
                law["extracted_text"] = ""
        else:
            errors += 1
            law["pdf_path"] = None
            law["extracted_text"] = ""
            progress.add_failure(slug, f"PDF download failed: {pdf_url}")

        # Step 3: Save timestamp
        law["scraped_at"] = datetime.now(timezone.utc).isoformat()

        # Step 4: Save all formats
        _save_law_all_formats(law, json_path, txt_path, pdf_path, html_path)

        # Mark completed
        progress.mark_completed(slug)
        progress.data["htmls_generated"] = progress.data.get("htmls_generated", 0) + 1

        # Save progress every 10 laws
        if (i + 1) % 10 == 0:
            progress.save()
            _save_index(laws)
            logger.info(
                f"  Progress: {downloaded} downloaded, {skipped} skipped, "
                f"{errors} errors, {i+1}/{total} processed"
            )

        polite_delay()

    progress.data["phase3_complete"] = True
    progress.save()
    logger.info(
        f"Phase 3 complete: {downloaded} downloaded, {skipped} skipped, {errors} errors"
    )


def _save_law_all_formats(
    law: Dict[str, Any],
    json_path: Path,
    txt_path: Path,
    pdf_path: Path,
    html_path: Path,
) -> None:
    """Save a law in all required formats: JSON, TXT, HTML, JSONL."""
    slug = law["slug"]

    # 1. JSON
    try:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(law, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Failed to save JSON for {slug}: {e}")

    # 2. TXT (extracted text)
    try:
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        extracted = law.get("extracted_text", "")
        with open(txt_path, 'w', encoding='utf-8') as f:
            # Header
            f.write(f"{'=' * 72}\n")
            f.write(f"{law.get('title', 'Untitled')}\n")
            f.write(f"{'=' * 72}\n")
            f.write(f"Category: {law.get('category', 'N/A')}\n")
            f.write(f"Act Number: {law.get('act_number', 'N/A')} of {law.get('year', 'N/A')}\n")
            f.write(f"Promulgation Date: {law.get('promulgation_date', 'N/A')}\n")
            f.write(f"Status: {law.get('review_status', 'N/A')}\n")
            f.write(f"Source: {law.get('source_url', 'N/A')}\n")
            f.write(f"{'=' * 72}\n\n")
            f.write(extracted if extracted else "[Text could not be extracted from PDF]")
            f.write("\n")
    except Exception as e:
        logger.error(f"Failed to save TXT for {slug}: {e}")

    # 3. HTML (styled readable version)
    try:
        html_path.parent.mkdir(parents=True, exist_ok=True)
        html_content = generate_readable_html(law)
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    except Exception as e:
        logger.error(f"Failed to save HTML for {slug}: {e}")

    # 4. JSONL (append to master file)
    try:
        JSONL_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(JSONL_FILE, 'a', encoding='utf-8') as f:
            # Write compact JSON (no indentation)
            f.write(json.dumps(law, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Failed to append JSONL for {slug}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Index + Constitution
# ══════════════════════════════════════════════════════════════════════════════

def _save_index(laws: List[Dict[str, Any]]) -> None:
    """Save the master index.json with all law metadata (without extracted_text for size)."""
    INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)

    index_entries = []
    for law in laws:
        entry = {k: v for k, v in law.items() if k != 'extracted_text'}
        index_entries.append(entry)

    index = {
        "source": "pakistancode.gov.pk",
        "description": "Federal Laws of Pakistan — Pakistan Code (Ministry of Law and Justice)",
        "total_laws": len(index_entries),
        "types": {
            "acts": sum(1 for l in laws if l.get("type") != "ordinance"),
            "ordinances": sum(1 for l in laws if l.get("type") == "ordinance"),
        },
        "categories": {},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "laws": index_entries,
    }

    # Count by category
    for law in laws:
        cat = law.get("category", "Unknown")
        index["categories"][cat] = index["categories"].get(cat, 0) + 1

    with open(INDEX_FILE, 'w', encoding='utf-8') as f:
        json.dump(index, f, indent=2, ensure_ascii=False)

    logger.info(f"Index saved: {len(index_entries)} laws")


def download_constitution(session: requests.Session) -> None:
    """Download the Constitution PDF separately."""
    CONSTITUTION_DIR.mkdir(parents=True, exist_ok=True)
    pdf_path = CONSTITUTION_DIR / "constitution.pdf"

    if pdf_path.exists() and pdf_path.stat().st_size > 1000:
        logger.info("Constitution PDF already exists, skipping")
        return

    logger.info("Downloading Constitution PDF...")

    # Try known constitution URLs
    constitution_urls = [
        f"{BASE_URL}/pdffiles/constitution.pdf",
        f"{BASE_URL}/pdffiles/administratorconstitution.pdf",
    ]

    # Also try to find it from the homepage
    try:
        resp = fetch_with_retry(session, f"{ENGLISH_URL}/index.php")
        if resp:
            m = re.search(r'(pdffiles/[^\s"\'<>]*constitution[^\s"\'<>]*\.pdf)', resp.text, re.IGNORECASE)
            if m:
                constitution_urls.insert(0, f"{BASE_URL}/{m.group(1)}")
    except Exception:
        pass

    for url in constitution_urls:
        logger.info(f"  Trying: {url}")
        if download_pdf(session, url, pdf_path):
            logger.info(f"Constitution downloaded: {pdf_path}")
            return
        polite_delay(1, 2)

    logger.warning("Could not download Constitution PDF from any known URL")


# ══════════════════════════════════════════════════════════════════════════════
# Main Orchestrator
# ══════════════════════════════════════════════════════════════════════════════

def create_directories() -> None:
    """Ensure all output directories exist."""
    for d in [ACTS_DIR, ACTS_PDF_DIR, ORDINANCES_DIR, ORDINANCES_PDF_DIR,
              HTML_DIR, CONSTITUTION_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def load_cached_laws(progress: ProgressTracker) -> Optional[List[Dict[str, Any]]]:
    """Try to load laws from the index file if Phase 1 was already completed."""
    if not progress.data.get("phase1_complete"):
        return None
    if not INDEX_FILE.exists():
        return None

    try:
        with open(INDEX_FILE, 'r', encoding='utf-8') as f:
            index = json.load(f)

        laws = index.get("laws", [])
        if not laws:
            return None

        # The index doesn't have extracted_text, add it back as None
        for law in laws:
            if "extracted_text" not in law:
                law["extracted_text"] = None

        logger.info(f"Loaded {len(laws)} laws from cached index (Phase 1 already complete)")
        return laws

    except Exception as e:
        logger.warning(f"Failed to load cached index: {e}")
        return None


def rebuild_jsonl(laws: List[Dict[str, Any]]) -> None:
    """Rebuild the JSONL file from individual JSON files (for resume after crash)."""
    logger.info("Rebuilding JSONL from individual JSON files...")
    count = 0

    # Clear existing JSONL
    JSONL_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(JSONL_FILE, 'w', encoding='utf-8') as f:
        for law in laws:
            slug = law["slug"]
            law_type = law.get("type", "act")
            type_dir = ORDINANCES_DIR if law_type == "ordinance" else ACTS_DIR
            json_path = type_dir / f"{slug}.json"

            if json_path.exists():
                try:
                    with open(json_path, 'r', encoding='utf-8') as jf:
                        data = json.load(jf)
                    f.write(json.dumps(data, ensure_ascii=False) + '\n')
                    count += 1
                except Exception:
                    pass

    logger.info(f"JSONL rebuilt: {count} entries")


def main() -> None:
    """Main entry point — runs all three phases."""
    logger.info("=" * 72)
    logger.info("Pakistan Code Federal Laws Scraper")
    logger.info(f"Source: {LIST_PAGE_URL}")
    logger.info(f"Output: {DATA_DIR}")
    logger.info("=" * 72)

    create_directories()
    session = create_session()
    progress = ProgressTracker(PROGRESS_FILE)

    logger.info(f"Resume state: {progress.stats}")

    # ── Phase 1: Parse list page ─────────────────────────────────────────
    laws = load_cached_laws(progress)
    if laws is None:
        laws = parse_list_page(session)
        if not laws:
            logger.error("No laws found! Aborting.")
            return

        progress.data["phase1_complete"] = True
        progress.data["laws_parsed"] = len(laws)
        progress.save()
        _save_index(laws)
        logger.info(f"Phase 1: {len(laws)} laws indexed")
    else:
        logger.info(f"Phase 1: Using cached data ({len(laws)} laws)")

    # Print summary
    types = {}
    cats = {}
    for law in laws:
        t = law.get("type", "act")
        types[t] = types.get(t, 0) + 1
        c = law.get("category", "Unknown")
        cats[c] = cats.get(c, 0) + 1

    logger.info(f"Types: {types}")
    logger.info(f"Categories: {dict(sorted(cats.items(), key=lambda x: -x[1]))}")

    # ── Phase 2: Extract PDF URLs ────────────────────────────────────────
    if not progress.data.get("phase2_complete"):
        phase2_extract_pdf_urls(session, laws, progress)
        _save_index(laws)
    else:
        logger.info("Phase 2: Already complete (PDF URLs extracted)")
        # Load PDF URLs from index
        if INDEX_FILE.exists():
            with open(INDEX_FILE, 'r', encoding='utf-8') as f:
                cached_index = json.load(f)
            cached_laws = {l["slug"]: l for l in cached_index.get("laws", [])}
            for law in laws:
                if not law.get("pdf_url") and law["slug"] in cached_laws:
                    law["pdf_url"] = cached_laws[law["slug"]].get("pdf_url")

    # Log PDF URL stats
    with_pdf = sum(1 for l in laws if l.get("pdf_url"))
    without_pdf = len(laws) - with_pdf
    logger.info(f"PDF URLs: {with_pdf} found, {without_pdf} missing")

    # ── Phase 3: Download + Process ──────────────────────────────────────
    # Clear JSONL before phase 3 to avoid duplicates on restart
    if not progress.data.get("phase3_complete"):
        if JSONL_FILE.exists():
            # Rebuild from what we have so far
            rebuild_jsonl(laws)
        phase3_download_and_process(session, laws, progress)
    else:
        logger.info("Phase 3: Already complete")

    # ── Post-processing ──────────────────────────────────────────────────
    # Final index save
    _save_index(laws)

    # Rebuild JSONL from all individual files (ensures consistency)
    rebuild_jsonl(laws)

    # Download Constitution
    download_constitution(session)

    # Final stats
    pdf_count = sum(1 for d in [ACTS_PDF_DIR, ORDINANCES_PDF_DIR]
                    for f in d.glob("*.pdf") if f.stat().st_size > 100)
    json_count = sum(1 for d in [ACTS_DIR, ORDINANCES_DIR]
                     for f in d.glob("*.json"))
    txt_count = sum(1 for d in [ACTS_DIR, ORDINANCES_DIR]
                    for f in d.glob("*.txt"))
    html_count = sum(1 for f in HTML_DIR.glob("*.html"))
    jsonl_count = 0
    if JSONL_FILE.exists():
        with open(JSONL_FILE, 'r', encoding='utf-8') as f:
            jsonl_count = sum(1 for _ in f)

    logger.info("=" * 72)
    logger.info("SCRAPING COMPLETE")
    logger.info(f"  Laws parsed:    {len(laws)}")
    logger.info(f"  PDFs:           {pdf_count}")
    logger.info(f"  JSONs:          {json_count}")
    logger.info(f"  TXTs:           {txt_count}")
    logger.info(f"  HTMLs:          {html_count}")
    logger.info(f"  JSONL entries:  {jsonl_count}")
    logger.info(f"  Failures:       {len(progress.data.get('failures', []))}")
    logger.info(f"  Output dir:     {DATA_DIR}")
    logger.info("=" * 72)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user. Progress saved.")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
