#!/usr/bin/env python3
"""
PLS Legislation Scraper v2.1
============================
Scrapes statutes/legislation from pakistanlawsite.com.
Uses curl_cffi for TLS fingerprint impersonation (Chrome 120).

Output Format (matches case law pipeline):
- data_v2/legislation/{LETTER}/{statute_slug}.json - Individual JSON
- data_v2/legislation/{LETTER}/original/{statute_slug}.html - Raw HTML from PLS
- data_v2/html/statutes/{LETTER}/{statute_slug}.html - Clean readable HTML
- data_v2/legislation/{LETTER}.jsonl - JSONL per letter
- data_v2/legislation/all_statutes.jsonl - Master JSONL

JSON Schema:
{
    "citation": "Abandoned Properties Act 1975",
    "statute_id": "c818a4082f67",
    "title": "Abandoned Properties (Taking over and Management) Act 1975",
    "short_title": "Abandoned Properties Act",
    "alphabet": "A",
    "jurisdiction": "Federal",
    "enactment_date": "1975",
    "status": "in_force",
    "preamble": "<clean text>",
    "full_text": "<clean text of all sections combined>",
    "sections": [
        {
            "number": "1",
            "title": "Short title",
            "text": "<clean text>",
            "text_raw": "<raw HTML>",
            "cases_cited": ["1983 PLD 176"]
        }
    ],
    "statutes_cited": [],
    "cases_cited": ["1986 PLD 29", "1983 PLD 176"],
    "fetched_at": "2026-02-07T..."
}
"""

import os
import re
import json
import time
import random
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from html_cleaner import (
    strip_html_to_text, 
    clean_statute_html, 
    extract_preamble,
    normalize_citation,
    extract_case_citations,
    generate_statute_slug
)
from case_link_enricher import enrich_case_links, enrich_statute_case_links

# Pipeline status reporting (optional)
try:
    from pipeline_status import PipelineStatusReporter, ScriptType
    _status_reporter = PipelineStatusReporter(ScriptType.SCRAPER, "legislation_scraper")
    HAS_STATUS_REPORTER = True
except ImportError:
    _status_reporter = None
    HAS_STATUS_REPORTER = False

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
HTML_DIR = DATA_DIR / "html" / "statutes"
PROGRESS_FILE = LEGISLATION_DIR / "progress.json"
INDEX_FILE = LEGISLATION_DIR / "legislation_index.json"
LINKS_FILE = LEGISLATION_DIR / "statute_case_links.jsonl"
STATUTES_JSONL = LEGISLATION_DIR / "all_statutes.jsonl"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing (human-like)
MIN_DELAY = 3.0
MAX_DELAY = 8.0
LOGIN_DELAY = 5.0
RATE_LIMIT_BACKOFF = 60
READING_DELAY_MIN = 2.0
READING_DELAY_MAX = 6.0

# Break simulation
REQUESTS_BEFORE_BREAK = 30
BREAK_MIN = 30
BREAK_MAX = 90

# PLS Operating Hours (PKT = UTC+5) - NIGHT SHIFT for legislation
PLS_OPEN_HOUR = 22   # 10 PM PKT
PLS_CLOSE_HOUR = 5   # 5 AM PKT
PKT_OFFSET = timedelta(hours=5)
NIGHT_MODE = True

# Alphabet list
ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Section:
    """Represents a section of a statute."""
    number: str
    title: str = ""
    text: str = ""           # Clean text
    text_raw: str = ""       # Raw HTML
    cases_cited: List[Dict] = field(default_factory=list)  # Enriched case links
    section_id: str = ""     # PLS internal ID


@dataclass
class Statute:
    """Represents a complete statute with new schema."""
    citation: str            # e.g., "Abandoned Properties Act 1975"
    statute_id: str          # Hash ID
    title: str               # Full title
    short_title: str = ""
    alphabet: str = ""
    jurisdiction: str = "Federal"
    enactment_date: str = ""
    status: str = "in_force"
    preamble: str = ""       # Clean text
    preamble_raw: str = ""   # Raw HTML
    full_text: str = ""      # Clean text of all sections
    full_text_raw: str = ""  # Raw HTML of all sections
    sections: List[Section] = field(default_factory=list)
    statutes_cited: List[str] = field(default_factory=list)
    cases_cited: List[Dict] = field(default_factory=list)  # Enriched case links
    fetched_at: str = ""
    source_url: str = ""
    
    def __post_init__(self):
        if not self.fetched_at:
            self.fetched_at = datetime.now().isoformat()
        if not self.statute_id:
            self.statute_id = hashlib.md5(self.title.encode()).hexdigest()[:12]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "citation": self.citation,
            "statute_id": self.statute_id,
            "title": self.title,
            "short_title": self.short_title,
            "alphabet": self.alphabet,
            "jurisdiction": self.jurisdiction,
            "enactment_date": self.enactment_date,
            "status": self.status,
            "preamble": self.preamble,
            "preamble_raw": self.preamble_raw,
            "full_text": self.full_text,
            "full_text_raw": self.full_text_raw,
            "sections": [asdict(s) if isinstance(s, Section) else s for s in self.sections],
            "statutes_cited": self.statutes_cited,
            "cases_cited": self.cases_cited,
            "fetched_at": self.fetched_at,
            "source_url": self.source_url,
        }


# ══════════════════════════════════════════════════════════════════════════════
# HTML Generator for Statutes
# ══════════════════════════════════════════════════════════════════════════════

def generate_statute_page_html(statute: Dict) -> str:
    """Generate a clean, readable HTML page for a statute."""
    import html as html_module
    
    title = statute.get("title", "Unknown Statute")
    citation = statute.get("citation", title)
    jurisdiction = statute.get("jurisdiction", "Federal")
    year = statute.get("enactment_date", "")
    sections = statute.get("sections", [])
    preamble = statute.get("preamble", "")
    cases_cited = statute.get("cases_cited", [])
    
    # Build section HTML
    sections_html = []
    toc_items = []
    
    for sec in sections:
        sec_num = sec.get("number", "")
        sec_title = sec.get("title", "")
        sec_text = sec.get("text", "")
        sec_cases = sec.get("cases_cited", [])
        
        sec_id = re.sub(r'[^a-zA-Z0-9]', '_', str(sec_num or sec_title))
        
        # TOC entry
        toc_items.append(f'<li><a href="#section-{sec_id}">Section {sec_num}: {html_module.escape(sec_title)}</a></li>')
        
        # Section content
        cases_html = ""
        if sec_cases:
            cases_list = "".join(f'<li><a href="#">{html_module.escape(c)}</a></li>' for c in sec_cases)
            cases_html = f'''
            <div class="cited-cases">
                <strong>Cases citing this section:</strong>
                <ul>{cases_list}</ul>
            </div>
            '''
        
        sections_html.append(f'''
        <section class="statute-section" id="section-{sec_id}">
            <h3 class="section-header">
                <span class="section-number">{html_module.escape(sec_num)}</span>
                <span class="section-title">{html_module.escape(sec_title)}</span>
            </h3>
            <div class="section-content">
                {html_module.escape(sec_text) if sec_text else '<em>Content not available</em>'}
            </div>
            {cases_html}
        </section>
        ''')
    
    # Preamble section
    preamble_html = ""
    if preamble:
        preamble_html = f'''
        <section class="preamble-section">
            <h2>Preamble</h2>
            <div class="preamble-content">{html_module.escape(preamble)}</div>
        </section>
        '''
    
    # Cases cited section
    cases_section = ""
    if cases_cited:
        cases_list = "".join(f'<li><a href="#">{html_module.escape(c)}</a></li>' for c in cases_cited)
        cases_section = f'''
        <section class="all-cases-section">
            <h2>All Cases Citing This Statute ({len(cases_cited)})</h2>
            <ul class="cases-list">{cases_list}</ul>
        </section>
        '''
    
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="{html_module.escape(title)} - Pakistan Legislation">
    <title>{html_module.escape(citation)} | Qanoon Legal Research</title>
    <style>
        :root {{
            --primary-color: #006400;
            --secondary-color: #228B22;
            --text-color: #1a202c;
            --light-text: #4a5568;
            --border-color: #e2e8f0;
            --bg-light: #f7fafc;
            --bg-accent: #f0fff0;
        }}
        * {{ box-sizing: border-box; }}
        body {{
            font-family: 'Georgia', serif;
            line-height: 1.75;
            color: var(--text-color);
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 20px;
            background: #fff;
        }}
        .header {{
            border-bottom: 3px solid var(--primary-color);
            padding-bottom: 25px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            color: var(--primary-color);
            font-size: 1.5rem;
            margin: 0 0 10px 0;
        }}
        .meta {{
            display: flex;
            gap: 20px;
            color: var(--light-text);
            font-size: 0.95rem;
        }}
        .meta-item {{
            background: var(--bg-light);
            padding: 5px 15px;
            border-radius: 4px;
        }}
        .toc {{
            background: var(--bg-light);
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
        }}
        .toc h2 {{
            margin-top: 0;
            color: var(--primary-color);
        }}
        .toc ul {{
            columns: 2;
            column-gap: 30px;
            list-style: none;
            padding: 0;
        }}
        .toc li {{
            margin-bottom: 8px;
        }}
        .toc a {{
            color: var(--secondary-color);
            text-decoration: none;
        }}
        .toc a:hover {{
            text-decoration: underline;
        }}
        .preamble-section {{
            background: var(--bg-accent);
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 30px;
            border-left: 4px solid var(--primary-color);
        }}
        .preamble-section h2 {{
            margin-top: 0;
            color: var(--primary-color);
        }}
        .statute-section {{
            margin-bottom: 30px;
            padding: 20px;
            background: #fff;
            border: 1px solid var(--border-color);
            border-radius: 8px;
        }}
        .section-header {{
            display: flex;
            align-items: center;
            gap: 15px;
            margin: 0 0 15px 0;
            padding-bottom: 10px;
            border-bottom: 1px solid var(--border-color);
        }}
        .section-number {{
            background: var(--primary-color);
            color: white;
            padding: 5px 12px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 0.9rem;
        }}
        .section-title {{
            color: var(--text-color);
            font-weight: 600;
        }}
        .section-content {{
            text-align: justify;
            white-space: pre-wrap;
        }}
        .cited-cases {{
            margin-top: 15px;
            padding-top: 15px;
            border-top: 1px dashed var(--border-color);
            font-size: 0.9rem;
        }}
        .cited-cases ul {{
            margin: 10px 0 0 0;
            padding-left: 20px;
        }}
        .cited-cases li {{
            margin-bottom: 5px;
        }}
        .cited-cases a {{
            color: var(--secondary-color);
        }}
        .all-cases-section {{
            background: var(--bg-light);
            padding: 20px;
            border-radius: 8px;
            margin-top: 40px;
        }}
        .all-cases-section h2 {{
            color: var(--primary-color);
            margin-top: 0;
        }}
        .cases-list {{
            columns: 3;
            column-gap: 20px;
            list-style: none;
            padding: 0;
        }}
        .cases-list li {{
            margin-bottom: 8px;
        }}
        .cases-list a {{
            color: var(--secondary-color);
            text-decoration: none;
        }}
        .footer {{
            margin-top: 50px;
            padding-top: 20px;
            border-top: 1px solid var(--border-color);
            text-align: center;
            color: var(--light-text);
            font-size: 0.85rem;
        }}
        @media (max-width: 600px) {{
            .toc ul, .cases-list {{ columns: 1; }}
        }}
        @media print {{
            body {{ max-width: none; padding: 0; }}
        }}
    </style>
</head>
<body>
    <header class="header">
        <h1>{html_module.escape(title)}</h1>
        <div class="meta">
            <span class="meta-item">📍 {html_module.escape(jurisdiction)}</span>
            {'<span class="meta-item">📅 ' + html_module.escape(year) + '</span>' if year else ''}
            <span class="meta-item">📄 {len(sections)} Sections</span>
        </div>
    </header>
    
    <nav class="toc">
        <h2>Table of Contents</h2>
        <ul>
            {''.join(toc_items)}
        </ul>
    </nav>
    
    {preamble_html}
    
    <main>
        {''.join(sections_html)}
    </main>
    
    {cases_section}
    
    <footer class="footer">
        <p>Generated by <strong>Qanoon Legal Research Platform</strong></p>
        <p>For informational purposes only. Always verify with official sources.</p>
    </footer>
</body>
</html>
'''


# ══════════════════════════════════════════════════════════════════════════════
# Scraper Class
# ══════════════════════════════════════════════════════════════════════════════

class LegislationScraperV2:
    """Pakistan Law Site Legislation Scraper with new output format."""
    
    def __init__(self, ignore_hours: bool = True):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.progress = self._load_progress()
        self.ignore_hours = ignore_hours
        self.requests_since_break = 0
        
        # Create directories
        LEGISLATION_DIR.mkdir(parents=True, exist_ok=True)
        HTML_DIR.mkdir(parents=True, exist_ok=True)
        for letter in ALPHABETS:
            (LEGISLATION_DIR / letter).mkdir(exist_ok=True)
            (LEGISLATION_DIR / letter / "original").mkdir(exist_ok=True)
            (HTML_DIR / letter).mkdir(exist_ok=True)
    
    def _is_pls_open(self) -> bool:
        """Check if within operating hours (10 PM - 5 AM PKT for night shift)."""
        if self.ignore_hours:
            return True
        
        utc_now = datetime.now(timezone.utc)
        pkt_now = utc_now + PKT_OFFSET
        current_hour = pkt_now.hour
        
        if NIGHT_MODE:
            is_open = current_hour >= PLS_OPEN_HOUR or current_hour < PLS_CLOSE_HOUR
        else:
            is_open = PLS_OPEN_HOUR <= current_hour < PLS_CLOSE_HOUR
        
        if not is_open:
            logger.info(f"Outside operating hours (PKT: {pkt_now.strftime('%H:%M')})")
        
        return is_open
    
    def _wait_for_pls_open(self) -> None:
        """Wait until operating window opens."""
        while not self._is_pls_open():
            utc_now = datetime.now(timezone.utc)
            pkt_now = utc_now + PKT_OFFSET
            
            if NIGHT_MODE:
                if pkt_now.hour >= PLS_CLOSE_HOUR and pkt_now.hour < PLS_OPEN_HOUR:
                    open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)
                else:
                    open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)
            else:
                if pkt_now.hour >= PLS_CLOSE_HOUR:
                    tomorrow = pkt_now.date() + timedelta(days=1)
                    open_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, PLS_OPEN_HOUR, 0)
                else:
                    open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)
            
            wait_seconds = (open_time - pkt_now.replace(tzinfo=None)).total_seconds()
            wait_seconds = max(60, wait_seconds)
            
            hours, remainder = divmod(int(wait_seconds), 3600)
            minutes = remainder // 60
            
            logger.info(f"Waiting {hours}h {minutes}m until window opens...")
            chunk = min(300, wait_seconds)
            time.sleep(chunk)
    
    def _maybe_take_break(self) -> None:
        """Take a random break every N requests."""
        self.requests_since_break += 1
        if self.requests_since_break >= REQUESTS_BEFORE_BREAK:
            break_duration = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"Taking a {break_duration:.0f}s break...")
            time.sleep(break_duration)
            self.requests_since_break = 0
    
    def _create_session(self) -> Session:
        """Create a new curl_cffi session with Chrome 120 impersonation."""
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Upgrade-Insecure-Requests": "1",
            "X-Requested-With": "XMLHttpRequest",
        })
        return session
    
    def _human_delay(self, min_s: float = None, max_s: float = None, reading: bool = False):
        """Wait a random human-like delay."""
        if reading:
            min_s = min_s or READING_DELAY_MIN
            max_s = max_s or READING_DELAY_MAX
        else:
            min_s = min_s or MIN_DELAY
            max_s = max_s or MAX_DELAY
        
        delay = random.uniform(min_s, max_s) + random.gauss(0, 0.5)
        delay = max(1.0, delay)
        time.sleep(delay)
    
    def _request(self, method: str, url: str, retries: int = 3, **kwargs) -> Optional[Any]:
        """Make a request with rate limiting and retries."""
        if not self._is_pls_open():
            self._wait_for_pls_open()
            self.logged_in = False
            if not self.login():
                return None
        
        self._maybe_take_break()
        
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY:
            time.sleep(MIN_DELAY - elapsed)
        
        last_error = None
        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, timeout=30, **kwargs)
                else:
                    resp = self.session.post(url, timeout=30, **kwargs)
                
                self.last_request_time = time.time()
                self.request_count += 1
                
                if resp.status_code in [403, 429, 500]:
                    backoff = RATE_LIMIT_BACKOFF * (2 ** attempt)
                    logger.warning(f"{resp.status_code} - backing off {backoff}s...")
                    time.sleep(backoff)
                    continue
                
                if resp.status_code != 200:
                    logger.warning(f"Unexpected status {resp.status_code} for {url}")
                    return None
                
                return resp
                
            except Exception as e:
                last_error = e
                backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                logger.error(f"Request failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(backoff)
        
        logger.error(f"All {retries} attempts failed for {url}: {last_error}")
        return None
    
    def _load_progress(self) -> Dict:
        """Load progress from file."""
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
            except:
                pass
        return {
            "completed_alphabets": [],
            "statutes_scraped": [],
            "current_alphabet": None,
            "total_statutes": 0,
            "last_updated": None
        }
    
    def _save_progress(self):
        """Save progress to file."""
        self.progress["last_updated"] = datetime.now().isoformat()
        PROGRESS_FILE.write_text(json.dumps(self.progress, indent=2), encoding='utf-8')
    
    def _save_statute(self, statute: Statute, raw_html: str = ""):
        """Save statute in all required formats."""
        slug = generate_statute_slug(statute.title)
        letter = statute.alphabet
        
        statute_dir = LEGISLATION_DIR / letter
        html_output_dir = HTML_DIR / letter
        
        # 1. Save individual JSON
        json_path = statute_dir / f"{slug}.json"
        json_path.write_text(json.dumps(statute.to_dict(), indent=2, ensure_ascii=False), encoding='utf-8')
        
        # 2. Save raw HTML (original from PLS)
        if raw_html:
            html_path = statute_dir / "original" / f"{slug}.html"
            html_path.write_text(raw_html, encoding='utf-8')
        
        # 3. Generate and save clean readable HTML
        clean_html = generate_statute_page_html(statute.to_dict())
        clean_html_path = html_output_dir / f"{slug}.html"
        clean_html_path.write_text(clean_html, encoding='utf-8')
        
        # 4. Append to letter-specific JSONL
        letter_jsonl = LEGISLATION_DIR / f"{letter}.jsonl"
        with open(letter_jsonl, 'a', encoding='utf-8') as f:
            f.write(json.dumps(statute.to_dict(), ensure_ascii=False) + '\n')
        
        # 5. Append to master JSONL
        with open(STATUTES_JSONL, 'a', encoding='utf-8') as f:
            f.write(json.dumps(statute.to_dict(), ensure_ascii=False) + '\n')
        
        # 6. Append to case links file
        if statute.cases_cited:
            with open(LINKS_FILE, 'a', encoding='utf-8') as f:
                for citation in statute.cases_cited:
                    entry = {
                        "statute_id": statute.statute_id,
                        "statute_title": statute.title,
                        "case_citation": citation
                    }
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved: {statute.title[:60]} ({len(statute.sections)} sections)")
    
    # ── Login ─────────────────────────────────────────────────────────────────
    
    def login(self) -> bool:
        """Login to PLS."""
        if not self._is_pls_open():
            self._wait_for_pls_open()
        
        logger.info("Logging in to PLS...")
        self.session = self._create_session()
        
        resp = self.session.get(f"{BASE_URL}/", timeout=30)
        if not resp or resp.status_code != 200:
            logger.error("Failed to load homepage")
            return False
        
        self._human_delay(reading=True)
        
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False
        
        csrf_token = csrf_match.group(1)
        self._human_delay(2, 4)
        
        login_resp = None
        for attempt in range(3):
            try:
                login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
                    "Login.UserName": PLS_USER,
                    "Login.Password": PLS_PASS,
                    "__RequestVerificationToken": csrf_token
                }, timeout=30)
                if login_resp and login_resp.status_code == 200:
                    break
                self._human_delay(5, 10)
            except Exception as e:
                logger.warning(f"Login attempt {attempt + 1} error: {e}")
                self._human_delay(5, 10)
        
        if not login_resp or login_resp.status_code != 200:
            logger.error("Login request failed")
            return False
        
        self._human_delay(2, 3)
        
        check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if not check_resp or "pakistanlaws" not in check_resp.text.lower():
            logger.error("Login verification failed")
            return False
        
        self.logged_in = True
        self.requests_since_break = 0
        logger.info("[OK] Login successful!")
        self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
        return True
    
    # ── Statute List ──────────────────────────────────────────────────────────
    
    def get_statutes_by_letter(self, letter: str) -> List[Dict]:
        """Get all statutes starting with a letter."""
        if not self.logged_in:
            if not self.login():
                return []
        
        logger.info(f"Fetching statutes starting with '{letter}'...")
        
        resp = self._request("GET", f"{BASE_URL}/Login/StatuecharSearch", 
                            params={"character": letter})
        if not resp:
            return []
        
        self._human_delay(reading=True)
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        statutes = []
        
        for row in soup.find_all('tr', class_='caseType'):
            caseid = row.get('casetypeid', '')
            if caseid:
                statutes.append({
                    "name": caseid.strip(),
                    "alphabet": letter
                })
        
        logger.info(f"  Found {len(statutes)} statutes for '{letter}'")
        return statutes
    
    # ── Statute Sections ──────────────────────────────────────────────────────
    
    def get_statute_sections(self, statute_name: str) -> List[Dict]:
        """Get all sections of a statute."""
        if not self.logged_in:
            if not self.login():
                return []
        
        resp = self._request("GET", f"{BASE_URL}/Login/GetStatuesSearch",
                            params={"caseName": statute_name})
        if not resp:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        sections = []
        
        for row in soup.find_all('tr', class_='table_row_hover'):
            cells = row.find_all('td')
            if len(cells) >= 4:
                read_cell = cells[0]
                section_num = cells[1].get_text(strip=True)
                act_name = cells[2].get_text(strip=True)
                definition = cells[3].get_text(strip=True)
                
                section_id = read_cell.get('casetypeid', '')
                if not section_id:
                    link = read_cell.find(class_='readCaseLaw')
                    if link:
                        section_id = link.get('casetypeid', '')
                
                case_cell = cells[4] if len(cells) > 4 else None
                case_id = case_cell.get('casetypeid', '') if case_cell else ''
                
                sections.append({
                    "section_id": section_id,
                    "number": section_num,
                    "act_name": act_name,
                    "definition": definition,
                    "case_type_id": case_id,
                })
        
        return sections
    
    # ── Section Content ───────────────────────────────────────────────────────
    
    def _is_error_response(self, text: str) -> bool:
        """Check if response is an error."""
        if not text:
            return True
        text_stripped = text.strip().lower()
        error_responses = ["-1", "1", "-2", "error", "null", "undefined", ""]
        if text_stripped in error_responses:
            return True
        # Very short numeric responses are likely errors
        if len(text_stripped) <= 3 and text_stripped.lstrip('-').isdigit():
            return True
        return False
    
    def _is_content_valid(self, text: str) -> bool:
        """Check if content is valid (not too short)."""
        if not text:
            return False
        clean = strip_html_to_text(text) if '<' in text else text
        # Minimum 50 chars for valid content
        return len(clean.strip()) >= 50
    
    def get_section_content(self, section_id: str, retries: int = 3) -> tuple:
        """
        Get the content of a section with retry logic.
        
        Returns (raw_html, clean_text) or ("", "") if all retries fail.
        Detects error responses and retries with backoff.
        """
        if not section_id:
            return "", ""
        
        for attempt in range(retries):
            resp = self._request("POST", f"{BASE_URL}/Login/SearchStatueFile",
                                data={"caseTypeId": section_id})
            
            if not resp:
                logger.warning(f"Section {section_id}: No response (attempt {attempt + 1}/{retries})")
                time.sleep(RATE_LIMIT_BACKOFF * (attempt + 1))
                continue
            
            raw_html = resp.text
            
            # Check for error responses
            if self._is_error_response(raw_html):
                logger.warning(f"Section {section_id}: Error response '{raw_html[:20]}' (attempt {attempt + 1}/{retries})")
                time.sleep(5 * (attempt + 1))  # Shorter backoff for section retries
                continue
            
            clean_text = strip_html_to_text(raw_html)
            
            # Validate content isn't suspiciously short
            if not self._is_content_valid(clean_text) and attempt < retries - 1:
                logger.warning(f"Section {section_id}: Content too short ({len(clean_text)} chars) (attempt {attempt + 1}/{retries})")
                time.sleep(5 * (attempt + 1))
                continue
            
            # Success!
            return raw_html, clean_text
        
        # All retries failed
        logger.error(f"Section {section_id}: All {retries} attempts failed")
        return "", ""
    
    # ── Case Law Links ────────────────────────────────────────────────────────
    
    def get_section_case_links(self, case_type_id: str) -> List[str]:
        """Get case law citations for a section."""
        if not case_type_id:
            return []
        
        resp = self._request("POST", f"{BASE_URL}/Login/GetStatuteCaseLaw",
                            data={"caseTypeId": case_type_id, "subTopic": ""})
        
        if not resp or len(resp.text) < 50:
            return []
        
        # Extract citations using regex
        citations = extract_case_citations(resp.text)
        
        # Normalize and deduplicate
        normalized = []
        seen = set()
        for c in citations:
            norm = normalize_citation(c)
            if norm and norm not in seen:
                seen.add(norm)
                normalized.append(norm)
        
        return normalized
    
    # ── Full Statute Scraping ─────────────────────────────────────────────────
    
    def scrape_statute(self, statute_info: Dict) -> Optional[Statute]:
        """Scrape a complete statute with all sections and case links."""
        statute_name = statute_info["name"]
        alphabet = statute_info["alphabet"]
        
        if statute_name in self.progress["statutes_scraped"]:
            logger.debug(f"Skipping {statute_name} (already scraped)")
            return None
        
        logger.info(f"Scraping: {statute_name[:60]}")
        
        self._human_delay(1, 2)
        sections = self.get_statute_sections(statute_name)
        
        if not sections:
            logger.warning(f"  No sections found for {statute_name}")
            self.progress["statutes_scraped"].append(statute_name)
            return None
        
        # Create statute object
        statute = Statute(
            citation=statute_name,
            statute_id="",
            title=statute_name,
            alphabet=alphabet,
            source_url=f"{BASE_URL}/Login/GetStatuesSearch?caseName={statute_name}",
        )
        
        # Extract year from title
        year_match = re.search(r'(\d{4})$', statute_name)
        if year_match:
            statute.enactment_date = year_match.group(1)
            statute.short_title = statute_name[:statute_name.rfind(year_match.group(1))].strip()
        else:
            statute.short_title = statute_name
        
        # Determine jurisdiction
        jurisdiction_keywords = {
            "Federal": ["Federal", "Pakistan", "National"],
            "Punjab": ["Punjab"],
            "Sindh": ["Sindh", "Karachi"],
            "KPK": ["KPK", "Khyber", "Pakhtunkhwa", "NWFP"],
            "Balochistan": ["Balochistan", "Baluchistan"],
            "AJK": ["Azad Jammu", "Kashmir", "AJK"],
            "Gilgit-Baltistan": ["Gilgit", "Baltistan", "GBLR"],
        }
        for jurisdiction, keywords in jurisdiction_keywords.items():
            if any(kw.lower() in statute_name.lower() for kw in keywords):
                statute.jurisdiction = jurisdiction
                break
        
        all_case_links = []
        full_text_parts = []
        all_raw_html = []
        processed_sections = []
        failed_sections = []  # Track sections that couldn't be fetched
        
        # Scrape each section
        for i, section_info in enumerate(sections):
            if i > 0 and i % 5 == 0:
                self._human_delay(4, 8)
            else:
                self._human_delay(1.5, 3)
            
            section_id = section_info.get("section_id", "")
            case_type_id = section_info.get("case_type_id", "")
            section_num = section_info.get("number", "")
            
            # Get section content with retry logic
            raw_html, clean_text = "", ""
            if section_id:
                raw_html, clean_text = self.get_section_content(section_id)
                if raw_html:
                    all_raw_html.append(raw_html)
                    if clean_text:
                        self._human_delay(reading=True)
                else:
                    # Track failed section for later re-scrape
                    failed_sections.append({
                        "section_id": section_id,
                        "number": section_num,
                        "reason": "fetch_failed"
                    })
                    logger.warning(f"  Section {section_num}: Failed to fetch content")
            
            # Get case links and enrich them
            section_ref = f"Section {section_num}" if section_num else ""
            case_citations = []
            if case_type_id:
                self._human_delay(1, 2)
                case_citations = self.get_section_case_links(case_type_id)
            
            # Enrich case links with full details
            enriched_cases = enrich_case_links(case_citations, section_ref)
            all_case_links.extend(enriched_cases)
            
            section = Section(
                number=section_num,
                title=section_info.get("definition", ""),
                text=clean_text,
                text_raw=raw_html,
                cases_cited=enriched_cases,  # Now enriched dicts instead of strings
                section_id=section_id,
            )
            processed_sections.append(section)
            
            if clean_text:
                full_text_parts.append(f"[Section {section.number}]\n{clean_text}")
        
        # Combine all data
        statute.sections = processed_sections
        statute.full_text = "\n\n".join(full_text_parts)
        
        # Combined raw HTML
        combined_raw_html = "\n\n<!-- SECTION BREAK -->\n\n".join(all_raw_html)
        statute.full_text_raw = combined_raw_html
        
        # Extract preamble (both clean and raw)
        statute.preamble = extract_preamble([asdict(s) for s in processed_sections]) or ""
        # Find preamble raw HTML
        for s in processed_sections:
            if s.number.upper() == 'PREAMBLE' or 'preamble' in s.number.lower():
                statute.preamble_raw = s.text_raw
                break
        
        # Deduplicate enriched case citations
        seen_citations = set()
        unique_cases = []
        for case in all_case_links:
            if isinstance(case, dict):
                citation = case.get("citation", "")
                if citation and citation not in seen_citations:
                    seen_citations.add(citation)
                    unique_cases.append(case)
        statute.cases_cited = unique_cases
        
        # Mark as scraped
        self.progress["statutes_scraped"].append(statute_name)
        self.progress["total_statutes"] += 1
        
        # Track incomplete statutes (those with failed sections)
        if failed_sections:
            if "incomplete_statutes" not in self.progress:
                self.progress["incomplete_statutes"] = {}
            self.progress["incomplete_statutes"][statute_name] = {
                "failed_sections": failed_sections,
                "timestamp": datetime.now().isoformat()
            }
            logger.warning(f"  Statute has {len(failed_sections)} incomplete sections - marked for re-scrape")
        
        return statute, combined_raw_html
    
    # ── Alphabet Scraping ─────────────────────────────────────────────────────
    
    def scrape_alphabet(self, letter: str, limit: int = None) -> int:
        """Scrape all statutes for a given alphabet letter."""
        if letter in self.progress["completed_alphabets"]:
            logger.info(f"Skipping '{letter}' (already completed)")
            return 0
        
        logger.info(f"=== Scraping alphabet '{letter}' ===")
        self.progress["current_alphabet"] = letter
        self._save_progress()
        
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.start(task=f"Scraping statutes '{letter}'", alphabet=letter)
        
        statutes = self.get_statutes_by_letter(letter)
        if not statutes:
            logger.warning(f"No statutes found for '{letter}'")
            self._save_progress()
            return 0
        
        if limit:
            statutes = statutes[:limit]
        
        scraped_count = 0
        total = len(statutes)
        
        for i, statute_info in enumerate(statutes):
            try:
                if not self._is_pls_open():
                    self._wait_for_pls_open()
                    self.logged_in = False
                
                self._human_delay()
                result = self.scrape_statute(statute_info)
                
                if result:
                    statute, raw_html = result
                    self._save_statute(statute, raw_html)
                    scraped_count += 1
                
                self._save_progress()
                
                if (i + 1) % 10 == 0:
                    logger.info(f"  Progress: {i + 1}/{total} for '{letter}' ({scraped_count} with sections)")
                    if HAS_STATUS_REPORTER and _status_reporter:
                        _status_reporter.progress_update(i + 1, total, f"Scraped {scraped_count}")
                
            except KeyboardInterrupt:
                logger.info("Interrupted. Saving progress...")
                self._save_progress()
                raise
            except Exception as e:
                logger.error(f"Error scraping {statute_info['name']}: {e}")
                self._save_progress()
                self.logged_in = False
                time.sleep(RATE_LIMIT_BACKOFF)
        
        self.progress["completed_alphabets"].append(letter)
        self.progress["current_alphabet"] = None
        self._save_progress()
        
        logger.info(f"Completed '{letter}': {scraped_count} statutes scraped")
        
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.complete(success=True, message=f"{scraped_count} statutes")
        
        return scraped_count
    
    def scrape_all(self, start_letter: str = "A", limit_per_letter: int = None):
        """Scrape all alphabets starting from a given letter."""
        logger.info(f"Starting full scrape from '{start_letter}'")
        
        if not self.login():
            logger.error("Failed to login. Aborting.")
            return
        
        start_index = ALPHABETS.index(start_letter) if start_letter in ALPHABETS else 0
        total_scraped = 0
        
        for letter in ALPHABETS[start_index:]:
            try:
                if not self._is_pls_open():
                    self._wait_for_pls_open()
                    self.logged_in = False
                    if not self.login():
                        break
                
                count = self.scrape_alphabet(letter, limit=limit_per_letter)
                total_scraped += count
                
                if letter != ALPHABETS[-1]:
                    break_time = random.uniform(60, 120)
                    logger.info(f"Break between alphabets: {break_time:.0f}s")
                    time.sleep(break_time)
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user.")
                break
        
        logger.info(f"Scraping complete! Total statutes: {total_scraped}")
        self._save_progress()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="PLS Legislation Scraper v2.1")
    parser.add_argument("command", choices=["scrape", "test", "status", "resume"],
                        help="Command to run")
    parser.add_argument("--letter", "-l", help="Specific letter (A-Z)")
    parser.add_argument("--limit", "-n", type=int, help="Limit statutes per letter")
    parser.add_argument("--start", "-s", default="A", help="Start from letter")
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7)")
    
    args = parser.parse_args()
    
    scraper = LegislationScraperV2(ignore_hours=(not args.respect_hours))
    
    if args.command == "test":
        if scraper.login():
            print("[OK] Login successful!")
            scraper._human_delay()
            statutes = scraper.get_statutes_by_letter("A")
            print(f"[OK] Found {len(statutes)} statutes for 'A'")
            if statutes:
                print(f"  First: {statutes[0]['name'][:60]}")
                scraper._human_delay()
                result = scraper.scrape_statute(statutes[0])
                if result:
                    statute, raw_html = result
                    print(f"  Sections: {len(statute.sections)}")
                    print(f"  Cases cited: {len(statute.cases_cited)}")
                    print(f"  Preamble: {statute.preamble[:100]}..." if statute.preamble else "  No preamble")
                    
                    # Save test statute
                    scraper._save_statute(statute, raw_html)
                    print(f"  Saved to: {LEGISLATION_DIR / statute.alphabet}")
        else:
            print("[FAIL] Login failed!")
    
    elif args.command == "status":
        print(f"Progress file: {PROGRESS_FILE}")
        print(f"Data directory: {LEGISLATION_DIR}")
        print(f"Completed alphabets: {scraper.progress['completed_alphabets']}")
        print(f"Current alphabet: {scraper.progress['current_alphabet']}")
        print(f"Statutes scraped: {len(scraper.progress['statutes_scraped'])}")
        print(f"Total statutes: {scraper.progress['total_statutes']}")
    
    elif args.command == "scrape":
        if args.letter:
            scraper.login()
            scraper.scrape_alphabet(args.letter, limit=args.limit)
        else:
            scraper.scrape_all(start_letter=args.start, limit_per_letter=args.limit)
    
    elif args.command == "resume":
        current = scraper.progress.get("current_alphabet")
        if current:
            scraper.scrape_all(start_letter=current)
        else:
            for letter in ALPHABETS:
                if letter not in scraper.progress["completed_alphabets"]:
                    scraper.scrape_all(start_letter=letter)
                    break


if __name__ == "__main__":
    main()
