#!/usr/bin/env python3
"""
PLS Legislation Scraper v2.0
============================
Scrapes statutes/legislation from pakistanlawsite.com.
Uses curl_cffi for TLS fingerprint impersonation (Chrome 120).

Features:
- Chrome 120 TLS fingerprint
- Automatic session/cookie management
- Human-like delays with jitter + reading simulation
- Night shift hours (10 PM - 5 AM PKT) - runs while case scraper sleeps
- Random breaks to simulate human behavior
- Alphabetical navigation (A-Z)
- Section-by-section extraction
- Case law links extraction
- Resumable progress tracking
- Pipeline status reporting for orchestrator
- JSON output with metadata
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
DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
PROGRESS_FILE = DATA_DIR / "progress.json"
INDEX_FILE = DATA_DIR / "legislation_index.json"
LINKS_FILE = DATA_DIR / "statute_case_links.jsonl"
STATUTES_JSONL = DATA_DIR / "all_statutes.jsonl"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing (aggressive 24/7 - no restrictions per Abdul)
MIN_DELAY = 0.5  # Minimum seconds between requests
MAX_DELAY = 1.2  # Maximum seconds between requests
LOGIN_DELAY = 1.5  # Delay after login
RATE_LIMIT_BACKOFF = 20  # Seconds to wait if rate limited
READING_DELAY_MIN = 0.3  # Minimum "reading" delay for content
READING_DELAY_MAX = 0.8  # Maximum "reading" delay for content

# Break simulation (minimal breaks)
REQUESTS_BEFORE_BREAK = 150  # Take a break every N requests
BREAK_MIN = 3  # Minimum break seconds
BREAK_MAX = 8  # Maximum break seconds

# PLS Operating Hours - DISABLED (24/7 mode)
PLS_OPEN_HOUR = 0   # No restrictions
PLS_CLOSE_HOUR = 24  # No restrictions
PKT_OFFSET = timedelta(hours=5)
NIGHT_MODE = False  # Disabled - run anytime

# Alphabet list
ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Logging — force flush on every log line (critical for redirected output)
import sys
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S',
    stream=sys.stderr
)
# Force unbuffered stderr
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(line_buffering=True)
logger = logging.getLogger(__name__)
# Add flush handler to ensure logs are written immediately
for handler in logging.root.handlers:
    handler.flush = lambda h=handler: (h.stream.flush() if hasattr(h, 'stream') else None)


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Section:
    """Represents a section of a statute."""
    section_id: str
    number: str
    title: str = ""
    text: str = ""
    case_links: List[Dict] = field(default_factory=list)


@dataclass
class Statute:
    """Represents a complete statute."""
    id: str
    title: str
    short_title: str = ""
    alphabet: str = ""
    enactment_date: str = ""
    jurisdiction: str = ""
    status: str = "in_force"
    sections: List[Section] = field(default_factory=list)
    case_links: List[Dict] = field(default_factory=list)
    full_text: str = ""
    amendments: List[str] = field(default_factory=list)
    scraped_at: str = ""
    source_url: str = ""
    
    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now().isoformat()
        if not self.id:
            self.id = hashlib.md5(self.title.encode()).hexdigest()[:12]
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "title": self.title,
            "short_title": self.short_title,
            "alphabet": self.alphabet,
            "enactment_date": self.enactment_date,
            "jurisdiction": self.jurisdiction,
            "status": self.status,
            "sections": [asdict(s) if isinstance(s, Section) else s for s in self.sections],
            "case_links": self.case_links,
            "full_text": self.full_text,
            "amendments": self.amendments,
            "scraped_at": self.scraped_at,
            "source_url": self.source_url,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Scraper Class
# ══════════════════════════════════════════════════════════════════════════════

class LegislationScraper:
    """Pakistan Law Site Legislation Scraper using curl_cffi."""
    
    def __init__(self, ignore_hours: bool = True):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.progress = self._load_progress()
        self.ignore_hours = ignore_hours  # For testing only
        self.requests_since_break = 0
        
        # Create data directories
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        for letter in ALPHABETS:
            (DATA_DIR / letter).mkdir(exist_ok=True)
            (DATA_DIR / letter / "original").mkdir(exist_ok=True)
    
    def _is_pls_open(self) -> bool:
        """Check if within operating hours - DISABLED (24/7 mode)."""
        return True  # Always open - 24/7 aggressive scraping per Abdul
    
    def _wait_for_pls_open(self) -> None:
        """Wait until operating window opens (10 PM PKT for night shift)."""
        while not self._is_pls_open():
            utc_now = datetime.now(timezone.utc)
            pkt_now = utc_now + PKT_OFFSET
            
            # Night mode: wait until 10 PM PKT
            if NIGHT_MODE:
                if pkt_now.hour >= PLS_CLOSE_HOUR and pkt_now.hour < PLS_OPEN_HOUR:
                    # Between 5 AM and 10 PM - wait until 10 PM today
                    open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)
                else:
                    # Should not happen if _is_pls_open works correctly
                    open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)
            else:
                # Day mode (original logic)
                if pkt_now.hour >= PLS_CLOSE_HOUR:
                    tomorrow = pkt_now.date() + timedelta(days=1)
                    open_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, PLS_OPEN_HOUR, 0)
                else:
                    open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)
            
            wait_seconds = (open_time - pkt_now.replace(tzinfo=None)).total_seconds()
            wait_seconds = max(60, wait_seconds)  # At least 1 minute
            
            hours, remainder = divmod(int(wait_seconds), 3600)
            minutes = remainder // 60
            
            logger.info(f"Waiting {hours}h {minutes}m until window opens at {PLS_OPEN_HOUR}:00 PKT...")
            
            # Sleep in chunks to allow interruption
            chunk = min(300, wait_seconds)  # 5 minute chunks
            time.sleep(chunk)
    
    def _maybe_take_break(self) -> None:
        """Take a random break every N requests to simulate human behavior."""
        self.requests_since_break += 1
        
        if self.requests_since_break >= REQUESTS_BEFORE_BREAK:
            break_duration = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"Taking a {break_duration:.0f}s break (human simulation)...")
            time.sleep(break_duration)
            self.requests_since_break = 0
    
    def _create_session(self) -> Session:
        """Create a new curl_cffi session with Chrome 120 impersonation."""
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "X-Requested-With": "XMLHttpRequest",  # Required for AJAX endpoints
        })
        return session
    
    def _human_delay(self, min_s: float = None, max_s: float = None, reading: bool = False):
        """Wait a random human-like delay.
        
        Args:
            min_s: Minimum delay (default MIN_DELAY or READING_DELAY_MIN if reading)
            max_s: Maximum delay (default MAX_DELAY or READING_DELAY_MAX if reading)
            reading: If True, simulate reading content (longer delay)
        """
        if reading:
            min_s = min_s or READING_DELAY_MIN
            max_s = max_s or READING_DELAY_MAX
        else:
            min_s = min_s or MIN_DELAY
            max_s = max_s or MAX_DELAY
        
        delay = random.uniform(min_s, max_s)
        # Add Gaussian jitter for more natural timing
        delay += random.gauss(0, 0.2)
        delay = max(0.3, delay)  # At least 0.3 second
        
        logger.debug(f"Waiting {delay:.1f}s...")
        time.sleep(delay)
    
    def _request(self, method: str, url: str, retries: int = 3, **kwargs) -> Optional[Any]:
        """Make a request with rate limiting, retries, and error handling."""
        # Check PLS operating hours
        if not self._is_pls_open():
            self._wait_for_pls_open()
            # Re-login after waiting (session may have expired)
            self.logged_in = False
            if not self.login():
                return None
        
        # Maybe take a break (human simulation)
        self._maybe_take_break()
        
        # Enforce minimum delay between requests
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
                
                if resp.status_code == 403:
                    logger.warning(f"403 Forbidden - backing off {RATE_LIMIT_BACKOFF}s...")
                    time.sleep(RATE_LIMIT_BACKOFF)
                    continue  # Retry
                
                if resp.status_code == 429:
                    backoff = RATE_LIMIT_BACKOFF * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"429 Rate Limited - backing off {backoff}s (attempt {attempt + 1}/{retries})...")
                    time.sleep(backoff)
                    continue  # Retry
                
                if resp.status_code == 500:
                    logger.warning(f"500 Server Error for {url} (attempt {attempt + 1}/{retries})")
                    time.sleep(RATE_LIMIT_BACKOFF)
                    continue  # Retry
                
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
        PROGRESS_FILE.write_text(json.dumps(self.progress, indent=2, ensure_ascii=False), encoding='utf-8')
    
    def _save_statute(self, statute: Statute, html_content: str = ""):
        """Save statute to all 4 formats (JSON, Original HTML, Readable HTML, JSONL).
        
        ALWAYS saves all 4 formats regardless of content availability.
        Even statutes with no section text get metadata-only original/readable HTML.
        """
        safe_name = re.sub(r'[^\w\-]', '_', statute.title)[:100]
        statute_dir = DATA_DIR / statute.alphabet
        html_dir = DATA_DIR / "html" / statute.alphabet
        
        # Create directories
        statute_dir.mkdir(parents=True, exist_ok=True)
        (statute_dir / "original").mkdir(exist_ok=True)
        html_dir.mkdir(parents=True, exist_ok=True)
        
        # 1. Save JSON (structured data)
        json_path = statute_dir / f"{safe_name}.json"
        json_path.write_text(json.dumps(statute.to_dict(), indent=2, ensure_ascii=False), encoding='utf-8')
        
        # 2. Save original HTML (raw PLS content — ALWAYS save)
        original_path = statute_dir / "original" / f"{safe_name}.html"
        if html_content:
            original_path.write_text(html_content, encoding='utf-8')
        elif statute.full_text:
            original_path.write_text(statute.full_text, encoding='utf-8')
        else:
            # Build minimal original HTML from section data
            parts = [f"<h1>{statute.title}</h1>"]
            if statute.enactment_date:
                parts.append(f"<p><b>Enacted:</b> {statute.enactment_date}</p>")
            if statute.jurisdiction:
                parts.append(f"<p><b>Jurisdiction:</b> {statute.jurisdiction}</p>")
            for sec in statute.sections:
                sec_num = sec.get('number', '')
                sec_title = sec.get('title', '')
                sec_text = sec.get('text', '')
                parts.append(f"<h3>Section {sec_num}: {sec_title}</h3>")
                if sec_text:
                    parts.append(f"<p>{sec_text}</p>")
                else:
                    parts.append("<p><em>[Content not available on source]</em></p>")
            original_path.write_text("\n".join(parts), encoding='utf-8')
        
        # 3. Generate readable HTML (dark-theme styled — inline, no external dependency)
        readable_path = html_dir / f"{safe_name}.html"
        try:
            sections_html = ""
            for sec in statute.sections:
                sec_num = sec.get('number', '')
                sec_title = sec.get('title', '')
                sec_text = sec.get('text', '')
                case_links = sec.get('case_links', [])
                
                sections_html += f'<div class="section"><h3>Section {sec_num}'
                if sec_title:
                    sections_html += f': {sec_title}'
                sections_html += '</h3>'
                
                if sec_text:
                    sections_html += f'<div class="section-text">{sec_text}</div>'
                else:
                    sections_html += '<p class="unavailable"><em>[Content not available on source]</em></p>'
                
                if case_links:
                    sections_html += '<div class="case-links"><h4>Related Cases</h4><ul>'
                    for cl in case_links:
                        cit = cl.get('citation', cl.get('text', ''))
                        sections_html += f'<li>{cit}</li>'
                    sections_html += '</ul></div>'
                sections_html += '</div>'
            
            readable = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{statute.title}</title>
<style>
body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px;
       line-height: 1.8; color: #e0e0e0; background: #1a1a2e; }}
h1 {{ color: #64ffda; font-size: 1.5em; border-bottom: 2px solid #64ffda; padding-bottom: 10px; }}
h3 {{ color: #bb86fc; margin-top: 30px; }}
h4 {{ color: #03dac6; font-size: 0.9em; }}
.meta {{ color: #888; margin-bottom: 20px; padding: 15px; background: #16213e; border-radius: 8px; }}
.section {{ margin: 20px 0; padding: 15px; border-left: 3px solid #333; }}
.section:hover {{ border-left-color: #64ffda; }}
.section-text {{ white-space: pre-wrap; margin: 10px 0; }}
.unavailable {{ color: #666; font-style: italic; }}
.case-links {{ margin-top: 10px; padding: 10px; background: #0d1b2a; border-radius: 5px; }}
.case-links ul {{ margin: 5px 0; padding-left: 20px; }}
.case-links li {{ color: #03dac6; font-size: 0.9em; }}
</style>
</head>
<body>
<h1>{statute.title}</h1>
<div class="meta">
<strong>Jurisdiction:</strong> {statute.jurisdiction or 'N/A'}<br>
<strong>Enacted:</strong> {statute.enactment_date or 'N/A'}<br>
<strong>Sections:</strong> {len(statute.sections)}<br>
<strong>Status:</strong> {statute.status}
</div>
{sections_html}
</body>
</html>"""
            readable_path.write_text(readable, encoding='utf-8')
        except Exception as e:
            logger.warning(f"Could not generate readable HTML: {e}")
        
        # 4. Append to main JSONL file (all statutes)
        with open(STATUTES_JSONL, 'a', encoding='utf-8') as f:
            f.write(json.dumps(statute.to_dict(), ensure_ascii=False) + '\n')
        
        # Append to case links file
        if statute.case_links:
            with open(LINKS_FILE, 'a', encoding='utf-8') as f:
                for link in statute.case_links:
                    entry = {
                        "statute_id": statute.id,
                        "statute_title": statute.title,
                        **link
                    }
                    f.write(json.dumps(entry, ensure_ascii=False) + '\n')
        
        logger.info(f"Saved (4 formats): {statute.title[:60]}")
    
    # ── Login ─────────────────────────────────────────────────────────────────
    
    def login(self) -> bool:
        """Login to PLS with ClearLoginHistory flow (post-Feb 2026 redesign).
        
        The new PLS site uses ClearLoginHistory to clear old sessions AND log in.
        Falls back to Login/Login if needed.
        """
        # Check operating hours before login
        if not self._is_pls_open():
            self._wait_for_pls_open()
        
        logger.info("Logging in to PLS...")
        
        for attempt in range(3):
            self.session = self._create_session()
            
            # Get homepage for CSRF token
            try:
                resp = self.session.get(f"{BASE_URL}/", timeout=30)
            except Exception as e:
                logger.warning(f"Homepage fetch failed (attempt {attempt+1}): {e}")
                self._human_delay(5, 10)
                continue
                
            if not resp or resp.status_code != 200:
                logger.error(f"Failed to load homepage (status {resp.status_code if resp else 'None'})")
                self._human_delay(5, 10)
                continue
            
            # Simulate reading the homepage
            self._human_delay(reading=True)
            
            csrf_match = re.search(
                r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                resp.text
            )
            if not csrf_match:
                logger.error("CSRF token not found")
                self._human_delay(5, 10)
                continue
            
            csrf_token = csrf_match.group(1)
            logger.debug(f"CSRF token: {csrf_token[:40]}...")
            
            self._human_delay(2, 4)
            
            # Step 1: ClearLoginHistory — clears old sessions AND logs us in
            logger.info("  Clearing login history (also logs in)...")
            try:
                clear_resp = self.session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
                    "Login.UserName": PLS_USER,
                    "Login.Password": PLS_PASS,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)
            except Exception as e:
                logger.warning(f"ClearLoginHistory failed: {e}")
                self._human_delay(5, 10)
                continue
            
            self._human_delay(2, 3)
            
            # Step 2: Check if ClearLoginHistory logged us in
            try:
                check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check_resp and check_resp.status_code == 200 and "Logout" in check_resp.text:
                    self.logged_in = True
                    self.requests_since_break = 0
                    logger.info("[OK] Login successful (via ClearLoginHistory)")
                    self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
                    return True
            except Exception:
                pass
            
            # Step 3: Fallback — try explicit Login/Login
            logger.info("  ClearLoginHistory didn't log in, trying Login/Login...")
            
            # Get fresh CSRF
            try:
                resp2 = self.session.get(f"{BASE_URL}/", timeout=30)
                csrf_match2 = re.search(
                    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                    resp2.text
                )
                csrf2 = csrf_match2.group(1) if csrf_match2 else csrf_token
            except Exception:
                csrf2 = csrf_token
            
            try:
                login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
                    "Login.UserName": PLS_USER,
                    "Login.Password": PLS_PASS,
                    "__RequestVerificationToken": csrf2
                }, timeout=30)
            except Exception as e:
                logger.warning(f"Login/Login failed: {e}")
                self._human_delay(5, 10)
                continue
            
            if not login_resp or login_resp.status_code != 200:
                logger.error(f"Login request failed (attempt {attempt+1})")
                self._human_delay(5, 10)
                continue
            
            # Handle "Account Already In Use"
            if "Account Already In Use" in login_resp.text:
                logger.warning("Account still in use, waiting...")
                self._human_delay(10, 15)
                continue
            
            self._human_delay(2, 3)
            
            # Verify login
            try:
                check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check_resp and check_resp.status_code == 200 and ("Logout" in check_resp.text or "pakistanlaws" in check_resp.text.lower()):
                    self.logged_in = True
                    self.requests_since_break = 0
                    logger.info("[OK] Login successful (via Login/Login)")
                    self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
                    return True
            except Exception:
                pass
            
            logger.error(f"Login verification failed (attempt {attempt+1})")
            self._human_delay(5, 10)
        
        logger.error("Login failed after 3 attempts")
        return False
    
    # ── Session Health Check ─────────────────────────────────────────────────
    
    def check_session(self) -> bool:
        """Verify the PLS session is still alive. Returns True if logged in."""
        try:
            resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=15)
            if resp and resp.status_code == 200:
                text = resp.text.lower()
                if "logout" in text or "pakistanlaws" in text:
                    return True
            self.logged_in = False
            return False
        except Exception:
            self.logged_in = False
            return False
    
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
        
        # Simulate reading/scrolling through the list
        self._human_delay(reading=True)
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        statutes = []
        rows = soup.find_all('tr', class_='caseType')
        
        for row in rows:
            caseid = row.get('casetypeid', '')
            if caseid:
                statutes.append({
                    "name": caseid.strip(),
                    "alphabet": letter
                })
        
        # BULLETPROOF: If no statutes found, verify session is alive
        if not statutes:
            logger.warning(f"  Zero statutes for '{letter}' — verifying session health...")
            if not self.check_session():
                logger.error(f"  SESSION DEAD! Re-logging in...")
                if self.login():
                    # Retry with fresh session
                    resp = self._request("GET", f"{BASE_URL}/Login/StatuecharSearch",
                                        params={"character": letter})
                    if resp:
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        rows = soup.find_all('tr', class_='caseType')
                        for row in rows:
                            caseid = row.get('casetypeid', '')
                            if caseid:
                                statutes.append({"name": caseid.strip(), "alphabet": letter})
                        logger.info(f"  After re-login: Found {len(statutes)} statutes for '{letter}'")
            else:
                logger.info(f"  Session alive — '{letter}' genuinely has 0 statutes")
        
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
        rows = soup.find_all('tr', class_='table_row_hover')
        
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 4:
                read_cell = cells[0]
                section_num = cells[1].get_text(strip=True)
                act_name = cells[2].get_text(strip=True)
                definition = cells[3].get_text(strip=True)
                
                # Get the casetypeid for fetching content
                section_id = read_cell.get('casetypeid', '')
                if not section_id:
                    link = read_cell.find(class_='readCaseLaw')
                    if link:
                        section_id = link.get('casetypeid', '')
                
                # Get the case law cell info
                case_cell = cells[4] if len(cells) > 4 else None
                case_id = case_cell.get('casetypeid', '') if case_cell else ''
                statute_section = case_cell.get('statutename', '') if case_cell else ''
                
                sections.append({
                    "section_id": section_id,
                    "number": section_num,
                    "act_name": act_name,
                    "definition": definition,
                    "case_type_id": case_id,
                    "statute_section": statute_section,
                })
        
        return sections
    
    # ── Section Content ───────────────────────────────────────────────────────
    
    def get_section_content(self, section_id: str, max_retries: int = 3) -> tuple:
        """Get the full text content of a section. Returns (raw_html, clean_text).
        
        Retries up to max_retries times if PLS returns '-1' (transient error).
        Uses exponential backoff between retries.
        """
        if not section_id:
            return "", ""
        
        for attempt in range(max_retries):
            resp = self._request("POST", f"{BASE_URL}/Login/SearchStatueFile",
                                data={"caseTypeId": section_id})
            
            # Network failure
            if not resp:
                if attempt < max_retries - 1:
                    backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                    logger.warning(f"  Section {section_id}: no response, retry in {backoff}s (attempt {attempt+1})")
                    time.sleep(backoff)
                    continue
                return "", ""
            
            text = resp.text.strip()
            
            # Check for -1 / empty responses (PLS transient error)
            is_neg1 = text in ["-1", '"-1"', '"-1', '-1"', ""] or len(text) < 10
            
            if is_neg1:
                # Don't retry — proven across 5,000+ sections that '-1' = permanently unavailable on PLS
                logger.debug(f"  Section {section_id}: unavailable on PLS (-1)")
                return "", ""
            
            # Got real content — PLS may return HTML wrapped in quotes
            raw_html = resp.text
            if raw_html.startswith('"'):
                # Try proper JSON decode first
                try:
                    raw_html = json.loads(raw_html)
                except (json.JSONDecodeError, ValueError):
                    # Fallback: strip outer quotes and unescape sequences
                    stripped = raw_html.strip()
                    if stripped.endswith('"'):
                        raw_html = stripped[1:-1].replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
            
            # Parse HTML for clean text
            soup = BeautifulSoup(raw_html, 'html.parser')
            clean_text = soup.get_text(separator='\n', strip=True)
            
            # If clean text is too short, it's probably an error
            if len(clean_text) < 10:
                if attempt < max_retries - 1:
                    logger.warning(f"  Section {section_id}: clean text too short ({len(clean_text)} chars), retrying...")
                    time.sleep(10)
                    continue
                return "", ""
            
            return raw_html, clean_text
        
        return "", ""
    
    # ── Case Law Links ────────────────────────────────────────────────────────
    
    def get_section_case_links(self, case_type_id: str) -> List[Dict]:
        """Get case law citations for a section."""
        if not case_type_id:
            return []
        
        resp = self._request("POST", f"{BASE_URL}/Login/GetStatuteCaseLaw",
                            data={"caseTypeId": case_type_id, "subTopic": ""})
        
        if not resp or len(resp.text) < 50:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        case_links = []
        # Look for case citations (PLD, SCMR, etc.)
        for link in soup.find_all('a'):
            text = link.get_text(strip=True)
            href = link.get('href', '')
            
            # Match citation pattern
            citation_match = re.search(
                r'(\d{4})\s+(PLD|SCMR|CLC|PCrLJ|MLD|YLR|PTD|PLC|CLD|GBLR)\s+(\d+)',
                text
            )
            if citation_match:
                case_links.append({
                    "citation": text,
                    "year": citation_match.group(1),
                    "reporter": citation_match.group(2),
                    "page": citation_match.group(3),
                    "url": href,
                })
        
        # Also look for table rows with citations
        for row in soup.find_all('tr'):
            text = row.get_text(strip=True)
            citation_match = re.search(
                r'(\d{4})\s+(PLD|SCMR|CLC|PCrLJ|MLD|YLR|PTD|PLC|CLD|GBLR)\s+(\d+)',
                text
            )
            if citation_match:
                case_links.append({
                    "citation": citation_match.group(0),
                    "year": citation_match.group(1),
                    "reporter": citation_match.group(2),
                    "page": citation_match.group(3),
                    "url": "",
                })
        
        # Deduplicate
        seen = set()
        unique = []
        for cl in case_links:
            key = cl["citation"]
            if key not in seen:
                seen.add(key)
                unique.append(cl)
        
        return unique
    
    # ── Full Statute Scraping ─────────────────────────────────────────────────
    
    def _statute_file_path(self, statute_name: str, alphabet: str) -> Path:
        """Get the expected JSON file path for a statute."""
        safe_name = re.sub(r'[^\w\-]', '_', statute_name)[:100]
        return DATA_DIR / alphabet / f"{safe_name}.json"
    
    def scrape_statute(self, statute_info: Dict) -> Optional[Statute]:
        """Scrape a complete statute with all sections and case links."""
        statute_name = statute_info["name"]
        alphabet = statute_info["alphabet"]
        
        # Check if file ACTUALLY EXISTS on disk (not just in progress list)
        json_path = self._statute_file_path(statute_name, alphabet)
        if json_path.exists():
            logger.debug(f"Skipping {statute_name} (file exists on disk)")
            # Ensure it's in statutes_scraped for consistency
            if statute_name not in self.progress["statutes_scraped"]:
                self.progress["statutes_scraped"].append(statute_name)
            return None
        
        logger.info(f"Scraping: {statute_name[:60]}")
        
        # Get sections
        self._human_delay(1, 2)
        sections = self.get_statute_sections(statute_name)
        
        if not sections:
            logger.warning(f"  No sections found for {statute_name}")
            # Still mark as attempted so we don't retry
            self.progress["statutes_scraped"].append(statute_name)
            return None
        
        statute = Statute(
            id="",
            title=statute_name,
            alphabet=alphabet,
            source_url=f"{BASE_URL}/Login/GetStatuesSearch?caseName={statute_name}",
        )
        
        # Extract metadata from title
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
        full_html_parts = []
        
        # Scrape each section (with rate limiting and reading simulation)
        for i, section_info in enumerate(sections):
            # Session health check every 20 sections
            if i > 0 and i % 20 == 0:
                if not self.check_session():
                    logger.warning(f"  Session died during {statute_name}, re-logging in...")
                    if not self.login():
                        logger.error(f"  Re-login failed! Saving partial data for {statute_name}")
                        break
            
            # Brief pause between sections
            if i > 0 and i % 10 == 0:
                self._human_delay(2, 4)  # Slightly longer every 10 sections
            else:
                self._human_delay(0.5, 1.2)  # Quick delay between sections
            
            section_id = section_info.get("section_id", "")
            case_type_id = section_info.get("case_type_id", "")
            
            # Get section content (raw HTML + clean text)
            section_text = ""
            section_html = ""
            if section_id:
                section_html, section_text = self.get_section_content(section_id)
                # Simulate reading the section content
                if section_text:
                    self._human_delay(reading=True)
            
            # Get case links
            case_links = []
            if case_type_id:
                self._human_delay(0.3, 0.8)  # Quick delay before fetching case links
                case_links = self.get_section_case_links(case_type_id)
                all_case_links.extend([
                    {**cl, "section": section_info.get("number", "")}
                    for cl in case_links
                ])
            
            section = Section(
                section_id=section_id,
                number=section_info.get("number", ""),
                title=section_info.get("definition", ""),
                text=section_text,
                case_links=case_links,
            )
            statute.sections.append(asdict(section))
            
            if section_text:
                full_text_parts.append(f"[Section {section.number}]\n{section_text}")
            if section_html:
                full_html_parts.append(f"<!-- Section {section.number} -->\n{section_html}")
        
        statute.full_text = "\n\n".join(full_html_parts)  # Store raw HTML in full_text
        statute.case_links = all_case_links
        
        # Mark as scraped
        self.progress["statutes_scraped"].append(statute_name)
        self.progress["total_statutes"] += 1
        
        return statute
    
    # ── Alphabet Scraping ─────────────────────────────────────────────────────
    
    def scrape_alphabet(self, letter: str, limit: int = None) -> int:
        """Scrape all statutes for a given alphabet letter."""
        if letter in self.progress["completed_alphabets"]:
            logger.info(f"Skipping '{letter}' (already completed)")
            return 0
        
        logger.info(f"=== Scraping alphabet '{letter}' ===")
        self.progress["current_alphabet"] = letter
        self._save_progress()
        
        # Report status to orchestrator
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.start(task=f"Scraping statutes '{letter}'", alphabet=letter)
        
        # Get statute list
        statutes = self.get_statutes_by_letter(letter)
        if not statutes:
            # Verify this isn't a dead session before giving up
            if not self.check_session():
                logger.error(f"Session dead when listing '{letter}' — re-logging in and retrying...")
                if self.login():
                    statutes = self.get_statutes_by_letter(letter)
            
            if not statutes:
                logger.warning(f"No statutes found for '{letter}' after session check — NOT marking as complete")
                self._save_progress()
                if HAS_STATUS_REPORTER and _status_reporter:
                    _status_reporter.complete(success=True, message="No statutes found")
                return 0
        
        if limit:
            statutes = statutes[:limit]
        
        scraped_count = 0
        total = len(statutes)
        
        for i, statute_info in enumerate(statutes):
            try:
                # Check operating hours before each statute
                if not self._is_pls_open():
                    self._wait_for_pls_open()
                    self.logged_in = False  # Re-login after waiting
                
                # Session health check every 25 statutes
                if i > 0 and i % 25 == 0:
                    if not self.check_session():
                        logger.warning(f"Session died at statute {i}/{total} — re-logging in...")
                        if not self.login():
                            logger.error("Re-login failed! Saving progress and stopping.")
                            self._save_progress()
                            return scraped_count
                
                self._human_delay()
                statute = self.scrape_statute(statute_info)
                
                if statute:
                    self._save_statute(statute)
                    scraped_count += 1
                
                # Save progress after every attempt (for reliable resume)
                self._save_progress()
                
                # Log progress every 10 statutes
                if (i + 1) % 10 == 0:
                    logger.info(f"  Progress: {i + 1}/{total} for '{letter}' ({scraped_count} with sections)")
                    # Update orchestrator status
                    if HAS_STATUS_REPORTER and _status_reporter:
                        _status_reporter.progress_update(i + 1, total, f"Scraped {scraped_count} statutes")
                
            except KeyboardInterrupt:
                logger.info("Interrupted by user. Saving progress...")
                self._save_progress()
                if HAS_STATUS_REPORTER and _status_reporter:
                    _status_reporter.complete(success=False, message="Interrupted by user")
                raise
            except Exception as e:
                logger.error(f"Error scraping {statute_info['name']}: {e}")
                self._save_progress()
                # Re-login and continue
                self.logged_in = False
                time.sleep(RATE_LIMIT_BACKOFF)
        
        # Mark alphabet as complete
        self.progress["completed_alphabets"].append(letter)
        self.progress["current_alphabet"] = None
        self._save_progress()
        
        logger.info(f"Completed '{letter}': {scraped_count} statutes scraped")
        
        # Report completion to orchestrator
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.complete(success=True, message=f"{scraped_count} statutes scraped")
        
        return scraped_count
    
    # ── Main Entry Points ─────────────────────────────────────────────────────
    
    def scrape_all(self, start_letter: str = "A", limit_per_letter: int = None):
        """Scrape all alphabets starting from a given letter.
        
        Features:
        - Respects PLS operating hours (7 AM - 9 PM PKT)
        - Takes random breaks every 30 requests (human simulation)
        - Uses reading delays when viewing content
        - Retries failed requests with exponential backoff
        - Resumes from where it left off if interrupted
        """
        logger.info(f"Starting full scrape from '{start_letter}'")
        logger.info(f"PLS hours: {PLS_OPEN_HOUR}:00 - {PLS_CLOSE_HOUR}:00 PKT")
        
        if not self.login():
            logger.error("Failed to login. Aborting.")
            return
        
        start_index = ALPHABETS.index(start_letter) if start_letter in ALPHABETS else 0
        total_scraped = 0
        
        for letter in ALPHABETS[start_index:]:
            try:
                # Check operating hours at the start of each alphabet
                if not self._is_pls_open():
                    self._wait_for_pls_open()
                    self.logged_in = False  # Session may have expired
                    if not self.login():
                        logger.error("Failed to re-login after waiting. Aborting.")
                        break
                
                count = self.scrape_alphabet(letter, limit=limit_per_letter)
                total_scraped += count
                
                # Auto-verify after completing each alphabet
                try:
                    logger.info(f"{'═' * 60}")
                    logger.info(f"  ALPHABET '{letter}' COMPLETE — Running verifier...")
                    logger.info(f"{'═' * 60}")
                    import subprocess
                    verify_cmd = [
                        "python", "-u", str(Path(__file__).parent / "verify_legislation.py"),
                        "--letter", letter
                    ]
                    verify_env = os.environ.copy()
                    verify_env["PYTHONIOENCODING"] = "utf-8"
                    verify_env["PYTHONUNBUFFERED"] = "1"
                    result = subprocess.run(
                        verify_cmd,
                        cwd=str(Path(__file__).parent),
                        capture_output=True, text=True, timeout=600,
                        env=verify_env, encoding='utf-8', errors='replace'
                    )
                    if result.stdout:
                        for line in result.stdout.strip().split('\n')[-10:]:
                            logger.info(f"  [VERIFY] {line}")
                    if result.returncode != 0 and result.stderr:
                        for line in result.stderr.strip().split('\n')[-5:]:
                            logger.warning(f"  [VERIFY ERR] {line}")
                    logger.info(f"  Verification for '{letter}' complete (exit code: {result.returncode})")
                except Exception as e:
                    logger.warning(f"  Verifier failed for '{letter}': {e}")
                
                # Brief break between alphabets
                if letter != ALPHABETS[-1]:
                    break_time = random.uniform(10, 25)  # 10-25 second break
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
    
    parser = argparse.ArgumentParser(description="PLS Legislation Scraper v2.0")
    parser.add_argument("command", choices=["scrape", "test", "status", "resume"],
                        help="Command to run")
    parser.add_argument("--letter", "-l", help="Specific letter (A-Z)")
    parser.add_argument("--limit", "-n", type=int, help="Limit statutes per letter")
    parser.add_argument("--start", "-s", default="A", help="Start from letter")
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7)")
    
    args = parser.parse_args()
    
    scraper = LegislationScraper(ignore_hours=not args.respect_hours)
    
    if args.command == "test":
        # Quick test
        if scraper.login():
            print("[OK] Login successful!")
            scraper._human_delay()
            statutes = scraper.get_statutes_by_letter("A")
            print(f"[OK] Found {len(statutes)} statutes for 'A'")
            if statutes:
                print(f"  First: {statutes[0]['name'][:60]}")
                # Test scraping one statute
                scraper._human_delay()
                statute = scraper.scrape_statute(statutes[0])
                if statute:
                    print(f"  Sections: {len(statute.sections)}")
                    print(f"  Case links: {len(statute.case_links)}")
        else:
            print("[FAIL] Login failed!")
    
    elif args.command == "status":
        print(f"Progress file: {PROGRESS_FILE}")
        print(f"Data directory: {DATA_DIR}")
        print(f"Completed alphabets: {scraper.progress['completed_alphabets']}")
        print(f"Current alphabet: {scraper.progress['current_alphabet']}")
        print(f"Statutes scraped: {len(scraper.progress['statutes_scraped'])}")
        print(f"Total statutes: {scraper.progress['total_statutes']}")
        print(f"Last updated: {scraper.progress.get('last_updated', 'Never')}")
    
    elif args.command == "scrape":
        if args.letter:
            scraper.login()
            scraper.scrape_alphabet(args.letter, limit=args.limit)
        else:
            scraper.scrape_all(start_letter=args.start, limit_per_letter=args.limit)
    
    elif args.command == "resume":
        # Resume from current alphabet or start
        current = scraper.progress.get("current_alphabet")
        if current:
            scraper.scrape_all(start_letter=current)
        else:
            # Find first incomplete alphabet
            for letter in ALPHABETS:
                if letter not in scraper.progress["completed_alphabets"]:
                    scraper.scrape_all(start_letter=letter)
                    break


if __name__ == "__main__":
    main()
