#!/usr/bin/env python3
"""
PLS Scraper v2.0 - curl_cffi Edition
=====================================
Uses curl_cffi for TLS fingerprint impersonation (Chrome 120).
Undetectable browser-like requests.

Features:
- Chrome 120 TLS fingerprint
- Automatic session/cookie management
- Human-like delays with jitter
- Rate limit detection & backoff
- Resumable progress tracking
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
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Pipeline status reporting (optional)
try:
    from pipeline_status import PipelineStatusReporter, ScriptType
    _status_reporter = PipelineStatusReporter(ScriptType.SCRAPER, "pls_scraper_v2")
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
PROGRESS_FILE = DATA_DIR / "progress.json"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Bright Data Proxy (Pakistan residential IPs with rotation)
BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY", "9df9e22c-78b8-416b-a12a-c54616392b85")
# Residential proxy format: http://user:pass@host:port
# Using Web Unlocker zone for anti-detection
USE_PROXY = False  # Disabled - Bright Data needs KYC for POST requests

# Timing (human-like)
MIN_DELAY = 1.5  # Minimum seconds between requests (aggressive mode)
MAX_DELAY = 3.0  # Maximum seconds between requests (aggressive mode)
LOGIN_DELAY = 5.0  # Delay after login
RATE_LIMIT_BACKOFF = 60  # Seconds to wait if rate limited
READING_DELAY_MIN = 2.0  # Minimum "reading" delay for content
READING_DELAY_MAX = 6.0  # Maximum "reading" delay for content

# Break simulation (take breaks like a human would)
REQUESTS_BEFORE_BREAK = 100  # Take a break every N requests (aggressive mode)
BREAK_MIN = 30  # Minimum break seconds
BREAK_MAX = 90  # Maximum break seconds

# PLS Operating Hours - DISABLED (24/7 mode)
PLS_OPEN_HOUR = 0   # No restrictions
PLS_CLOSE_HOUR = 24  # No restrictions
PKT_OFFSET = timedelta(hours=5)

# Reporters to scrape
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
START_YEAR = 1947
END_YEAR = 2026

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
class Case:
    citation: str
    case_name: str
    title: str = ""
    court: str = ""
    date: str = ""
    judges: List[str] = None
    headnotes: str = ""
    judgment: str = ""
    judgment_raw: str = ""  # Original HTML for authenticity
    statutes_cited: List[str] = None
    cases_cited: List[str] = None
    fetched_at: str = ""

    def __post_init__(self):
        if self.judges is None:
            self.judges = []
        if self.statutes_cited is None:
            self.statutes_cited = []
        if self.cases_cited is None:
            self.cases_cited = []
        if not self.fetched_at:
            self.fetched_at = datetime.now().isoformat()

# ══════════════════════════════════════════════════════════════════════════════
# Scraper Class
# ══════════════════════════════════════════════════════════════════════════════

class PLSScraperV2:
    """Pakistan Law Site Scraper using curl_cffi + Bright Data Pakistan proxies."""

    def __init__(self, use_proxy: bool = USE_PROXY, ignore_hours: bool = True):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.progress = self._load_progress()
        self.use_proxy = use_proxy
        self.ignore_hours = ignore_hours  # For testing only
        self.requests_since_break = 0

        # Bright Data proxy configuration (Pakistan residential IPs)
        self.proxy_url = None
        if self.use_proxy:
            bd_username = os.getenv("BRIGHTDATA_USERNAME", "")
            bd_password = os.getenv("BRIGHTDATA_PASSWORD", "")
            bd_host = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")
            bd_port = os.getenv("BRIGHTDATA_PORT", "33335")

            if bd_username and bd_password:
                # Add country-pk for Pakistan IP rotation
                self.proxy_url = f"http://{bd_username}-country-pk:{bd_password}@{bd_host}:{bd_port}"
                logger.info(f"Using Bright Data Pakistan proxy ({bd_host}:{bd_port})")
            else:
                logger.warning("No proxy credentials found, running direct")
                self.use_proxy = False
        else:
            logger.info("Running without proxy (direct connection)")

        # JSONL duplicate-check sets (loaded lazily)
        self._jsonl_citation_sets: Dict[str, set] = {}  # keyed by JSONL filename
        self._master_citations: set = set()
        self._master_citations_loaded = False

        # Create data directory
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _create_session(self) -> Session:
        """Create a new curl_cffi session with Chrome 120 impersonation + proxy."""
        session = Session(impersonate=BrowserType.chrome120)

        # Set proxy if configured
        if self.proxy_url:
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url,
            }
            # Disable SSL verification for Bright Data proxy (uses self-signed cert)
            session.verify = False

        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
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
        })
        return session

    def _is_pls_open(self) -> bool:
        """Check if PLS is within operating hours (7 AM - 9 PM PKT)."""
        if self.ignore_hours:
            return True

        utc_now = datetime.now(timezone.utc)
        pkt_now = utc_now + PKT_OFFSET
        current_hour = pkt_now.hour

        is_open = PLS_OPEN_HOUR <= current_hour < PLS_CLOSE_HOUR

        if not is_open:
            logger.info(f"PLS closed (PKT: {pkt_now.strftime('%H:%M')}). Open: {PLS_OPEN_HOUR}:00-{PLS_CLOSE_HOUR}:00 PKT")

        return is_open

    def _wait_for_pls_open(self) -> None:
        """Wait until PLS opens (7 AM PKT)."""
        while not self._is_pls_open():
            utc_now = datetime.now(timezone.utc)
            pkt_now = utc_now + PKT_OFFSET

            # Calculate time until 7 AM PKT
            if pkt_now.hour >= PLS_CLOSE_HOUR:
                # After 9 PM, wait until tomorrow 7 AM
                tomorrow = pkt_now.date() + timedelta(days=1)
                open_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, PLS_OPEN_HOUR, 0)
            else:
                # Before 7 AM, wait until today 7 AM
                open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)

            wait_seconds = (open_time - pkt_now.replace(tzinfo=None)).total_seconds()
            wait_seconds = max(60, wait_seconds)  # At least 1 minute

            hours, remainder = divmod(int(wait_seconds), 3600)
            minutes = remainder // 60

            logger.info(f"Waiting {hours}h {minutes}m until PLS opens at {PLS_OPEN_HOUR}:00 PKT...")

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
        delay += random.gauss(0, 0.5)
        delay = max(1.0, delay)  # At least 1 second

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

                # Check for rate limiting
                if resp.status_code == 403:
                    backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                    logger.warning(f"403 Forbidden - backing off {backoff}s (attempt {attempt + 1}/{retries})...")
                    time.sleep(backoff)
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
            "completed_searches": [],  # List of "YEAR-REPORTER" strings
            "cases_fetched": [],  # List of case citations
            "total_cases": 0,
            "last_updated": None
        }

    def _save_progress(self):
        """Save progress to file."""
        self.progress["last_updated"] = datetime.now().isoformat()
        PROGRESS_FILE.write_text(json.dumps(self.progress, indent=2, ensure_ascii=False), encoding='utf-8')

    def _load_jsonl_citations(self, jsonl_path: Path) -> set:
        """Load all citations from a JSONL file into a set."""
        citations = set()
        if jsonl_path.exists():
            try:
                with open(jsonl_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                            if "citation" in obj:
                                citations.add(obj["citation"])
                        except json.JSONDecodeError:
                            # Fallback: regex extract citation from line
                            m = re.search(r'"citation":\s*"([^"]+)"', line)
                            if m:
                                citations.add(m.group(1))
            except Exception as e:
                logger.warning(f"Could not load citations from {jsonl_path}: {e}")
        return citations

    def _get_jsonl_set(self, jsonl_path: Path) -> set:
        """Get or lazily load the citation set for a reporter JSONL file."""
        key = str(jsonl_path)
        if key not in self._jsonl_citation_sets:
            self._jsonl_citation_sets[key] = self._load_jsonl_citations(jsonl_path)
            logger.info(f"Loaded {len(self._jsonl_citation_sets[key])} existing citations from {jsonl_path.name}")
        return self._jsonl_citation_sets[key]

    def _get_master_set(self) -> set:
        """Get or lazily load the master JSONL citation set."""
        if not self._master_citations_loaded:
            master_jsonl = DATA_DIR / "all_cases.jsonl"
            self._master_citations = self._load_jsonl_citations(master_jsonl)
            self._master_citations_loaded = True
            logger.info(f"Loaded {len(self._master_citations)} existing citations from all_cases.jsonl")
        return self._master_citations

    def _save_case(self, case: Case):
        """Save case to JSON file, JSONL, and original HTML."""
        # Create directory structure: data_v2/REPORTER/YEAR/
        reporter = case.citation.split()[1] if len(case.citation.split()) > 1 else "UNKNOWN"
        year = case.citation.split()[0] if case.citation else "0000"

        case_dir = DATA_DIR / reporter / year
        case_dir.mkdir(parents=True, exist_ok=True)

        case_dict = asdict(case)
        safe_citation = re.sub(r'[^\w\-]', '_', case.citation)

        # 1. Save individual JSON file
        json_filepath = case_dir / f"{safe_citation}.json"
        json_filepath.write_text(json.dumps(case_dict, indent=2, ensure_ascii=False), encoding='utf-8')

        # 2. Save original HTML file (preserves authenticity)
        original_dir = case_dir / "original"
        original_dir.mkdir(parents=True, exist_ok=True)
        html_filepath = original_dir / f"{safe_citation}.html"

        # Decode the JSON-encoded HTML and save as original
        original_html = case.judgment
        if original_html:
            try:
                # If it's JSON-escaped, decode it
                if original_html.startswith('"') or '\\u' in original_html:
                    import html as html_lib
                    original_html = original_html.encode().decode('unicode_escape')
                    original_html = html_lib.unescape(original_html)
                html_filepath.write_text(original_html, encoding='utf-8')
            except Exception as e:
                logger.warning(f"Could not save original HTML for {case.citation}: {e}")

        # 3. Append to JSONL file (one line per case) - SET-BASED DUPLICATE CHECK
        jsonl_filepath = DATA_DIR / f"{reporter}_{year}.jsonl"
        jsonl_set = self._get_jsonl_set(jsonl_filepath)

        if case.citation not in jsonl_set:
            with open(jsonl_filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(case_dict, ensure_ascii=False) + "\n")
            jsonl_set.add(case.citation)

        # 4. Append to master JSONL (all cases) - SET-BASED DUPLICATE CHECK
        master_set = self._get_master_set()

        if case.citation not in master_set:
            master_jsonl = DATA_DIR / "all_cases.jsonl"
            with open(master_jsonl, "a", encoding="utf-8") as f:
                f.write(json.dumps(case_dict, ensure_ascii=False) + "\n")
            master_set.add(case.citation)

        # 5. Generate readable HTML
        self._save_readable_html(case, reporter, year, safe_citation)

        logger.info(f"Saved: {case.citation}")

    def _save_readable_html(self, case: Case, reporter: str, year: str, safe_citation: str):
        """Generate readable HTML for case viewing."""
        try:
            readable_dir = DATA_DIR / "html" / reporter / year
            readable_dir.mkdir(parents=True, exist_ok=True)
            readable_path = readable_dir / f"{safe_citation}.html"

            # Get judgment text (decode if needed)
            judgment = case.judgment or ""
            if judgment.startswith('"') or '\\u' in judgment:
                try:
                    import html as html_lib
                    judgment = judgment.encode().decode('unicode_escape')
                    judgment = html_lib.unescape(judgment)
                except:
                    pass

            # Build readable HTML
            html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{case.citation} - {case.case_name or "Case"}</title>
    <style>
        body {{ font-family: Georgia, serif; max-width: 900px; margin: 0 auto; padding: 20px; line-height: 1.6; background: #fafafa; }}
        .header {{ background: #1a365d; color: white; padding: 20px; margin: -20px -20px 20px; }}
        .citation {{ font-size: 1.4em; font-weight: bold; }}
        .case-name {{ font-style: italic; margin-top: 10px; }}
        .meta {{ background: #e2e8f0; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
        .meta-item {{ margin: 5px 0; }}
        .meta-label {{ font-weight: bold; color: #2d3748; }}
        .judgment {{ background: white; padding: 20px; border: 1px solid #e2e8f0; text-align: justify; }}
        .footer {{ margin-top: 20px; padding-top: 20px; border-top: 1px solid #e2e8f0; font-size: 0.9em; color: #718096; }}
    </style>
</head>
<body>
    <div class="header">
        <div class="citation">{case.citation}</div>
        <div class="case-name">{case.case_name or ""}</div>
    </div>
    <div class="meta">
        <div class="meta-item"><span class="meta-label">Court:</span> {case.court or "N/A"}</div>
        <div class="meta-item"><span class="meta-label">Judge:</span> {', '.join(case.judges) if case.judges else "N/A"}</div>
        <div class="meta-item"><span class="meta-label">Date:</span> {case.date or "N/A"}</div>
    </div>
    <div class="judgment">
        {judgment}
    </div>
    <div class="footer">
        Source: Pakistan Law Site | Scraped: {case.fetched_at or "N/A"}
    </div>
</body>
</html>'''
            readable_path.write_text(html_content, encoding='utf-8')
        except Exception as e:
            logger.warning(f"Could not save readable HTML for {case.citation}: {e}")

    # ── Login ─────────────────────────────────────────────────────────────────

    def login(self) -> bool:
        """Login to PLS with ClearLoginHistory flow.
        
        ClearLoginHistory both clears old sessions AND logs in the current session.
        After calling it, we check /Login/Check. Only fall back to Login/Login if needed.
        """
        # Check operating hours before login
        if not self._is_pls_open():
            self._wait_for_pls_open()

        logger.info("Logging in to PLS...")

        self.session = self._create_session()

        # Get homepage for CSRF token (direct request to avoid recursion)
        try:
            resp = self.session.get(f"{BASE_URL}/", timeout=30)
            if resp.status_code != 200:
                logger.error(f"Failed to load homepage: status {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"Failed to load homepage: {e}")
            return False

        # Simulate reading the homepage
        self._human_delay(reading=True)

        # Extract CSRF token
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False

        csrf_token = csrf_match.group(1)
        logger.debug(f"CSRF token: {csrf_token[:40]}...")

        self._human_delay(2, 4)

        # ClearLoginHistory — this clears old sessions AND logs us in
        logger.info("  Clearing login history (also logs in)...")
        try:
            self.session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
                "Login.UserName": PLS_USER,
                "Login.Password": PLS_PASS,
                "__RequestVerificationToken": csrf_token,
            }, timeout=30)
        except Exception:
            pass
        self._human_delay(2, 4)

        # Check if ClearLoginHistory logged us in
        try:
            check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
            if check_resp and check_resp.status_code == 200 and "Logout" in check_resp.text:
                self.logged_in = True
                self.requests_since_break = 0
                logger.info("✓ Login successful! (via ClearLoginHistory)")
                self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
                return True
        except Exception:
            pass

        # If not logged in yet, try explicit Login/Login
        logger.info("  ClearLoginHistory didn't log in, trying Login/Login...")
        
        # Get fresh CSRF token
        try:
            resp2 = self.session.get(f"{BASE_URL}/", timeout=30)
            csrf_match2 = re.search(
                r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                resp2.text
            )
            if csrf_match2:
                csrf_token = csrf_match2.group(1)
        except Exception:
            pass
        self._human_delay(1, 2)

        # Submit login form
        login_resp = self._request("POST", f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
            "__RequestVerificationToken": csrf_token
        })

        if not login_resp:
            logger.error("Login request failed")
            return False

        # Handle "Account Already In Use" — shouldn't happen after ClearLoginHistory
        if "Account Already In Use" in login_resp.text:
            logger.warning("Account still in use — waiting and retrying...")
            time.sleep(10)
            return False

        self._human_delay(2, 3)

        # Verify login — check login response itself
        if login_resp and "Logout" in login_resp.text:
            self.logged_in = True
            self.requests_since_break = 0
            logger.info("✓ Login successful! (via Login/Login)")
            self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
            return True

        # Fallback: check /Login/Check
        check_resp = self._request("GET", f"{BASE_URL}/Login/Check")
        if check_resp and "Logout" in check_resp.text:
            self.logged_in = True
            self.requests_since_break = 0
            logger.info("✓ Login successful! (verified via /Login/Check)")
            self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
            return True

        logger.error("Login verification failed")
        return False

    # ── Search Methods ────────────────────────────────────────────────────────

    def citation_search(self, year: int, reporter: str) -> List[Dict]:
        """Search cases by year and reporter."""
        if not self.logged_in:
            if not self.login():
                return []

        logger.info(f"Searching: {year} {reporter}")

        resp = self._request("POST", f"{BASE_URL}/Login/CitationSearch", data={
            "year": year,
            "book": reporter,
            "code": "",
            "court": "",
            "judge": "",
            "lawyer": "",
            "party": "",
        })

        if not resp:
            return []

        cases = self._parse_search_results(resp.text)
        logger.info(f"  Found {len(cases)} cases")

        return cases

    def _parse_search_results(self, html: str) -> List[Dict]:
        """Parse case listings from search results."""
        cases = []
        soup = BeautifulSoup(html, 'html.parser')

        # Format 1: Table rows with class="caseType" (CitationSearch format)
        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                # Citation is in second cell (index 1), first is row number
                citation = cells[1].get_text(strip=True) if len(cells) > 1 else ""

                # Get casetypeid from button
                btn = row.find('input', attrs={'casetypeid': True})
                case_id = btn.get('casetypeid', '') if btn else ""

                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    cases.append({
                        "citation": citation,
                        "case_name": case_id,  # casetypeid is the case identifier
                    })

        # Format 2: caseLawTable format (from keyword search)
        for table in soup.find_all('table', class_='caseLawTable'):
            onclick = table.get('onclick', '')
            case_name_match = re.search(r"'([^']+)'", onclick)
            case_name = case_name_match.group(1) if case_name_match else ""

            # Get citation from table content
            citation_match = re.search(r'\d{4}\s+[A-Z]+\s+\d+', table.get_text())
            if citation_match:
                cases.append({
                    "citation": citation_match.group(0),
                    "case_name": case_name,
                })

        # Format 3: Direct regex fallback for any citation patterns
        if not cases:
            citations = re.findall(r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', html)
            case_ids = re.findall(r'casetypeid="([^"]+)"', html)

            for i, citation in enumerate(citations):
                case_id = case_ids[i] if i < len(case_ids) else ""
                cases.append({
                    "citation": citation,
                    "case_name": case_id,
                })

        # Deduplicate
        seen = set()
        unique = []
        for c in cases:
            if c["citation"] not in seen:
                seen.add(c["citation"])
                unique.append(c)

        return unique

    # ── Case Fetching ─────────────────────────────────────────────────────────

    def fetch_case(self, case_id: str, citation: str = "") -> Optional[Case]:
        """Fetch full case content using casetypeid."""
        if not self.logged_in:
            if not self.login():
                return None

        logger.info(f"Fetching: {citation or case_id}")

        # Use GetCaseFile endpoint directly (verified working)
        resp = self._request("POST", f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": case_id,
            "headNotes": 0,
        })

        if not resp or resp.text.strip() in ["1", '"1"', ""] or len(resp.text) < 100:
            logger.warning(f"  Failed to fetch case content")
            return None

        # Simulate reading the case content
        self._human_delay(reading=True)

        html_text = resp.text
        # PLS API returns HTML wrapped as JSON string ("\u003chtml...\u003c/html\u003e")
        if html_text.startswith('"'):
            try:
                html_text = json.loads(html_text)
            except (json.JSONDecodeError, ValueError):
                pass
        case = self._parse_case_content(html_text, citation, case_id)
        return case

    def _parse_case_content(self, html: str, citation: str, case_name: str) -> Case:
        """Parse case content from HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Extract title
        title = ""
        title_elem = soup.find(['h1', 'h2', 'h3'], class_=re.compile(r'title|heading', re.I))
        if title_elem:
            title = title_elem.get_text(strip=True)

        # Extract court
        court = ""
        court_match = re.search(r'(Supreme Court|High Court|Federal Shariat|Tribunal)[^<]*', html, re.I)
        if court_match:
            court = court_match.group(0).strip()

        # Extract date
        date = ""
        date_match = re.search(r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})', html)
        if date_match:
            date = date_match.group(1)

        # Extract judges
        judges = []
        judge_section = soup.find(string=re.compile(r'Before|Coram|JUDGE', re.I))
        if judge_section:
            parent = judge_section.find_parent()
            if parent:
                judge_text = parent.get_text()
                judges = re.findall(r'(?:Mr\.|Mrs\.|Justice|J\.)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', judge_text)

        # Extract headnotes
        headnotes = ""
        hn_section = soup.find(['div', 'p'], class_=re.compile(r'headnote', re.I))
        if hn_section:
            headnotes = hn_section.get_text(strip=True)

        # Extract judgment text
        judgment = ""
        # Try different selectors
        for selector in ['.judgment', '.caseText', '#caseContent', 'div[class*="case"]']:
            elem = soup.select_one(selector)
            if elem:
                judgment = elem.get_text(separator='\n', strip=True)
                break

        if not judgment:
            # Fallback: get all text
            judgment = soup.get_text(separator='\n', strip=True)

        # Extract cited statutes
        statutes = re.findall(r'(?:Act|Ordinance|Code|Rules?),?\s+\d{4}', html)
        statutes = list(set(statutes))

        # Extract cited cases
        cited_cases = re.findall(r'\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR)\s+\d+', html)
        cited_cases = list(set(cited_cases))

        return Case(
            citation=citation,
            case_name=case_name,
            title=title,
            court=court,
            date=date,
            judges=judges,
            headnotes=headnotes,
            judgment=judgment,
            judgment_raw=html,  # Store original HTML
            statutes_cited=statutes,
            cases_cited=cited_cases,
        )

    # ── Main Scraping Loop ────────────────────────────────────────────────────

    def scrape_reporter_year(self, reporter: str, year: int) -> int:
        """Scrape all cases for a reporter/year combination."""
        search_key = f"{year}-{reporter}"

        # Reload progress from disk to pick up external flag changes
        self.progress = self._load_progress()

        if search_key in self.progress["completed_searches"]:
            logger.info(f"Skipping {search_key} (already completed)")
            return 0

        # Report status to orchestrator
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.start(task=f"Scraping {year} {reporter}", year=year, reporter=reporter)

        # Search for cases
        self._human_delay()
        cases = self.citation_search(year, reporter)

        if not cases:
            logger.info(f"No cases found for {year} {reporter}")
            self.progress["completed_searches"].append(search_key)
            self._save_progress()
            if HAS_STATUS_REPORTER and _status_reporter:
                _status_reporter.complete(success=True, message="No cases found")
            return 0

        fetched = 0
        skipped = 0
        total_cases = len(cases)
        session_start_time = time.time()

        for case_info in cases:
            citation = case_info["citation"]
            case_name = case_info["case_name"]

            # Skip if already fetched (check progress)
            if citation in self.progress["cases_fetched"]:
                logger.debug(f"Skipping {citation} (in progress file)")
                skipped += 1
                continue

            # Skip if file already exists on disk (CRITICAL: prevents re-downloading)
            parts = citation.split()
            if len(parts) >= 3:
                case_year = parts[0]
                case_reporter = parts[1]
                safe_citation = re.sub(r'[^\w\-]', '_', citation)
                existing_file = DATA_DIR / case_reporter / case_year / f"{safe_citation}.json"
                if existing_file.exists():
                    logger.debug(f"Skipping {citation} (file exists on disk, not adding to cases_fetched)")
                    skipped += 1
                    continue

            # Fetch full case
            self._human_delay()
            case = self.fetch_case(case_name, citation)

            if case:
                self._save_case(case)
                self.progress["cases_fetched"].append(citation)
                self.progress["total_cases"] += 1
                fetched += 1

                # Save progress periodically
                if fetched % 10 == 0:
                    self._save_progress()
                    # Update status for orchestrator
                    if HAS_STATUS_REPORTER and _status_reporter:
                        _status_reporter.progress_update(fetched, total_cases, f"Scraped {citation}")

        # Mark search as completed
        self.progress["completed_searches"].append(search_key)
        self._save_progress()

        # ── Session Statistics ────────────────────────────────────────────
        elapsed = time.time() - session_start_time
        elapsed_min = elapsed / 60
        elapsed_hr = elapsed / 3600
        rate = fetched / elapsed_hr if elapsed_hr > 0 else 0

        logger.info(f"{'═' * 60}")
        logger.info(f"  {search_key} Summary")
        logger.info(f"  Cases scraped:  {fetched} / {total_cases} found")
        logger.info(f"  Skipped (exist): {skipped}")
        logger.info(f"  Time elapsed:   {elapsed_min:.1f} min")
        logger.info(f"  Rate:           {rate:.1f} cases/hr")
        logger.info(f"{'═' * 60}")

        # Report completion to orchestrator
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.complete(success=True, message=f"{fetched} cases fetched")

        return fetched

    def scrape_all(self, reporters: List[str] = None, start_year: int = None, end_year: int = None):
        """Scrape all reporters and years.

        Features:
        - Respects PLS operating hours (7 AM - 9 PM PKT)
        - Takes random breaks every 30 requests (human simulation)
        - Uses reading delays when viewing content
        - Retries failed requests with exponential backoff
        - Resumes from where it left off if interrupted
        """
        reporters = reporters or REPORTERS
        start_year = start_year or START_YEAR
        end_year = end_year or END_YEAR

        logger.info(f"Starting scrape: {reporters} from {start_year} to {end_year}")
        logger.info(f"PLS hours: {PLS_OPEN_HOUR}:00 - {PLS_CLOSE_HOUR}:00 PKT")

        if not self.login():
            logger.error("Failed to login. Aborting.")
            return

        total_fetched = 0

        # Go year by year, newest first
        for year in range(end_year, start_year - 1, -1):
            # Check operating hours at the start of each year
            if not self._is_pls_open():
                self._wait_for_pls_open()
                self.logged_in = False  # Session may have expired
                if not self.login():
                    logger.error("Failed to re-login after waiting. Aborting.")
                    break

            for reporter in reporters:
                try:
                    fetched = self.scrape_reporter_year(reporter, year)
                    total_fetched += fetched
                except KeyboardInterrupt:
                    logger.info("Interrupted by user. Saving progress...")
                    self._save_progress()
                    raise
                except Exception as e:
                    logger.error(f"Error scraping {year} {reporter}: {e}")
                    self._save_progress()
                    # Re-login and continue
                    self.logged_in = False
                    time.sleep(RATE_LIMIT_BACKOFF)

        logger.info(f"Scraping complete! Total cases: {total_fetched}")
        self._save_progress()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse

    parser = argparse.ArgumentParser(description="PLS Scraper v2.0 (curl_cffi)")
    parser.add_argument("command", choices=["scrape", "test", "status", "resume"],
                        help="Command to run")
    parser.add_argument("--reporter", "-r", help="Specific reporter (e.g., SCMR)")
    parser.add_argument("--year", "-y", type=int, help="Specific year")
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7 mode)")
    parser.add_argument("--no-continue", action="store_true",
                        help="Don't auto-continue to previous years (only with --year)")

    args = parser.parse_args()

    scraper = PLSScraperV2(ignore_hours=not args.respect_hours)  # 24/7 by default

    if args.command == "test":
        # Quick test
        if scraper.login():
            print("[OK] Login successful!")
            scraper._human_delay()
            cases = scraper.citation_search(2024, "SCMR")
            print(f"[OK] Found {len(cases)} cases for 2024 SCMR")
            if cases:
                print(f"  First: {cases[0]['citation']}")
        else:
            print("[FAIL] Login failed!")

    elif args.command == "status":
        print(f"Progress file: {PROGRESS_FILE}")
        print(f"Data directory: {DATA_DIR}")
        print(f"Completed searches: {len(scraper.progress['completed_searches'])}")
        print(f"Cases fetched: {len(scraper.progress['cases_fetched'])}")
        print(f"Last updated: {scraper.progress.get('last_updated', 'Never')}")

    elif args.command == "scrape":
        reporters = [args.reporter] if args.reporter else REPORTERS
        if args.year:
            # Scrape all reporters for the specified year
            scraper.login()
            current_year = args.year
            while current_year >= START_YEAR:
                for reporter in reporters:
                    try:
                        scraper.scrape_reporter_year(reporter, current_year)
                    except KeyboardInterrupt:
                        logger.info("Interrupted by user. Saving progress...")
                        scraper._save_progress()
                        return
                    except Exception as e:
                        logger.error(f"Error scraping {current_year} {reporter}: {e}")
                        scraper.logged_in = False
                        time.sleep(RATE_LIMIT_BACKOFF)

                # Auto-verify after completing all reporters for this year
                try:
                    logger.info(f"{'═' * 60}")
                    logger.info(f"  YEAR {current_year} COMPLETE — Running verifier...")
                    logger.info(f"{'═' * 60}")
                    import subprocess
                    verify_cmd = [
                        "python", str(Path(__file__).parent / "verify_scraper.py"),
                        "--year", str(current_year), "--fix"
                    ]
                    result = subprocess.run(
                        verify_cmd,
                        cwd=str(Path(__file__).parent),
                        capture_output=True, text=True, timeout=600
                    )
                    if result.stdout:
                        for line in result.stdout.strip().split('\n')[-10:]:
                            logger.info(f"  [VERIFY] {line}")
                    if result.returncode != 0 and result.stderr:
                        for line in result.stderr.strip().split('\n')[-5:]:
                            logger.warning(f"  [VERIFY ERR] {line}")
                    logger.info(f"  Verification for {current_year} complete (exit code: {result.returncode})")
                except Exception as e:
                    logger.warning(f"  Verifier failed for {current_year}: {e}")

                if args.no_continue:
                    break

                # Auto-continue to previous year
                current_year -= 1
                if current_year >= START_YEAR:
                    logger.info(f"{'═' * 60}")
                    logger.info(f"  AUTO-CONTINUE: Moving to year {current_year}")
                    logger.info(f"{'═' * 60}")
        else:
            scraper.scrape_all(reporters, args.start_year, args.end_year)

    elif args.command == "resume":
        scraper.scrape_all()


if __name__ == "__main__":
    main()
