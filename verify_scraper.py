#!/usr/bin/env python3
"""
PLS Verification Script
========================
Verifies scraped data against PLS website to ensure nothing was missed.

Features:
- Compares local cases vs PLS website counts
- Identifies missing/extra cases
- Detects empty or suspiciously short judgments
- Can re-scrape missing cases with --fix
- Generates audit reports

Usage:
    python verify_scraper.py --year 2024           # Verify specific year
    python verify_scraper.py --year 2024 --fix     # Re-scrape missing cases
    python verify_scraper.py --all                 # Verify all years we have
    python verify_scraper.py --report              # Generate full audit report
"""

import os
import re
import json
import time
import random
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Set, Tuple

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Pipeline status reporting (optional)
try:
    from pipeline_status import PipelineStatusReporter, ScriptType
    _status_reporter = PipelineStatusReporter(ScriptType.VERIFIER, "verify_scraper")
    HAS_STATUS_REPORTER = True
except ImportError:
    _status_reporter = None
    HAS_STATUS_REPORTER = False

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Configuration
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing (gentle for verification)
MIN_DELAY = 2.0
MAX_DELAY = 4.0
LOGIN_DELAY = 3.0
RATE_LIMIT_BACKOFF = 60

# Break simulation (less frequent than scraper since verification is lighter)
REQUESTS_BEFORE_BREAK = 50  # Take a break every N requests
BREAK_MIN = 20  # Minimum break seconds
BREAK_MAX = 60  # Maximum break seconds

# PLS Operating Hours (PKT = UTC+5)
PLS_OPEN_HOUR = 7   # 7 AM PKT
PLS_CLOSE_HOUR = 21  # 9 PM PKT
PKT_OFFSET = timedelta(hours=5)

# Reporters
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Quality thresholds
MIN_JUDGMENT_LENGTH = 500  # Characters - below this is suspicious
EMPTY_JUDGMENT_LENGTH = 100  # Characters - below this is definitely empty

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Data Classes
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

@dataclass
class CaseInfo:
    """Minimal case info for verification."""
    citation: str
    case_id: str  # casetypeid for API calls

@dataclass
class VerificationResult:
    """Result of verifying a reporter/year."""
    reporter: str
    year: int
    pls_count: int = 0
    local_count: int = 0
    missing_cases: List[Dict] = field(default_factory=list)  # On PLS but not local
    extra_cases: List[str] = field(default_factory=list)  # Local but not on PLS
    empty_judgments: List[str] = field(default_factory=list)  # Empty/too short
    short_judgments: List[str] = field(default_factory=list)  # Suspiciously short
    verified_at: str = ""
    status: str = "pending"  # pending, ok, warning, error

    def __post_init__(self):
        if not self.verified_at:
            self.verified_at = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        return asdict(self)

    def summary(self) -> str:
        """One-line summary."""
        issues = []
        if self.missing_cases:
            issues.append(f"{len(self.missing_cases)} missing")
        if self.extra_cases:
            issues.append(f"{len(self.extra_cases)} extra")
        if self.empty_judgments:
            issues.append(f"{len(self.empty_judgments)} empty")
        if self.short_judgments:
            issues.append(f"{len(self.short_judgments)} short")
        
        if not issues:
            return f"{self.reporter} {self.year}: [OK] ({self.local_count}/{self.pls_count} cases)"
        else:
            return f"{self.reporter} {self.year}: [WARN] {', '.join(issues)}"

@dataclass
class AuditReport:
    """Full audit report across all reporters/years."""
    generated_at: str = ""
    total_pls_cases: int = 0
    total_local_cases: int = 0
    total_missing: int = 0
    total_extra: int = 0
    total_empty: int = 0
    total_short: int = 0
    results: List[Dict] = field(default_factory=list)
    summary_by_reporter: Dict = field(default_factory=dict)
    summary_by_year: Dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.now().isoformat()

    def to_dict(self) -> Dict:
        return asdict(self)

# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Verifier Class
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

class PLSVerifier:
    """Verifies scraped data against PLS website."""

    def __init__(self, ignore_hours: bool = True):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.ignore_hours = ignore_hours  # 24/7 aggressive scraping mode
        self.requests_since_break = 0

        # Ensure audit directory exists
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)

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

            if pkt_now.hour >= PLS_CLOSE_HOUR:
                tomorrow = pkt_now.date() + timedelta(days=1)
                open_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day, PLS_OPEN_HOUR, 0)
            else:
                open_time = datetime(pkt_now.year, pkt_now.month, pkt_now.day, PLS_OPEN_HOUR, 0)

            wait_seconds = (open_time - pkt_now.replace(tzinfo=None)).total_seconds()
            wait_seconds = max(60, wait_seconds)

            hours, remainder = divmod(int(wait_seconds), 3600)
            minutes = remainder // 60

            logger.info(f"Waiting {hours}h {minutes}m until PLS opens at {PLS_OPEN_HOUR}:00 PKT...")

            chunk = min(300, wait_seconds)  # 5 minute chunks
            time.sleep(chunk)

    def _maybe_take_break(self) -> None:
        """Take a random break every N requests to simulate human behavior."""
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

    def _gentle_delay(self, min_s: float = None, max_s: float = None):
        """Wait a random delay (gentler than main scraper)."""
        min_s = min_s or MIN_DELAY
        max_s = max_s or MAX_DELAY
        delay = random.uniform(min_s, max_s)
        time.sleep(delay)

    def _request(self, method: str, url: str, retries: int = 3, **kwargs) -> Optional[any]:
        """Make a request with rate limiting, retries, and exponential backoff."""
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

    def login(self) -> bool:
        """Login to PLS with ClearLoginHistory flow.
        
        ClearLoginHistory both clears old sessions AND logs in the current session.
        """
        logger.info("Logging in to PLS...")

        self.session = self._create_session()

        # Get homepage for CSRF token
        resp = self._request("GET", f"{BASE_URL}/")
        if not resp:
            logger.error("Failed to load homepage")
            return False

        # Extract CSRF token
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False

        csrf_token = csrf_match.group(1)

        self._gentle_delay(1, 2)

        # ClearLoginHistory — clears old sessions AND logs us in
        logger.info("  Clearing login history (also logs in)...")
        try:
            self.session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
                "Login.UserName": PLS_USER,
                "Login.Password": PLS_PASS,
                "__RequestVerificationToken": csrf_token,
            }, timeout=30)
        except Exception:
            pass
        self._gentle_delay(2, 4)

        # Check if ClearLoginHistory logged us in
        try:
            check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
            if check_resp and check_resp.status_code == 200 and "Logout" in check_resp.text:
                self.logged_in = True
                self.requests_since_break = 0
                logger.info("[OK] Login successful! (via ClearLoginHistory)")
                self._gentle_delay(LOGIN_DELAY, LOGIN_DELAY + 1)
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
        self._gentle_delay(1, 2)

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

        self._gentle_delay(1, 2)

        # Verify login
        if login_resp and "Logout" in login_resp.text:
            self.logged_in = True
            self.requests_since_break = 0
            logger.info("[OK] Login successful! (via Login/Login)")
            self._gentle_delay(LOGIN_DELAY, LOGIN_DELAY + 1)
            return True

        # Fallback: check /Login/Check
        check_resp = self._request("GET", f"{BASE_URL}/Login/Check")
        if check_resp and "Logout" in check_resp.text:
            self.logged_in = True
            self.requests_since_break = 0
            logger.info("[OK] Login successful! (verified via /Login/Check)")
            self._gentle_delay(LOGIN_DELAY, LOGIN_DELAY + 1)
            return True

        logger.error("Login verification failed")
        return False

    def get_pls_cases(self, year: int, reporter: str) -> List[CaseInfo]:
        """Get list of cases from PLS for a year/reporter."""
        if not self.logged_in:
            if not self.login():
                return []

        logger.info(f"Querying PLS: {year} {reporter}")

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
        logger.info(f"  PLS has {len(cases)} cases")

        return cases

    def _parse_search_results(self, html: str) -> List[CaseInfo]:
        """Parse case listings from search results."""
        cases = []
        soup = BeautifulSoup(html, 'html.parser')

        # Format 1: Table rows with class="caseType"
        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                citation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                btn = row.find('input', attrs={'casetypeid': True})
                case_id = btn.get('casetypeid', '') if btn else ""

                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    cases.append(CaseInfo(citation=citation, case_id=case_id))

        # Fallback: regex
        if not cases:
            citations = re.findall(r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', html)
            case_ids = re.findall(r'casetypeid="([^"]+)"', html)

            for i, citation in enumerate(citations):
                case_id = case_ids[i] if i < len(case_ids) else ""
                cases.append(CaseInfo(citation=citation, case_id=case_id))

        # Deduplicate
        seen = set()
        unique = []
        for c in cases:
            if c.citation not in seen:
                seen.add(c.citation)
                unique.append(c)

        return unique

    def get_local_cases(self, year: int, reporter: str) -> Dict[str, Dict]:
        """Get local cases for a year/reporter with metadata."""
        case_dir = DATA_DIR / reporter / str(year)
        local_cases = {}

        if not case_dir.exists():
            return local_cases

        for json_file in case_dir.glob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding='utf-8'))
                citation = data.get("citation", "")
                if citation:
                    # Get judgment length
                    judgment = data.get("judgment", "") or data.get("judgment_clean", "")
                    local_cases[citation] = {
                        "file": str(json_file),
                        "judgment_length": len(judgment),
                        "case_id": data.get("case_name", ""),
                    }
            except Exception as e:
                logger.warning(f"Failed to read {json_file}: {e}")

        return local_cases

    def verify_reporter_year(self, reporter: str, year: int) -> VerificationResult:
        """Verify a specific reporter/year combination."""
        result = VerificationResult(reporter=reporter, year=year)

        # Get PLS cases
        self._gentle_delay()
        pls_cases = self.get_pls_cases(year, reporter)
        pls_citations = {c.citation for c in pls_cases}
        pls_case_map = {c.citation: c.case_id for c in pls_cases}
        result.pls_count = len(pls_cases)

        # Get local cases
        local_cases = self.get_local_cases(year, reporter)
        local_citations = set(local_cases.keys())
        result.local_count = len(local_cases)

        # Find missing (on PLS but not local)
        missing = pls_citations - local_citations
        for citation in missing:
            result.missing_cases.append({
                "citation": citation,
                "case_id": pls_case_map.get(citation, "")
            })

        # Find extra (local but not on PLS) - shouldn't happen
        extra = local_citations - pls_citations
        result.extra_cases = list(extra)

        # Check judgment quality
        for citation, info in local_cases.items():
            length = info["judgment_length"]
            if length < EMPTY_JUDGMENT_LENGTH:
                result.empty_judgments.append(citation)
            elif length < MIN_JUDGMENT_LENGTH:
                result.short_judgments.append(citation)

        # Set status
        if result.missing_cases or result.empty_judgments:
            result.status = "error"
        elif result.extra_cases or result.short_judgments:
            result.status = "warning"
        else:
            result.status = "ok"

        return result

    def fix_missing_cases(self, result: VerificationResult) -> int:
        """Re-scrape missing cases. Returns count of fixed cases."""
        if not result.missing_cases:
            return 0

        # Import the main scraper for re-fetching
        from pls_scraper_v2 import PLSScraperV2

        logger.info(f"Fixing {len(result.missing_cases)} missing cases for {result.year} {result.reporter}")

        scraper = PLSScraperV2()
        if not scraper.login():
            logger.error("Failed to login for fixing")
            return 0

        fixed = 0
        for case_info in result.missing_cases:
            citation = case_info["citation"]
            case_id = case_info["case_id"]

            logger.info(f"Re-fetching: {citation}")
            scraper._human_delay()

            case = scraper.fetch_case(case_id, citation)
            if case:
                scraper._save_case(case)
                fixed += 1
                logger.info(f"  [OK] Fixed: {citation}")
            else:
                logger.warning(f"  [FAIL] Failed to fix: {citation}")

        return fixed

    def verify_year(self, year: int, fix: bool = False) -> List[VerificationResult]:
        """Verify all reporters for a year."""
        results = []

        # Report status to orchestrator
        if HAS_STATUS_REPORTER and _status_reporter:
            _status_reporter.start(task=f"Verifying {year}", year=year, fix=fix)

        reporters_to_check = []
        for reporter in REPORTERS:
            case_dir = DATA_DIR / reporter / str(year)
            if case_dir.exists():
                reporters_to_check.append(reporter)

        for i, reporter in enumerate(reporters_to_check):
            # Check if we have any local data for this reporter/year
            case_dir = DATA_DIR / reporter / str(year)
            if not case_dir.exists():
                logger.info(f"Skipping {reporter} {year} (no local data)")
                continue

            result = self.verify_reporter_year(reporter, year)
            logger.info(result.summary())

            if fix and result.missing_cases:
                fixed = self.fix_missing_cases(result)
                logger.info(f"  Fixed {fixed}/{len(result.missing_cases)} missing cases")

            results.append(result)

            # Update status for orchestrator
            if HAS_STATUS_REPORTER and _status_reporter:
                _status_reporter.progress_update(i + 1, len(reporters_to_check), f"Verified {reporter}")

            # Be gentle between reporters
            self._gentle_delay(1, 2)

        # Report completion
        if HAS_STATUS_REPORTER and _status_reporter:
            total_missing = sum(len(r.missing_cases) for r in results)
            _status_reporter.complete(success=True, message=f"Verified {len(results)} reporters, {total_missing} missing")

        return results

    def verify_all(self) -> List[VerificationResult]:
        """Verify all local data."""
        results = []

        # Find all years we have data for
        years_with_data = set()
        for reporter in REPORTERS:
            reporter_dir = DATA_DIR / reporter
            if reporter_dir.exists():
                for year_dir in reporter_dir.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        years_with_data.add(int(year_dir.name))

        years_sorted = sorted(years_with_data, reverse=True)
        logger.info(f"Found data for {len(years_sorted)} years: {min(years_sorted)}-{max(years_sorted)}")

        for year in years_sorted:
            year_results = self.verify_year(year, fix=False)
            results.extend(year_results)

            # Longer pause between years
            self._gentle_delay(2, 4)

        return results

    def generate_report(self, results: List[VerificationResult]) -> AuditReport:
        """Generate full audit report from results."""
        report = AuditReport()

        # Aggregate stats
        by_reporter = {}
        by_year = {}

        for r in results:
            report.total_pls_cases += r.pls_count
            report.total_local_cases += r.local_count
            report.total_missing += len(r.missing_cases)
            report.total_extra += len(r.extra_cases)
            report.total_empty += len(r.empty_judgments)
            report.total_short += len(r.short_judgments)
            report.results.append(r.to_dict())

            # By reporter
            if r.reporter not in by_reporter:
                by_reporter[r.reporter] = {"pls": 0, "local": 0, "missing": 0, "empty": 0}
            by_reporter[r.reporter]["pls"] += r.pls_count
            by_reporter[r.reporter]["local"] += r.local_count
            by_reporter[r.reporter]["missing"] += len(r.missing_cases)
            by_reporter[r.reporter]["empty"] += len(r.empty_judgments)

            # By year
            year_key = str(r.year)
            if year_key not in by_year:
                by_year[year_key] = {"pls": 0, "local": 0, "missing": 0, "empty": 0}
            by_year[year_key]["pls"] += r.pls_count
            by_year[year_key]["local"] += r.local_count
            by_year[year_key]["missing"] += len(r.missing_cases)
            by_year[year_key]["empty"] += len(r.empty_judgments)

        report.summary_by_reporter = by_reporter
        report.summary_by_year = by_year

        return report

    def save_report(self, report: AuditReport, filename: str = None) -> Path:
        """Save report to audit directory."""
        if not filename:
            filename = f"{datetime.now().strftime('%Y-%m-%d')}_verification.json"

        filepath = AUDIT_DIR / filename
        filepath.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"Report saved: {filepath}")
        return filepath

    def print_summary(self, report: AuditReport):
        """Print summary to console."""
        print("\n" + "=" * 60)
        print("VERIFICATION REPORT")
        print("=" * 60)
        print(f"Generated: {report.generated_at}")
        print()
        print(f"Total cases on PLS:    {report.total_pls_cases:,}")
        print(f"Total cases scraped:   {report.total_local_cases:,}")
        print(f"Coverage:              {report.total_local_cases / max(report.total_pls_cases, 1) * 100:.1f}%")
        print()
        print(f"Missing cases:         {report.total_missing:,}")
        print(f"Extra cases:           {report.total_extra:,}")
        print(f"Empty judgments:       {report.total_empty:,}")
        print(f"Short judgments:       {report.total_short:,}")

        if report.summary_by_reporter:
            print("\n" + "-" * 40)
            print("BY REPORTER:")
            print("-" * 40)
            for reporter, stats in sorted(report.summary_by_reporter.items()):
                coverage = stats['local'] / max(stats['pls'], 1) * 100
                issues = []
                if stats['missing']: issues.append(f"{stats['missing']} missing")
                if stats['empty']: issues.append(f"{stats['empty']} empty")
                issue_str = f" ({', '.join(issues)})" if issues else ""
                print(f"  {reporter:8} {stats['local']:5}/{stats['pls']:5} ({coverage:5.1f}%){issue_str}")

        print("\n" + "=" * 60)


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# Integration Functions
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def verify_after_scrape(year: int, fix: bool = True, ignore_hours: bool = True) -> bool:
    """
    Call this after scraping a year to verify completeness.
    Returns True if verification passed (or was fixed).

    Usage in daily_scraper.py:
        from verify_scraper import verify_after_scrape
        success = verify_after_scrape(year, fix=True)

    Args:
        year: Year to verify
        fix: Whether to re-scrape missing cases
        ignore_hours: Ignore PLS operating hours check
    """
    logger.info(f"Post-scrape verification for {year}")

    verifier = PLSVerifier(ignore_hours=ignore_hours)
    results = verifier.verify_year(year, fix=fix)

    # Check if any issues remain
    has_issues = any(
        r.missing_cases or r.empty_judgments
        for r in results
    )

    # Save mini-report
    if results:
        report = verifier.generate_report(results)
        filename = f"{datetime.now().strftime('%Y-%m-%d')}_{year}_verification.json"
        verifier.save_report(report, filename)

    return not has_issues


# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
# CLI
# â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•

def main():
    parser = argparse.ArgumentParser(
        description="PLS Verification Script - Check scraped data completeness"
    )
    parser.add_argument("--year", "-y", type=int, help="Verify specific year")
    parser.add_argument("--reporter", "-r", help="Verify specific reporter")
    parser.add_argument("--fix", action="store_true", help="Re-scrape missing cases")
    parser.add_argument("--all", action="store_true", help="Verify all years we have")
    parser.add_argument("--report", action="store_true", help="Generate full audit report")
    parser.add_argument("--quiet", "-q", action="store_true", help="Minimal output")
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7 mode)")

    args = parser.parse_args()

    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)

    verifier = PLSVerifier(ignore_hours=not args.respect_hours)  # 24/7 by default
    results = []

    if args.year:
        # Verify specific year
        if args.reporter:
            # Single reporter/year
            result = verifier.verify_reporter_year(args.reporter, args.year)
            print(result.summary())
            if args.fix and result.missing_cases:
                fixed = verifier.fix_missing_cases(result)
                print(f"Fixed {fixed}/{len(result.missing_cases)} missing cases")
            results = [result]
        else:
            # All reporters for year
            results = verifier.verify_year(args.year, fix=args.fix)

    elif args.all or args.report:
        # Verify everything
        results = verifier.verify_all()

    else:
        parser.print_help()
        return

    # Generate and save report
    if results:
        report = verifier.generate_report(results)
        filepath = verifier.save_report(report)

        if not args.quiet:
            verifier.print_summary(report)
            print(f"\nDetailed report: {filepath}")


if __name__ == "__main__":
    main()
