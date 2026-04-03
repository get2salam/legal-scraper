#!/usr/bin/env python3
"""
Legislation Verification Script
================================
Verifies scraped legislation data against PLS website to ensure nothing was missed.

Features:
- Compares local statutes vs PLS website counts per letter
- Identifies missing/incomplete statutes
- Detects empty or incomplete sections
- Can re-scrape missing statutes with --fix
- Checks all 4 output formats are in sync (JSON, original HTML, clean HTML, JSONL)
- Generates audit reports

Usage:
    python verify_legislation.py --letter A              # Verify specific letter
    python verify_legislation.py --letter A --fix        # Re-scrape missing statutes
    python verify_legislation.py --all                   # Verify all letters
    python verify_legislation.py --report                # Generate full audit report
    python verify_legislation.py --sync-check            # Check if all formats are in sync
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
    _status_reporter = PipelineStatusReporter(ScriptType.VERIFIER, "verify_legislation")
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
AUDIT_DIR = DATA_DIR / "audit"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing
MIN_DELAY = 2.0
MAX_DELAY = 4.0
LOGIN_DELAY = 3.0
RATE_LIMIT_BACKOFF = 60

# Break simulation
REQUESTS_BEFORE_BREAK = 50
BREAK_MIN = 20
BREAK_MAX = 60

# PLS Operating Hours (PKT = UTC+5) - matches case law scraper (day shift for verification)
PLS_OPEN_HOUR = 7   # 7 AM PKT
PLS_CLOSE_HOUR = 21  # 9 PM PKT
PKT_OFFSET = timedelta(hours=5)

# Alphabets
ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# Quality thresholds
MIN_SECTION_LENGTH = 100   # Characters - below this is suspicious
EMPTY_SECTION_LENGTH = 20  # Characters - below this is definitely empty

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
class StatuteInfo:
    """Minimal statute info for verification."""
    name: str
    alphabet: str


@dataclass
class VerificationResult:
    """Result of verifying a letter."""
    letter: str
    pls_count: int = 0
    local_count: int = 0
    missing_statutes: List[Dict] = field(default_factory=list)  # On PLS but not local
    extra_statutes: List[str] = field(default_factory=list)  # Local but not on PLS
    incomplete_statutes: List[str] = field(default_factory=list)  # Missing sections
    empty_sections: List[Dict] = field(default_factory=list)  # Empty section content
    format_mismatches: List[Dict] = field(default_factory=list)  # Format sync issues
    verified_at: str = ""
    status: str = "pending"
    
    def __post_init__(self):
        if not self.verified_at:
            self.verified_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def summary(self) -> str:
        """One-line summary."""
        issues = []
        if self.missing_statutes:
            issues.append(f"{len(self.missing_statutes)} missing")
        if self.extra_statutes:
            issues.append(f"{len(self.extra_statutes)} extra")
        if self.incomplete_statutes:
            issues.append(f"{len(self.incomplete_statutes)} incomplete")
        if self.empty_sections:
            issues.append(f"{len(self.empty_sections)} empty sections")
        if self.format_mismatches:
            issues.append(f"{len(self.format_mismatches)} format issues")
        
        if not issues:
            return f"Letter {self.letter}: [OK] ({self.local_count}/{self.pls_count} statutes)"
        else:
            return f"Letter {self.letter}: [WARN] {', '.join(issues)}"


@dataclass
class AuditReport:
    """Full audit report across all letters."""
    generated_at: str = ""
    total_pls_statutes: int = 0
    total_local_statutes: int = 0
    total_missing: int = 0
    total_extra: int = 0
    total_incomplete: int = 0
    total_empty_sections: int = 0
    total_format_issues: int = 0
    results: List[Dict] = field(default_factory=list)
    summary_by_letter: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.generated_at:
            self.generated_at = datetime.now().isoformat()
    
    def to_dict(self) -> Dict:
        return asdict(self)


# ══════════════════════════════════════════════════════════════════════════════
# Verifier Class
# ══════════════════════════════════════════════════════════════════════════════

class LegislationVerifier:
    """Verifies scraped legislation data against PLS website."""
    
    def __init__(self, ignore_hours: bool = True):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.ignore_hours = ignore_hours
        self.requests_since_break = 0
        
        AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    
    def _is_pls_open(self) -> bool:
        """Check if PLS is within operating hours."""
        if self.ignore_hours:
            return True
        
        utc_now = datetime.now(timezone.utc)
        pkt_now = utc_now + PKT_OFFSET
        current_hour = pkt_now.hour
        
        is_open = PLS_OPEN_HOUR <= current_hour < PLS_CLOSE_HOUR
        
        if not is_open:
            logger.info(f"PLS closed (PKT: {pkt_now.strftime('%H:%M')})")
        
        return is_open
    
    def _wait_for_pls_open(self) -> None:
        """Wait until PLS opens."""
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
            
            logger.info(f"Waiting {hours}h {minutes}m until PLS opens...")
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
        """Create a new curl_cffi session."""
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        })
        return session
    
    def _gentle_delay(self, min_s: float = None, max_s: float = None):
        """Wait a random delay."""
        min_s = min_s or MIN_DELAY
        max_s = max_s or MAX_DELAY
        delay = random.uniform(min_s, max_s)
        time.sleep(delay)
    
    def _request(self, method: str, url: str, retries: int = 3, **kwargs) -> Optional[any]:
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
    
    def login(self) -> bool:
        """Login to PLS."""
        logger.info("Logging in to PLS...")
        
        self.session = self._create_session()
        
        resp = self._request("GET", f"{BASE_URL}/")
        if not resp:
            logger.error("Failed to load homepage")
            return False
        
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False
        
        csrf_token = csrf_match.group(1)
        self._gentle_delay(1, 2)
        
        login_resp = self._request("POST", f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
            "__RequestVerificationToken": csrf_token
        })
        
        if not login_resp:
            logger.error("Login request failed")
            return False
        
        self._gentle_delay(1, 2)
        
        check_resp = self._request("GET", f"{BASE_URL}/Login/Check")
        if not check_resp or "Logout" not in check_resp.text:
            logger.error("Login verification failed")
            return False
        
        self.logged_in = True
        self.requests_since_break = 0
        logger.info("✓ Login successful!")
        
        self._gentle_delay(LOGIN_DELAY, LOGIN_DELAY + 1)
        return True
    
    def get_pls_statutes(self, letter: str) -> List[StatuteInfo]:
        """Get list of statutes from PLS for a letter."""
        if not self.logged_in:
            if not self.login():
                return []
        
        logger.info(f"Querying PLS: Letter {letter}")
        
        resp = self._request("GET", f"{BASE_URL}/Login/StatuecharSearch",
                            params={"character": letter})
        
        if not resp:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        statutes = []
        
        for row in soup.find_all('tr', class_='caseType'):
            name = row.get('casetypeid', '')
            if name:
                statutes.append(StatuteInfo(name=name.strip(), alphabet=letter))
        
        logger.info(f"  PLS has {len(statutes)} statutes for '{letter}'")
        return statutes
    
    def get_local_statutes(self, letter: str) -> Dict[str, Dict]:
        """Get local statutes for a letter with metadata."""
        statute_dir = LEGISLATION_DIR / letter
        local_statutes = {}
        
        if not statute_dir.exists():
            return local_statutes
        
        for json_file in statute_dir.glob("*.json"):
            try:
                data = json.loads(json_file.read_text(encoding='utf-8'))
                title = data.get("title", data.get("citation", ""))
                if title:
                    sections = data.get("sections", [])
                    total_text_length = sum(len(s.get("text", "")) for s in sections)
                    
                    local_statutes[title] = {
                        "file": str(json_file),
                        "section_count": len(sections),
                        "total_text_length": total_text_length,
                        "statute_id": data.get("statute_id", ""),
                    }
            except Exception as e:
                logger.warning(f"Failed to read {json_file}: {e}")
        
        return local_statutes
    
    def check_format_sync(self, letter: str) -> List[Dict]:
        """Check if all 4 output formats are in sync for a letter."""
        issues = []
        
        statute_dir = LEGISLATION_DIR / letter
        html_dir = HTML_DIR / letter
        original_dir = statute_dir / "original"
        letter_jsonl = LEGISLATION_DIR / f"{letter}.jsonl"
        
        # Get all statute slugs from JSON files
        json_files = {f.stem: f for f in statute_dir.glob("*.json")}
        
        for slug, json_file in json_files.items():
            missing = []
            
            # Check original HTML
            if not (original_dir / f"{slug}.html").exists():
                missing.append("original HTML")
            
            # Check clean HTML
            if not (html_dir / f"{slug}.html").exists():
                missing.append("clean HTML")
            
            # Check JSONL entry (expensive, sample check)
            # We'll just verify JSONL file exists for the letter
            if not letter_jsonl.exists():
                missing.append("letter JSONL")
            
            if missing:
                issues.append({
                    "statute": slug,
                    "missing_formats": missing
                })
        
        return issues
    
    def verify_letter(self, letter: str) -> VerificationResult:
        """Verify all statutes for a specific letter."""
        result = VerificationResult(letter=letter)
        
        # Get PLS statutes
        self._gentle_delay()
        pls_statutes = self.get_pls_statutes(letter)
        pls_names = {s.name for s in pls_statutes}
        result.pls_count = len(pls_statutes)
        
        # Get local statutes
        local_statutes = self.get_local_statutes(letter)
        local_names = set(local_statutes.keys())
        result.local_count = len(local_statutes)
        
        # Find missing (on PLS but not local)
        missing = pls_names - local_names
        for name in missing:
            result.missing_statutes.append({"name": name, "alphabet": letter})
        
        # Find extra (local but not on PLS)
        extra = local_names - pls_names
        result.extra_statutes = list(extra)
        
        # Check quality of local statutes
        for name, info in local_statutes.items():
            # Check for incomplete statutes (no sections)
            if info["section_count"] == 0:
                result.incomplete_statutes.append(name)
            
            # Check for empty sections
            if info["total_text_length"] < EMPTY_SECTION_LENGTH:
                result.empty_sections.append({
                    "statute": name,
                    "text_length": info["total_text_length"]
                })
        
        # Check format sync
        format_issues = self.check_format_sync(letter)
        result.format_mismatches = format_issues
        
        # Set status
        if result.missing_statutes or result.incomplete_statutes:
            result.status = "error"
        elif result.extra_statutes or result.empty_sections or result.format_mismatches:
            result.status = "warning"
        else:
            result.status = "ok"
        
        return result
    
    def fix_missing_statutes(self, result: VerificationResult) -> int:
        """Re-scrape missing statutes. Returns count of fixed statutes."""
        if not result.missing_statutes:
            return 0
        
        # Import the scraper for re-fetching
        from legislation_scraper_v2 import LegislationScraperV2
        
        logger.info(f"Fixing {len(result.missing_statutes)} missing statutes for letter {result.letter}")
        
        scraper = LegislationScraperV2()
        if not scraper.login():
            logger.error("Failed to login for fixing")
            return 0
        
        fixed = 0
        for statute_info in result.missing_statutes:
            name = statute_info["name"]
            letter = statute_info["alphabet"]
            
            logger.info(f"Re-fetching: {name}")
            scraper._human_delay()
            
            result_data = scraper.scrape_statute({"name": name, "alphabet": letter})
            if result_data:
                statute, raw_html = result_data
                scraper._save_statute(statute, raw_html)
                fixed += 1
                logger.info(f"  ✓ Fixed: {name}")
            else:
                logger.warning(f"  ✗ Failed to fix: {name}")
        
        return fixed
    
    def fix_format_sync(self, letter: str) -> int:
        """Regenerate missing format files from JSON source."""
        issues = self.check_format_sync(letter)
        if not issues:
            return 0
        
        from legislation_scraper_v2 import generate_statute_page_html
        from html_cleaner import generate_statute_slug
        
        fixed = 0
        statute_dir = LEGISLATION_DIR / letter
        html_dir = HTML_DIR / letter
        
        for issue in issues:
            slug = issue["statute"]
            json_file = statute_dir / f"{slug}.json"
            
            if not json_file.exists():
                continue
            
            try:
                data = json.loads(json_file.read_text(encoding='utf-8'))
                
                # Regenerate clean HTML if missing
                if "clean HTML" in issue["missing_formats"]:
                    html_content = generate_statute_page_html(data)
                    html_path = html_dir / f"{slug}.html"
                    html_path.write_text(html_content, encoding='utf-8')
                    fixed += 1
                    logger.info(f"  Generated clean HTML for {slug}")
                
            except Exception as e:
                logger.error(f"Failed to fix format for {slug}: {e}")
        
        return fixed
    
    def verify_all(self) -> List[VerificationResult]:
        """Verify all letters."""
        results = []
        
        # Find letters we have data for
        letters_with_data = set()
        if LEGISLATION_DIR.exists():
            for letter_dir in LEGISLATION_DIR.iterdir():
                if letter_dir.is_dir() and letter_dir.name in ALPHABETS:
                    letters_with_data.add(letter_dir.name)
        
        if not letters_with_data:
            logger.warning("No legislation data found")
            return results
        
        logger.info(f"Found data for {len(letters_with_data)} letters")
        
        for letter in sorted(letters_with_data):
            result = self.verify_letter(letter)
            logger.info(result.summary())
            results.append(result)
            self._gentle_delay(1, 2)
        
        return results
    
    def generate_report(self, results: List[VerificationResult]) -> AuditReport:
        """Generate full audit report from results."""
        report = AuditReport()
        
        by_letter = {}
        
        for r in results:
            report.total_pls_statutes += r.pls_count
            report.total_local_statutes += r.local_count
            report.total_missing += len(r.missing_statutes)
            report.total_extra += len(r.extra_statutes)
            report.total_incomplete += len(r.incomplete_statutes)
            report.total_empty_sections += len(r.empty_sections)
            report.total_format_issues += len(r.format_mismatches)
            report.results.append(r.to_dict())
            
            by_letter[r.letter] = {
                "pls": r.pls_count,
                "local": r.local_count,
                "missing": len(r.missing_statutes),
                "incomplete": len(r.incomplete_statutes),
                "status": r.status
            }
        
        report.summary_by_letter = by_letter
        return report
    
    def save_report(self, report: AuditReport, filename: str = None) -> Path:
        """Save report to audit directory."""
        if not filename:
            filename = f"{datetime.now().strftime('%Y-%m-%d')}_legislation_verification.json"
        
        filepath = AUDIT_DIR / filename
        filepath.write_text(json.dumps(report.to_dict(), indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"Report saved: {filepath}")
        return filepath
    
    def print_summary(self, report: AuditReport):
        """Print summary to console."""
        print("\n" + "=" * 60)
        print("LEGISLATION VERIFICATION REPORT")
        print("=" * 60)
        print(f"Generated: {report.generated_at}")
        print()
        print(f"Total statutes on PLS:   {report.total_pls_statutes:,}")
        print(f"Total statutes scraped:  {report.total_local_statutes:,}")
        coverage = report.total_local_statutes / max(report.total_pls_statutes, 1) * 100
        print(f"Coverage:                {coverage:.1f}%")
        print()
        print(f"Missing statutes:        {report.total_missing:,}")
        print(f"Extra statutes:          {report.total_extra:,}")
        print(f"Incomplete statutes:     {report.total_incomplete:,}")
        print(f"Empty sections:          {report.total_empty_sections:,}")
        print(f"Format sync issues:      {report.total_format_issues:,}")
        
        if report.summary_by_letter:
            print("\n" + "-" * 40)
            print("BY LETTER:")
            print("-" * 40)
            for letter, stats in sorted(report.summary_by_letter.items()):
                coverage = stats['local'] / max(stats['pls'], 1) * 100
                status_icon = "[OK]" if stats['status'] == 'ok' else "[WARN]" if stats['status'] == 'warning' else "[FAIL]"
                issues = []
                if stats['missing']: issues.append(f"{stats['missing']} missing")
                if stats['incomplete']: issues.append(f"{stats['incomplete']} incomplete")
                issue_str = f" ({', '.join(issues)})" if issues else ""
                print(f"  {letter}: {status_icon} {stats['local']:4}/{stats['pls']:4} ({coverage:5.1f}%){issue_str}")
        
        print("\n" + "=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# Integration Functions
# ══════════════════════════════════════════════════════════════════════════════

def verify_after_scrape(letter: str, fix: bool = True, ignore_hours: bool = True) -> bool:
    """
    Call this after scraping a letter to verify completeness.
    Returns True if verification passed (or was fixed).
    """
    logger.info(f"Post-scrape verification for letter {letter}")
    
    verifier = LegislationVerifier(ignore_hours=ignore_hours)
    result = verifier.verify_letter(letter)
    
    if fix:
        if result.missing_statutes:
            verifier.fix_missing_statutes(result)
        if result.format_mismatches:
            verifier.fix_format_sync(letter)
    
    # Check if any issues remain
    has_issues = bool(result.missing_statutes or result.incomplete_statutes)
    
    if [result]:
        report = verifier.generate_report([result])
        filename = f"{datetime.now().strftime('%Y-%m-%d')}_{letter}_verification.json"
        verifier.save_report(report, filename)
    
    return not has_issues


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Legislation Verification Script - Check scraped data completeness"
    )
    parser.add_argument("--letter", "-l", help="Verify specific letter")
    parser.add_argument("--fix", action="store_true", help="Re-scrape missing statutes")
    parser.add_argument("--all", action="store_true", help="Verify all letters")
    parser.add_argument("--report", action="store_true", help="Generate full audit report")
    parser.add_argument("--sync-check", action="store_true", help="Check format synchronization")
    parser.add_argument("--fix-sync", action="store_true", help="Fix format sync issues")
    parser.add_argument("--quiet", "-q", action="store_true", help="Minimal output")
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7)")
    
    args = parser.parse_args()
    
    if args.quiet:
        logging.getLogger().setLevel(logging.WARNING)
    
    verifier = LegislationVerifier(ignore_hours=not args.respect_hours)
    results = []
    
    if args.sync_check or args.fix_sync:
        # Just check/fix format sync without PLS queries
        for letter in ALPHABETS:
            if (LEGISLATION_DIR / letter).exists():
                issues = verifier.check_format_sync(letter)
                if issues:
                    print(f"Letter {letter}: {len(issues)} format issues")
                    for issue in issues[:5]:  # Show first 5
                        print(f"  - {issue['statute']}: missing {', '.join(issue['missing_formats'])}")
                    if len(issues) > 5:
                        print(f"  ... and {len(issues) - 5} more")
                    
                    if args.fix_sync:
                        fixed = verifier.fix_format_sync(letter)
                        print(f"  Fixed {fixed} issues")
                else:
                    print(f"Letter {letter}: [OK] All formats in sync")
        return
    
    if args.letter:
        result = verifier.verify_letter(args.letter)
        print(result.summary())
        if args.fix:
            if result.missing_statutes:
                fixed = verifier.fix_missing_statutes(result)
                print(f"Fixed {fixed}/{len(result.missing_statutes)} missing statutes")
            if result.format_mismatches:
                fixed = verifier.fix_format_sync(args.letter)
                print(f"Fixed {fixed} format issues")
        results = [result]
    
    elif args.all or args.report:
        results = verifier.verify_all()
    
    else:
        parser.print_help()
        return
    
    if results:
        report = verifier.generate_report(results)
        filepath = verifier.save_report(report)
        
        if not args.quiet:
            verifier.print_summary(report)
            print(f"\nDetailed report: {filepath}")


if __name__ == "__main__":
    main()
