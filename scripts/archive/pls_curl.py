#!/usr/bin/env python3
"""
PakistanLawSite.com Scraper using curl_cffi
============================================
Uses curl_cffi for TLS fingerprint impersonation (looks like real Chrome).
Much harder to detect than regular Python requests or Playwright.

Usage:
  python pls_curl.py status
  python pls_curl.py ip-check
  python pls_curl.py fetch-cases --limit 100
  python pls_curl.py enumerate --book PLD --year 2025
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL = "https://www.pakistanlawsite.com"
LOGIN_URL = f"{BASE_URL}/Login"

# Credentials
PLS_USER = os.environ.get("PLS_USER", "")
PLS_PASS = os.environ.get("PLS_PASS", "")

# Directories
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
CASES_DIR = DATA_DIR / "cases"
JSONL_DIR = DATA_DIR / "jsonl"
PROGRESS_FILE = DATA_DIR / "progress.json"

# Rate limiting (conservative but faster than before)
MIN_DELAY_SECONDS = 4
MAX_DELAY_SECONDS = 10
READING_PAUSE_CHANCE = 0.08  # 8% chance
READING_PAUSE_MIN = 15
READING_PAUSE_MAX = 35

# Session rotation
REQUESTS_PER_SESSION = 50
DAILY_REQUEST_LIMIT = 800

# Pakistan time window
PKT = timezone(timedelta(hours=5))
SCRAPE_START_HOUR = 7
SCRAPE_END_HOUR = 21

# Journals
BOOKS = ["PLD", "SCMR", "MLD", "PCrLJ", "PTD", "PLC-Service", "PLC-Labour", "YLR", "CLC", "CLD", "GBLR"]

# ─── Logging ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ─── Progress Tracker ────────────────────────────────────────────────────────

class ProgressTracker:
    """Track scraping progress across sessions."""

    def __init__(self, filepath: Path = PROGRESS_FILE):
        self.filepath = filepath
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()

    def _load(self) -> dict:
        if self.filepath.exists():
            with open(self.filepath) as f:
                return json.load(f)
        return {
            "created": datetime.now().isoformat(),
            "daily_requests": {},
            "enumerated": {},
            "fetched_cases": [],
            "fetched_headnotes": [],
            "total_cases_found": 0,
        }

    def save(self):
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.filepath, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)

    def get_today_requests(self) -> int:
        today = date.today().isoformat()
        return self.data["daily_requests"].get(today, 0)

    def increment_requests(self):
        today = date.today().isoformat()
        self.data["daily_requests"][today] = self.get_today_requests() + 1
        self.save()

    def can_make_request(self) -> bool:
        return self.get_today_requests() < DAILY_REQUEST_LIMIT

    def mark_enumerated(self, book: str, year: int, case_names: list):
        key = f"{book}_{year}"
        existing = set(self.data["enumerated"].get(key, []))
        existing.update(case_names)
        self.data["enumerated"][key] = list(existing)
        self.data["total_cases_found"] = sum(len(v) for v in self.data["enumerated"].values())
        self.save()

    def is_case_fetched(self, case_name: str) -> bool:
        return case_name in self.data["fetched_cases"]

    def mark_case_fetched(self, case_name: str):
        if case_name not in self.data["fetched_cases"]:
            self.data["fetched_cases"].append(case_name)
            self.save()

    def get_unfetched_cases(self) -> list:
        all_cases = set()
        for cases in self.data["enumerated"].values():
            all_cases.update(cases)
        return list(all_cases - set(self.data["fetched_cases"]))


# ─── curl_cffi Scraper ───────────────────────────────────────────────────────

class PLSCurlScraper:
    """
    Scraper using curl_cffi for TLS fingerprint impersonation.
    Looks exactly like Chrome to the server.
    """

    def __init__(self, proxy_url: Optional[str] = None):
        self.progress = ProgressTracker()
        self.proxy_url = proxy_url
        self.session = self._create_session()
        self.request_count = 0
        self.session_request_count = 0
        self.consecutive_failures = 0
        self.authenticated = False

        # Create directories
        for d in [DATA_DIR, CASES_DIR, JSONL_DIR]:
            d.mkdir(parents=True, exist_ok=True)

    def _create_session(self) -> Session:
        """Create a new curl_cffi session impersonating Chrome."""
        session = Session(impersonate=BrowserType.chrome120)
        
        if self.proxy_url:
            session.proxies = {
                "http": self.proxy_url,
                "https": self.proxy_url
            }
        
        # Additional headers to look more human
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        })
        
        return session

    def _rotate_session(self):
        """Rotate to a fresh session."""
        logger.info(f"Rotating session after {self.session_request_count} requests")
        self.session.close()
        self.session = self._create_session()
        self.session_request_count = 0
        # Re-authenticate with new session
        self.authenticated = False

    def _human_delay(self):
        """Generate human-like delay."""
        # Normal distribution centered between min and max
        mean = (MIN_DELAY_SECONDS + MAX_DELAY_SECONDS) / 2
        std = (MAX_DELAY_SECONDS - MIN_DELAY_SECONDS) / 4
        delay = random.gauss(mean, std)
        
        # Occasionally take a longer "reading" pause
        if random.random() < READING_PAUSE_CHANCE:
            delay = random.uniform(READING_PAUSE_MIN, READING_PAUSE_MAX)
            logger.info(f"Reading pause ({delay:.0f}s)...")
        
        # Apply exponential backoff on failures
        if self.consecutive_failures > 0:
            delay *= (1.5 ** self.consecutive_failures)
        
        delay = max(MIN_DELAY_SECONDS, min(delay, 120))  # Cap at 2 min
        time.sleep(delay)

    def _check_time_window(self) -> bool:
        """Check if we're within allowed scraping hours (PKT)."""
        now_pkt = datetime.now(PKT)
        if not (SCRAPE_START_HOUR <= now_pkt.hour < SCRAPE_END_HOUR):
            logger.warning(f"Outside PKT window ({now_pkt.strftime('%H:%M')} PKT). Allowed: {SCRAPE_START_HOUR}:00-{SCRAPE_END_HOUR}:00")
            return False
        return True

    def _request(self, method: str, url: str, data: dict = None, retries: int = 3) -> Optional[str]:
        """Make a request with all anti-detection measures."""
        if not self.progress.can_make_request():
            logger.warning(f"Daily limit reached ({DAILY_REQUEST_LIMIT})")
            return None

        # Check if session rotation needed
        if self.session_request_count >= REQUESTS_PER_SESSION:
            self._rotate_session()
            if not self.login():
                logger.error("Failed to re-authenticate after session rotation")
                return None

        self._human_delay()

        for attempt in range(retries):
            try:
                start_time = time.time()
                
                if method == "GET":
                    resp = self.session.get(url, timeout=30)
                else:
                    resp = self.session.post(url, data=data, timeout=30)
                
                elapsed = time.time() - start_time
                self.request_count += 1
                self.session_request_count += 1
                self.progress.increment_requests()

                # Handle responses
                if resp.status_code == 200:
                    self.consecutive_failures = 0
                    if elapsed > 5:
                        logger.warning(f"Slow response: {elapsed:.1f}s")
                    return resp.text
                
                elif resp.status_code == 403:
                    logger.error(f"403 Forbidden - possible IP block")
                    self.consecutive_failures += 1
                    if attempt < retries - 1:
                        wait = 60 * (attempt + 1)
                        logger.info(f"Waiting {wait}s before retry...")
                        time.sleep(wait)
                    continue
                
                elif resp.status_code == 429:
                    logger.warning("Rate limited (429)")
                    self.consecutive_failures += 1
                    time.sleep(300)  # Wait 5 min
                    continue
                
                else:
                    logger.warning(f"Unexpected status: {resp.status_code}")
                    self.consecutive_failures += 1
                    return None

            except Exception as e:
                logger.error(f"Request error (attempt {attempt + 1}): {e}")
                self.consecutive_failures += 1
                if attempt < retries - 1:
                    time.sleep(15 * (attempt + 1))

        logger.error(f"Failed after {retries} attempts: {url}")
        return None

    def check_ip_status(self) -> bool:
        """Quick check if IP is blocked."""
        logger.info("Checking IP status...")
        try:
            resp = self.session.get(LOGIN_URL, timeout=30)
            if resp.status_code == 403:
                logger.error("IP BLOCKED (403)")
                return False
            elif resp.status_code == 200:
                logger.info("IP OK - access granted")
                return True
            else:
                logger.warning(f"Unexpected status: {resp.status_code}")
                return resp.status_code < 400
        except Exception as e:
            logger.error(f"IP check error: {e}")
            return False

    def login(self) -> bool:
        """Login to PLS with proper ASP.NET form handling."""
        logger.info("Logging in to PLS...")
        
        try:
            # Step 1: Get login page for verification token
            resp = self.session.get(LOGIN_URL, timeout=30)
            if resp.status_code == 403:
                logger.error("403 on login page - IP blocked")
                return False
            
            # Parse for ASP.NET tokens
            soup = BeautifulSoup(resp.text, 'lxml')
            
            # Find verification token
            token_input = soup.find('input', {'name': '__RequestVerificationToken'})
            if not token_input:
                logger.error("No verification token found")
                return False
            
            verification_token = token_input.get('value', '')
            
            # Step 2: Submit login
            login_data = {
                "__RequestVerificationToken": verification_token,
                "Login.UserName": PLS_USER,
                "Login.Password": PLS_PASS,
            }
            
            # Update headers for POST
            self.session.headers.update({
                "Referer": LOGIN_URL,
                "Origin": BASE_URL,
                "Content-Type": "application/x-www-form-urlencoded",
            })
            
            resp = self.session.post(f"{BASE_URL}/Login/Login", data=login_data, timeout=30)
            
            # Step 3: Verify login success
            if resp.status_code == 200 and (PLS_USER in resp.text or 'logout' in resp.text.lower()):
                logger.info("Login successful!")
                self.authenticated = True
                # Set AJAX header for API calls
                self.session.headers["X-Requested-With"] = "XMLHttpRequest"
                return True
            
            # Check cookies
            cookies = {c.name for c in self.session.cookies}
            if 'ASP.NET_SessionId' in cookies:
                logger.info("Login successful (session cookie acquired)")
                self.authenticated = True
                self.session.headers["X-Requested-With"] = "XMLHttpRequest"
                return True
            
            logger.error("Login failed - check credentials")
            return False
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    def enumerate_book_year(self, book: str, year: int) -> list:
        """Enumerate all cases for a book and year."""
        logger.info(f"Enumerating {book} {year}...")
        
        cases = []
        page = 1
        
        while True:
            data = {
                "book": book,
                "year": str(year),
                "page": str(page),
            }
            
            html = self._request("POST", f"{BASE_URL}/Login/CitationSearch", data=data)
            if not html:
                break
            
            # Parse case names
            soup = BeautifulSoup(html, 'lxml')
            page_cases = []
            
            for el in soup.find_all(['input', 'button'], attrs={'casename': True}):
                case_name = el.get('casename')
                if case_name:
                    page_cases.append(case_name)
            
            for el in soup.find_all(['input', 'button'], attrs={'casetypeid': True}):
                case_id = el.get('casetypeid')
                if case_id:
                    page_cases.append(case_id)
            
            if not page_cases:
                break
            
            cases.extend(page_cases)
            logger.info(f"  Page {page}: {len(page_cases)} cases (total: {len(cases)})")
            
            # Check for more pages
            if 'Next' not in html and 'next' not in html.lower():
                break
            
            page += 1
        
        # Save to progress
        self.progress.mark_enumerated(book, year, cases)
        logger.info(f"Enumerated {len(cases)} cases for {book} {year}")
        
        return list(set(cases))

    def fetch_case(self, case_name: str) -> Optional[dict]:
        """Fetch a single case's content."""
        if self.progress.is_case_fetched(case_name):
            return None
        
        logger.info(f"Fetching case: {case_name}")
        
        # Fetch headnotes
        headnotes_data = {"caseName": case_name}
        headnotes_html = self._request("POST", f"{BASE_URL}/Login/GetHeadNotes", data=headnotes_data)
        
        # Fetch full text
        fulltext_data = {"caseName": case_name}
        fulltext_html = self._request("POST", f"{BASE_URL}/Login/GetCaseFile", data=fulltext_data)
        
        if not headnotes_html and not fulltext_html:
            logger.warning(f"Failed to fetch case {case_name}")
            return None
        
        # Parse content
        case_data = self._parse_case(case_name, headnotes_html, fulltext_html)
        
        if case_data:
            # Save to file
            case_file = CASES_DIR / f"{case_name}.json"
            with open(case_file, 'w', encoding='utf-8') as f:
                json.dump(case_data, f, indent=2, ensure_ascii=False)
            
            # Append to JSONL
            self._append_jsonl(case_data)
            
            # Mark as fetched
            self.progress.mark_case_fetched(case_name)
            logger.info(f"  Saved: {case_data.get('citation', case_name)} ({case_data.get('char_count', 0)} chars)")
        
        return case_data

    def _parse_case(self, case_name: str, headnotes_html: str, fulltext_html: str) -> Optional[dict]:
        """Parse case content from HTML."""
        try:
            # Decode JSON-encoded HTML if needed
            for html in [headnotes_html, fulltext_html]:
                if html and html.startswith('"'):
                    try:
                        html = json.loads(html)
                    except:
                        pass
            
            # Parse headnotes
            headnotes_text = ""
            if headnotes_html:
                soup = BeautifulSoup(headnotes_html, 'lxml')
                headnotes_text = soup.get_text(separator='\n', strip=True)
            
            # Parse full text
            fulltext_text = ""
            title = ""
            if fulltext_html:
                soup = BeautifulSoup(fulltext_html, 'lxml')
                fulltext_text = soup.get_text(separator='\n', strip=True)
                
                # Try to get title from first lines
                lines = [l.strip() for l in fulltext_text.split('\n') if l.strip()]
                if lines:
                    title = lines[0][:200]
            
            # Extract citation
            combined_text = headnotes_text + " " + fulltext_text
            citation_match = re.search(r'(\d{4})\s+([A-Z]+(?:\s+[A-Z]+)?)\s+(\d+)', combined_text[:1000])
            citation = citation_match.group(0) if citation_match else ""
            
            return {
                "caseName": case_name,
                "citation": citation,
                "title": title,
                "headnotes": headnotes_text,
                "fulltext": fulltext_text,
                "fetched_at": datetime.now().isoformat(),
                "char_count": len(headnotes_text) + len(fulltext_text),
            }
            
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _append_jsonl(self, case_data: dict):
        """Append to JSONL file organized by book/year."""
        citation = case_data.get('citation', '')
        match = re.search(r'(\d{4})\s+([A-Z]+)', citation)
        
        if match:
            year, book = match.groups()
            filename = f"cases_{book}_{year}.jsonl"
        else:
            filename = "cases_unknown.jsonl"
        
        jsonl_file = JSONL_DIR / filename
        with open(jsonl_file, 'a', encoding='utf-8') as f:
            f.write(json.dumps(case_data, ensure_ascii=False) + '\n')

    def fetch_pending_cases(self, limit: int = 100):
        """Fetch unfetched cases."""
        unfetched = self.progress.get_unfetched_cases()
        
        if not unfetched:
            logger.info("No pending cases. Run enumerate first.")
            return
        
        logger.info(f"Fetching {min(limit, len(unfetched))} of {len(unfetched)} pending cases...")
        
        fetched = 0
        for case_name in unfetched[:limit]:
            if not self._check_time_window():
                logger.warning("Outside time window - stopping")
                break
            
            if not self.progress.can_make_request():
                logger.warning("Daily limit reached - stopping")
                break
            
            result = self.fetch_case(case_name)
            if result:
                fetched += 1
        
        logger.info(f"Fetched {fetched} cases this session")

    def show_status(self):
        """Show current status."""
        total = sum(len(v) for v in self.progress.data.get("enumerated", {}).values())
        fetched = len(self.progress.data.get("fetched_cases", []))
        today = self.progress.get_today_requests()
        
        print("\n" + "=" * 50)
        print("PLS CURL_CFFI SCRAPER STATUS")
        print("=" * 50)
        print(f"Cases enumerated: {total:,}")
        print(f"Cases fetched:    {fetched:,}")
        print(f"Remaining:        {total - fetched:,}")
        print(f"Today's requests: {today}/{DAILY_REQUEST_LIMIT}")
        print(f"Session requests: {self.session_request_count}/{REQUESTS_PER_SESSION}")
        print("=" * 50 + "\n")

    def close(self):
        """Clean up."""
        self.session.close()


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PLS curl_cffi Scraper")
    sub = parser.add_subparsers(dest="command")
    
    sub.add_parser("status", help="Show status")
    sub.add_parser("ip-check", help="Check if IP is blocked")
    
    enum_p = sub.add_parser("enumerate", help="Enumerate cases")
    enum_p.add_argument("--book", required=True, choices=BOOKS)
    enum_p.add_argument("--year", required=True, type=int)
    
    fetch_p = sub.add_parser("fetch-cases", help="Fetch pending cases")
    fetch_p.add_argument("--limit", type=int, default=100)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    scraper = PLSCurlScraper()
    
    try:
        if args.command == "status":
            scraper.show_status()
            return
        
        if args.command == "ip-check":
            if scraper.check_ip_status():
                print("\n[OK] IP is NOT blocked")
                sys.exit(0)
            else:
                print("\n[BLOCKED] IP is blocked")
                sys.exit(1)
        
        # Commands that need login
        if not scraper.login():
            logger.error("Cannot proceed without authentication")
            sys.exit(1)
        
        if args.command == "enumerate":
            scraper.enumerate_book_year(args.book, args.year)
        
        elif args.command == "fetch-cases":
            scraper.fetch_pending_cases(limit=args.limit)
    
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
