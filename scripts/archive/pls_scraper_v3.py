#!/usr/bin/env python3
"""
PLS Scraper v3.0 — Bright Data Web Unlocker Edition
====================================================
Uses Bright Data Web Unlocker API with Pakistan IPs.
Maximum anti-detection + IP rotation.

Features:
- Bright Data Web Unlocker (bypasses all anti-bot)
- Pakistan IP addresses only
- Session cookie management
- Human-like delays
- Resumable progress
"""

import os
import re
import json
import time
import random
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v3"
PROGRESS_FILE = DATA_DIR / "progress.json"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Bright Data Web Unlocker
BRIGHTDATA_API_KEY = os.getenv("BRIGHTDATA_API_KEY", "9df9e22c-78b8-416b-a12a-c54616392b85")
BRIGHTDATA_ZONE = "web_unlocker1"

# Timing
MIN_DELAY = 4.0
MAX_DELAY = 10.0
RATE_LIMIT_BACKOFF = 120

# Reporters
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
START_YEAR = 1947
END_YEAR = 2025

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
# Bright Data Web Unlocker Client
# ══════════════════════════════════════════════════════════════════════════════

class BrightDataClient:
    """Client for Bright Data Web Unlocker API with session support."""
    
    def __init__(self, api_key: str, zone: str = "web_unlocker1"):
        self.api_key = api_key
        self.zone = zone
        self.api_url = "https://api.brightdata.com/request"
        self.cookies = {}  # Manual cookie tracking
        
    def request(self, url: str, method: str = "GET", data: dict = None, 
                headers: list = None) -> Optional[requests.Response]:
        """Make request through Web Unlocker with Pakistan IP."""
        
        payload = {
            "zone": self.zone,
            "url": url,
            "format": "json",
            "country": "pk",  # Pakistan IP only!
        }
        
        # Add method and body for POST
        if method.upper() == "POST" and data:
            payload["method"] = "POST"
            payload["body"] = urllib.parse.urlencode(data)
            # Headers as object (key-value), not array
            payload["headers"] = {"Content-Type": "application/x-www-form-urlencoded"}
        
        # Add cookies if we have them
        if self.cookies:
            cookie_str = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
            if "headers" not in payload:
                payload["headers"] = {}
            payload["headers"]["Cookie"] = cookie_str
        
        try:
            resp = requests.post(
                self.api_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                },
                json=payload,
                timeout=120
            )
            
            if resp.status_code != 200:
                logger.warning(f"API error: {resp.status_code} - {resp.text[:200]}")
                return None
            
            result = resp.json()
            
            # Extract cookies from response
            resp_headers = result.get("headers", {})
            if isinstance(resp_headers, dict):
                cookie_header = resp_headers.get("set-cookie", "")
                if cookie_header:
                    self._parse_cookies(cookie_header)
            elif isinstance(resp_headers, list):
                # Headers might be a list of {name, value} dicts
                for h in resp_headers:
                    if isinstance(h, dict) and h.get("name", "").lower() == "set-cookie":
                        self._parse_cookies(h.get("value", ""))
            
            # Create a fake response object
            class FakeResponse:
                def __init__(self, data):
                    self.status_code = data.get("status_code", 200)
                    self.text = data.get("body", "")
                    self.headers = data.get("headers", {})
            
            return FakeResponse(result)
            
        except Exception as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def _parse_cookies(self, cookie_header):
        """Parse Set-Cookie header and store cookies."""
        if not cookie_header:
            return
        
        # Handle if cookie_header is a list
        if isinstance(cookie_header, list):
            for c in cookie_header:
                self._parse_cookies(c)
            return
        
        if not isinstance(cookie_header, str):
            return
            
        # Handle multiple cookies
        for cookie in cookie_header.split(","):
            cookie = cookie.strip()
            if "=" in cookie:
                parts = cookie.split(";")[0]  # Get just name=value
                if "=" in parts:
                    name, value = parts.split("=", 1)
                    self.cookies[name.strip()] = value.strip()
    
    def clear_cookies(self):
        """Clear stored cookies."""
        self.cookies = {}

# ══════════════════════════════════════════════════════════════════════════════
# Scraper Class
# ══════════════════════════════════════════════════════════════════════════════

class PLSScraperV3:
    """Pakistan Law Site Scraper using Bright Data Web Unlocker."""
    
    def __init__(self):
        self.client = BrightDataClient(BRIGHTDATA_API_KEY, BRIGHTDATA_ZONE)
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.progress = self._load_progress()
        self.csrf_token = ""
        
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info("Using Bright Data Web Unlocker with Pakistan IPs")
        
    def _human_delay(self, min_s: float = None, max_s: float = None):
        """Random human-like delay."""
        min_s = min_s or MIN_DELAY
        max_s = max_s or MAX_DELAY
        delay = random.uniform(min_s, max_s)
        delay += random.gauss(0, 0.5)
        delay = max(2.0, delay)
        logger.debug(f"Waiting {delay:.1f}s...")
        time.sleep(delay)
    
    def _request(self, method: str, url: str, data: dict = None) -> Optional[Any]:
        """Make request with rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY:
            time.sleep(MIN_DELAY - elapsed)
        
        resp = self.client.request(url, method, data)
        self.last_request_time = time.time()
        self.request_count += 1
        
        if resp and resp.status_code == 403:
            logger.warning(f"403 Forbidden - backing off {RATE_LIMIT_BACKOFF}s")
            time.sleep(RATE_LIMIT_BACKOFF)
            return None
        
        return resp
    
    def _load_progress(self) -> Dict:
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
            except:
                pass
        return {
            "completed_searches": [],
            "cases_fetched": [],
            "total_cases": 0,
            "last_updated": None
        }
    
    def _save_progress(self):
        self.progress["last_updated"] = datetime.now().isoformat()
        PROGRESS_FILE.write_text(json.dumps(self.progress, indent=2, ensure_ascii=False), encoding='utf-8')
    
    def _save_case(self, case: Case):
        reporter = case.citation.split()[1] if len(case.citation.split()) > 1 else "UNKNOWN"
        year = case.citation.split()[0] if case.citation else "0000"
        
        case_dir = DATA_DIR / reporter / year
        case_dir.mkdir(parents=True, exist_ok=True)
        
        safe_citation = re.sub(r'[^\w\-]', '_', case.citation)
        filepath = case_dir / f"{safe_citation}.json"
        
        filepath.write_text(json.dumps(asdict(case), indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"Saved: {case.citation}")
    
    # ── Login ─────────────────────────────────────────────────────────────────
    
    def login(self) -> bool:
        """Login to PLS via Web Unlocker."""
        logger.info("Logging in to PLS (via Pakistan proxy)...")
        
        # Clear any old cookies
        self.client.clear_cookies()
        
        # Get homepage for CSRF
        resp = self._request("GET", f"{BASE_URL}/")
        if not resp or not resp.text:
            logger.error("Failed to load homepage")
            return False
        
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            logger.error("CSRF token not found")
            return False
        
        self.csrf_token = csrf_match.group(1)
        logger.debug(f"CSRF: {self.csrf_token[:40]}...")
        
        self._human_delay(3, 5)
        
        # Submit login
        login_resp = self._request("POST", f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
            "__RequestVerificationToken": self.csrf_token
        })
        
        if not login_resp:
            logger.error("Login request failed")
            return False
        
        self._human_delay(2, 4)
        
        # Verify login
        check_resp = self._request("GET", f"{BASE_URL}/Login/Check")
        if not check_resp or "Logout" not in check_resp.text:
            logger.error("Login verification failed")
            return False
        
        self.logged_in = True
        logger.info("[OK] Login successful (Pakistan IP)")
        
        self._human_delay(3, 5)
        return True
    
    # ── Search ────────────────────────────────────────────────────────────────
    
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
        
        # Table rows with class="caseType"
        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                citation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                btn = row.find('input', attrs={'casetypeid': True})
                case_id = btn.get('casetypeid', '') if btn else ""
                
                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    cases.append({
                        "citation": citation,
                        "case_name": case_id,
                    })
        
        # Fallback regex
        if not cases:
            citations = re.findall(r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', html)
            case_ids = re.findall(r'casetypeid="([^"]+)"', html)
            
            for i, citation in enumerate(citations):
                case_id = case_ids[i] if i < len(case_ids) else ""
                cases.append({"citation": citation, "case_name": case_id})
        
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
        """Fetch full case content."""
        if not self.logged_in:
            if not self.login():
                return None
        
        logger.info(f"Fetching: {citation or case_id}")
        
        resp = self._request("POST", f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": case_id,
            "headNotes": 0,
        })
        
        if not resp or not resp.text or len(resp.text) < 100:
            logger.warning("  Failed to fetch case content")
            return None
        
        return self._parse_case_content(resp.text, citation, case_id)
    
    def _parse_case_content(self, html: str, citation: str, case_id: str) -> Case:
        """Parse case content from HTML."""
        soup = BeautifulSoup(html, 'html.parser')
        
        title = ""
        title_elem = soup.find(['h1', 'h2', 'h3'], class_=re.compile(r'title|heading', re.I))
        if title_elem:
            title = title_elem.get_text(strip=True)
        
        court = ""
        court_match = re.search(r'(Supreme Court|High Court|Federal Shariat|Tribunal)[^<]*', html, re.I)
        if court_match:
            court = court_match.group(0).strip()
        
        date = ""
        date_match = re.search(r'(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})', html)
        if date_match:
            date = date_match.group(1)
        
        judges = []
        judge_section = soup.find(string=re.compile(r'Before|Coram|Present:', re.I))
        if judge_section:
            parent = judge_section.find_parent()
            if parent:
                judges = re.findall(r'(?:Justice|J\.)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', parent.get_text())
        
        headnotes = ""
        judgment = soup.get_text(separator='\n', strip=True)
        
        statutes = re.findall(r'(?:Act|Ordinance|Code|Rules?),?\s+\d{4}', html)
        cited_cases = re.findall(r'\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR)\s+\d+', html)
        
        return Case(
            citation=citation,
            case_name=case_id,
            title=title,
            court=court,
            date=date,
            judges=list(set(judges)),
            headnotes=headnotes,
            judgment=judgment,
            statutes_cited=list(set(statutes)),
            cases_cited=list(set(cited_cases)),
        )
    
    # ── Main Loop ─────────────────────────────────────────────────────────────
    
    def scrape_reporter_year(self, reporter: str, year: int) -> int:
        """Scrape all cases for a reporter/year."""
        search_key = f"{year}-{reporter}"
        
        if search_key in self.progress["completed_searches"]:
            logger.info(f"Skipping {search_key} (already done)")
            return 0
        
        self._human_delay()
        cases = self.citation_search(year, reporter)
        
        if not cases:
            self.progress["completed_searches"].append(search_key)
            self._save_progress()
            return 0
        
        fetched = 0
        for case_info in cases:
            citation = case_info["citation"]
            case_name = case_info["case_name"]
            
            if citation in self.progress["cases_fetched"]:
                continue
            
            self._human_delay()
            case = self.fetch_case(case_name, citation)
            
            if case:
                self._save_case(case)
                self.progress["cases_fetched"].append(citation)
                self.progress["total_cases"] += 1
                fetched += 1
                
                if fetched % 10 == 0:
                    self._save_progress()
        
        self.progress["completed_searches"].append(search_key)
        self._save_progress()
        
        logger.info(f"Completed {search_key}: {fetched} cases")
        return fetched
    
    def scrape_all(self, reporters: List[str] = None, start_year: int = None, end_year: int = None):
        """Scrape all reporters and years."""
        reporters = reporters or REPORTERS
        start_year = start_year or START_YEAR
        end_year = end_year or END_YEAR
        
        logger.info(f"Starting scrape: {reporters} ({start_year}-{end_year})")
        
        if not self.login():
            return
        
        total = 0
        for year in range(end_year, start_year - 1, -1):
            for reporter in reporters:
                try:
                    total += self.scrape_reporter_year(reporter, year)
                except KeyboardInterrupt:
                    self._save_progress()
                    raise
                except Exception as e:
                    logger.error(f"Error: {e}")
                    self._save_progress()
                    self.logged_in = False
                    time.sleep(RATE_LIMIT_BACKOFF)
        
        logger.info(f"Done! Total: {total} cases")

# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="PLS Scraper v3.0 (Bright Data)")
    parser.add_argument("command", choices=["scrape", "test", "status"])
    parser.add_argument("--reporter", "-r")
    parser.add_argument("--year", "-y", type=int)
    parser.add_argument("--start-year", type=int, default=START_YEAR)
    parser.add_argument("--end-year", type=int, default=END_YEAR)
    
    args = parser.parse_args()
    scraper = PLSScraperV3()
    
    if args.command == "test":
        if scraper.login():
            print("[OK] Login successful via Pakistan proxy!")
            scraper._human_delay()
            cases = scraper.citation_search(2024, "SCMR")
            print(f"[OK] Found {len(cases)} cases")
            if cases:
                print(f"  First: {cases[0]['citation']}")
    
    elif args.command == "status":
        print(f"Data: {DATA_DIR}")
        print(f"Completed: {len(scraper.progress['completed_searches'])}")
        print(f"Cases: {len(scraper.progress['cases_fetched'])}")
    
    elif args.command == "scrape":
        reporters = [args.reporter] if args.reporter else REPORTERS
        if args.year:
            scraper.login()
            scraper.scrape_reporter_year(args.reporter or "SCMR", args.year)
        else:
            scraper.scrape_all(reporters, args.start_year, args.end_year)


if __name__ == "__main__":
    main()
