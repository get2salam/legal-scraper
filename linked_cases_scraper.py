#!/usr/bin/env python3
"""
Linked Cases Scraper
====================
Scrapes the actual case content for cases referenced in statute links.
Reads from statute_case_links.jsonl and fetches each case that doesn't exist yet.

Uses the same curl_cffi + Chrome TLS fingerprint as other scrapers.
"""

import os
import re
import json
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Set

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path("data_v2")
LINKS_FILE = DATA_DIR / "legislation" / "statute_case_links.jsonl"
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Operating Hours (Night shift: 10 PM - 5 AM PKT)
PLS_OPEN_HOUR = 22  # 10 PM PKT
PLS_CLOSE_HOUR = 5   # 5 AM PKT
PKT_OFFSET = timedelta(hours=5)
NIGHT_MODE = True

# Timing
MIN_DELAY = 3.0
MAX_DELAY = 8.0
READING_DELAY_MIN = 2.0
READING_DELAY_MAX = 5.0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class LinkedCasesScraper:
    """Scrapes case content for cases linked from statutes."""
    
    def __init__(self, ignore_hours: bool = True):
        self.session = Session(impersonate=BrowserType.chrome120)
        self.logged_in = False
        self.ignore_hours = ignore_hours
        self.cases_scraped = 0
        self.cases_skipped = 0
        self.cases_failed = 0
        self._jsonl_sets: Dict[str, Set[str]] = {}
    
    def _is_open(self) -> bool:
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
            logger.info(f"Outside hours (PKT: {pkt_now.strftime('%H:%M')}). Window: {PLS_OPEN_HOUR}:00-{PLS_CLOSE_HOUR}:00 PKT")
        
        return is_open
    
    def _human_delay(self, min_sec: float = None, max_sec: float = None):
        """Random delay to simulate human behavior."""
        min_sec = min_sec or MIN_DELAY
        max_sec = max_sec or MAX_DELAY
        delay = random.uniform(min_sec, max_sec)
        time.sleep(delay)
    
    def _login(self) -> bool:
        """Login to PLS using CSRF token (same as case law scraper)."""
        if self.logged_in:
            return True
        
        logger.info("Logging in to PLS...")
        
        try:
            # Get homepage for CSRF token
            resp = self.session.get(f"{BASE_URL}/", timeout=30)
            if resp.status_code != 200:
                logger.error(f"Failed to load homepage: {resp.status_code}")
                return False
            
            self._human_delay(1, 2)
            
            # Extract CSRF token
            soup = BeautifulSoup(resp.text, 'html.parser')
            csrf_input = soup.find('input', {'name': '__RequestVerificationToken'})
            csrf_token = csrf_input.get('value', '') if csrf_input else ''
            
            if not csrf_token:
                logger.warning("No CSRF token found, trying without...")
            
            # Submit login with correct field names
            login_resp = self.session.post(
                f"{BASE_URL}/Login/Login",
                data={
                    "Login.UserName": PLS_USER,
                    "Login.Password": PLS_PASS,
                    "__RequestVerificationToken": csrf_token
                },
                allow_redirects=True,
                timeout=30
            )
            
            self._human_delay(2, 3)
            
            # Verify login
            check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
            if check_resp and "Logout" in check_resp.text:
                self.logged_in = True
                logger.info("[OK] Login successful!")
                return True
            else:
                logger.error("Login verification failed")
                return False
                
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False
    
    def parse_citation(self, citation: str) -> Optional[Dict]:
        """Parse citation like '1986 PLD 29' into components."""
        citation = citation.strip().rstrip(',')
        parts = citation.split()
        
        if len(parts) < 3:
            return None
        
        # Handle formats: "1986 PLD 29" or "1986 PLD 29 LAHORE"
        year = parts[0]
        reporter = parts[1]
        page = parts[2]
        
        # Validate
        if not year.isdigit() or len(year) != 4:
            return None
        if not page.isdigit():
            return None
        
        return {
            "year": year,
            "reporter": reporter,
            "page": page,
            "citation": f"{year} {reporter} {page}"
        }
    
    def case_exists(self, year: str, reporter: str, page: str) -> bool:
        """Check if case already exists locally."""
        case_file = DATA_DIR / reporter / year / f"{year}_{reporter}_{page}.json"
        return case_file.exists()
    
    def get_unique_citations(self) -> List[Dict]:
        """Get unique citations from statute links that we don't have yet."""
        if not LINKS_FILE.exists():
            logger.error(f"Links file not found: {LINKS_FILE}")
            return []
        
        seen = set()
        citations = []
        
        with open(LINKS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    link = json.loads(line)
                    citation = link.get("citation", "")
                    parsed = self.parse_citation(citation)
                    
                    if parsed:
                        key = f"{parsed['year']}_{parsed['reporter']}_{parsed['page']}"
                        if key not in seen:
                            seen.add(key)
                            if not self.case_exists(parsed['year'], parsed['reporter'], parsed['page']):
                                citations.append(parsed)
                except:
                    continue
        
        return citations
    
    def scrape_case(self, year: str, reporter: str, page: str) -> Optional[Dict]:
        """Scrape a single case by citation (same approach as case law scraper)."""
        citation = f"{year} {reporter} {page}"
        
        if not self._is_open():
            logger.info("Outside operating hours, waiting...")
            return None
        
        if not self._login():
            return None
        
        self._human_delay()
        
        try:
            # Step 1: Search for the case to get casetypeid
            search_resp = self.session.post(
                f"{BASE_URL}/Login/CitationSearch",
                data={
                    "year": year,
                    "book": reporter,
                    "code": page,
                    "court": "",
                    "judge": "",
                },
                timeout=30
            )
            
            if search_resp.status_code != 200:
                logger.warning(f"Search failed for {citation}: HTTP {search_resp.status_code}")
                return None
            
            # Find casetypeid in response
            case_id = None
            soup = BeautifulSoup(search_resp.text, 'html.parser')
            
            # Look for casetypeid in button or input
            btn = soup.find('input', attrs={'casetypeid': True})
            if btn:
                case_id = btn.get('casetypeid')
            else:
                # Try regex fallback
                match = re.search(r'casetypeid="([^"]+)"', search_resp.text)
                if match:
                    case_id = match.group(1)
            
            if not case_id:
                logger.warning(f"  No casetypeid found for {citation}")
                return None
            
            self._human_delay(1, 2)
            
            # Step 2: Fetch full case content
            case_resp = self.session.post(
                f"{BASE_URL}/Login/GetCaseFile",
                data={
                    "caseName": case_id,
                    "headNotes": 0,
                },
                timeout=30
            )
            
            if case_resp.status_code != 200 or len(case_resp.text) < 100:
                logger.warning(f"  Failed to fetch case content for {citation}")
                return None
            
            # Parse case content
            case_data = {
                "citation": citation,
                "year": int(year),
                "reporter": reporter,
                "page": int(page),
                "case_name": case_id,
                "scraped_at": datetime.now().isoformat(),
                "source": "linked_cases_scraper",
                "judgment_raw": case_resp.text
            }
            
            # Extract structured data
            soup = BeautifulSoup(case_resp.text, 'html.parser')
            
            # Title/parties
            title_elem = soup.find(['h1', 'h2', 'h3'])
            if title_elem:
                case_data["title"] = title_elem.get_text(strip=True)
            
            # Court
            court_match = re.search(r'(Supreme Court|High Court|Tribunal|Court)[^<]*', case_resp.text, re.I)
            if court_match:
                case_data["court"] = court_match.group(0).strip()
            
            # Full text
            case_data["judgment_text"] = soup.get_text(separator='\n', strip=True)
            
            return case_data
            
        except Exception as e:
            logger.error(f"Error scraping {citation}: {e}")
            return None
    
    def save_case(self, case_data: Dict):
        """Save case in all 4 formats: JSON, Original HTML, Readable HTML, JSONL."""
        year = str(case_data["year"])
        reporter = case_data["reporter"]
        page = str(case_data["page"])
        safe_citation = f"{year}_{reporter}_{page}"
        citation = case_data.get("citation", f"{year} {reporter} {page}")

        # 1. JSON
        case_dir = DATA_DIR / reporter / year
        case_dir.mkdir(parents=True, exist_ok=True)
        case_file = case_dir / f"{safe_citation}.json"
        with open(case_file, 'w', encoding='utf-8') as f:
            json.dump(case_data, f, ensure_ascii=False, indent=2)

        # 2. Original HTML
        if "judgment_raw" in case_data:
            orig_dir = case_dir / "original"
            orig_dir.mkdir(exist_ok=True)
            (orig_dir / f"{safe_citation}.html").write_text(case_data["judgment_raw"], encoding='utf-8')

        # 3. Readable HTML
        html_dir = DATA_DIR / "html" / reporter / year
        html_dir.mkdir(parents=True, exist_ok=True)
        court = case_data.get("court", "")
        judges = case_data.get("judges", "")
        date_decided = case_data.get("date_decided", case_data.get("scraped_at", ""))
        title = case_data.get("title", "")
        judgment_raw = case_data.get("judgment_raw", "")
        headnotes = case_data.get("headnotes", "")

        readable = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{citation}</title>
<style>body{{font-family:Georgia,serif;max-width:800px;margin:40px auto;padding:20px;line-height:1.6;color:#333}}
h1{{font-size:1.4rem;border-bottom:2px solid #333;padding-bottom:10px}}
.meta{{background:#f5f5f5;padding:15px;border-radius:5px;margin:15px 0}}
.headnotes{{border-left:3px solid #666;padding-left:15px;margin:20px 0;color:#555}}</style></head>
<body><h1>{citation}</h1>
{f'<h2>{title}</h2>' if title else ''}
<div class="meta"><b>Court:</b> {court}<br><b>Judges:</b> {judges}<br>
<b>Date:</b> {date_decided}</div>
{f'<div class="headnotes"><h3>Headnotes</h3>{headnotes}</div>' if headnotes else ''}
<div class="judgment">{judgment_raw}</div></body></html>"""
        (html_dir / f"{safe_citation}.html").write_text(readable, encoding='utf-8')

        # 4. JSONL (reporter + master) with dedup
        jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
        jsonl_key = str(jsonl_path)
        if jsonl_key not in self._jsonl_sets:
            self._jsonl_sets[jsonl_key] = set()
            if jsonl_path.exists():
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        m = re.search(r'"citation":\s*"([^"]+)"', line)
                        if m:
                            self._jsonl_sets[jsonl_key].add(m.group(1))

        if citation not in self._jsonl_sets[jsonl_key]:
            with open(jsonl_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(case_data, ensure_ascii=False) + '\n')
            self._jsonl_sets[jsonl_key].add(citation)

        # Master JSONL
        master = DATA_DIR / "all_cases.jsonl"
        master_key = str(master)
        if master_key not in self._jsonl_sets:
            self._jsonl_sets[master_key] = set()
            # Don't scan master (too large) — just append, dedup handled at reporter level
        with open(master, 'a', encoding='utf-8') as f:
            f.write(json.dumps(case_data, ensure_ascii=False) + '\n')

        logger.info(f"  Saved (4 formats): {case_file.name}")
    
    def run(self, limit: int = None):
        """Run the scraper."""
        logger.info("=" * 60)
        logger.info("Linked Cases Scraper")
        logger.info("=" * 60)
        
        # Get citations to scrape
        citations = self.get_unique_citations()
        total = len(citations)
        
        logger.info(f"Found {total} unique cases to scrape")
        
        if limit:
            citations = citations[:limit]
            logger.info(f"Limiting to {limit} cases")
        
        for i, cit in enumerate(citations):
            if not self._is_open():
                logger.info("Outside operating hours. Stopping.")
                break
            
            year, reporter, page = cit["year"], cit["reporter"], cit["page"]
            logger.info(f"[{i+1}/{len(citations)}] Scraping {cit['citation']}...")
            
            case_data = self.scrape_case(year, reporter, page)
            
            if case_data:
                self.save_case(case_data)
                self.cases_scraped += 1
            else:
                self.cases_failed += 1
            
            # Progress update
            if (i + 1) % 10 == 0:
                logger.info(f"  Progress: {i+1}/{len(citations)} | Scraped: {self.cases_scraped} | Failed: {self.cases_failed}")
        
        # Summary
        logger.info("=" * 60)
        logger.info(f"Completed: {self.cases_scraped} scraped, {self.cases_failed} failed")
        logger.info("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Scrape cases linked from statutes")
    parser.add_argument("--limit", "-n", type=int, help="Limit number of cases")
    parser.add_argument("--respect-hours", action="store_true", help="Respect PLS hours (default: 24/7)")
    args = parser.parse_args()
    
    scraper = LinkedCasesScraper(ignore_hours=(not args.respect_hours))
    scraper.run(limit=args.limit)


if __name__ == "__main__":
    main()
