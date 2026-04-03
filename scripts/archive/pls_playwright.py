#!/usr/bin/env python3
"""
PakistanLawSite.com Playwright Scraper
======================================
Uses real browser automation to avoid detection.
Mimics human behavior with realistic timing and interactions.
"""

import asyncio
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

from playwright.async_api import async_playwright, Page, Browser, BrowserContext
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
SESSION_DIR = DATA_DIR / "browser_session"

# Rate limiting (more conservative for safety)
MIN_DELAY_SECONDS = 5
MAX_DELAY_SECONDS = 12
READING_PAUSE_CHANCE = 0.10
READING_PAUSE_MIN = 15
READING_PAUSE_MAX = 45
REQUESTS_PER_HOUR_SAFE = 50

# Pakistan time
PKT = timezone(timedelta(hours=5))
SCRAPE_START_HOUR = 7
SCRAPE_END_HOUR = 21

# Journal codes
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

    def is_case_fetched(self, case_name: str) -> bool:
        return case_name in self.data["fetched_cases"]

    def mark_case_fetched(self, case_name: str):
        if case_name not in self.data["fetched_cases"]:
            self.data["fetched_cases"].append(case_name)
            self.save()

    def mark_enumerated(self, book: str, year: int, case_names: list):
        key = f"{book}_{year}"
        existing = set(self.data["enumerated"].get(key, []))
        existing.update(case_names)
        self.data["enumerated"][key] = list(existing)
        self.data["total_cases_found"] = sum(len(v) for v in self.data["enumerated"].values())
        self.save()

    def get_unfetched_cases(self) -> list:
        all_cases = set()
        for cases in self.data["enumerated"].values():
            all_cases.update(cases)
        return list(all_cases - set(self.data["fetched_cases"]))


# ─── Playwright Scraper ──────────────────────────────────────────────────────

class PLSPlaywrightScraper:
    """Browser-based scraper using Playwright."""

    def __init__(self):
        self.progress = ProgressTracker()
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.request_count = 0
        self.hour_requests = 0
        self.last_hour = datetime.now().hour

        # Create directories
        for d in [DATA_DIR, CASES_DIR, JSONL_DIR, SESSION_DIR]:
            d.mkdir(parents=True, exist_ok=True)

    async def _human_delay(self):
        """Random delay to mimic human behavior."""
        delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
        
        if random.random() < READING_PAUSE_CHANCE:
            delay = random.uniform(READING_PAUSE_MIN, READING_PAUSE_MAX)
            logger.info(f"Reading pause ({delay:.0f}s)...")
        
        await asyncio.sleep(delay)

    async def _human_type(self, element, text: str):
        """Type text with human-like delays between keystrokes."""
        for char in text:
            await element.type(char, delay=random.randint(50, 150))
            if random.random() < 0.1:  # 10% chance of micro-pause
                await asyncio.sleep(random.uniform(0.1, 0.3))

    async def _random_mouse_move(self):
        """Move mouse randomly to simulate human behavior."""
        if self.page:
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            await self.page.mouse.move(x, y)

    async def _check_hourly_limit(self) -> bool:
        """Check and reset hourly request count."""
        current_hour = datetime.now().hour
        if current_hour != self.last_hour:
            self.hour_requests = 0
            self.last_hour = current_hour
        
        if self.hour_requests >= REQUESTS_PER_HOUR_SAFE:
            mins_to_next = 60 - datetime.now().minute
            logger.warning(f"Hourly limit ({REQUESTS_PER_HOUR_SAFE}) reached. Waiting {mins_to_next}m...")
            await asyncio.sleep(mins_to_next * 60 + 60)
            self.hour_requests = 0
            return True
        return False

    async def start_browser(self, headless: bool = False):
        """Start browser with persistent session."""
        logger.info("Starting browser...")
        
        pw = await async_playwright().start()
        
        # Use persistent context to maintain session
        self.context = await pw.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=headless,
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='en-US',
            timezone_id='Asia/Karachi',
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        
        # Remove webdriver flag
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        
        await self.page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
        """)
        
        logger.info("Browser started successfully")

    async def close_browser(self):
        """Close browser gracefully."""
        if self.context:
            await self.context.close()
        logger.info("Browser closed")

    async def login(self) -> bool:
        """Login to PLS with human-like behavior."""
        logger.info("Navigating to login page...")
        
        try:
            await self.page.goto(LOGIN_URL, wait_until='networkidle', timeout=60000)
            await asyncio.sleep(random.uniform(1, 2))
            
            # Check if already logged in
            if PLS_USER and PLS_USER in await self.page.content():
                logger.info("Already logged in!")
                return True
            
            # Check for 403
            if "403" in await self.page.title() or "Forbidden" in await self.page.content():
                logger.error("403 Forbidden - IP may be blocked")
                return False
            
            # Find login form
            username_field = await self.page.query_selector('input[name="Login.UserName"]')
            password_field = await self.page.query_selector('input[name="Login.Password"]')
            
            if not username_field or not password_field:
                logger.error("Login form not found")
                return False
            
            # Random mouse movement before typing
            await self._random_mouse_move()
            await asyncio.sleep(random.uniform(0.5, 1))
            
            # Click and type username (human-like)
            await username_field.click()
            await asyncio.sleep(random.uniform(0.2, 0.5))
            await self._human_type(username_field, PLS_USER)
            
            await asyncio.sleep(random.uniform(0.3, 0.8))
            
            # Click and type password
            await password_field.click()
            await asyncio.sleep(random.uniform(0.2, 0.5))
            await self._human_type(password_field, PLS_PASS)
            
            await asyncio.sleep(random.uniform(0.5, 1))
            
            # Find and click submit button
            submit_btn = await self.page.query_selector('input[type="submit"], button[type="submit"]')
            if submit_btn:
                await self._random_mouse_move()
                await asyncio.sleep(random.uniform(0.2, 0.5))
                await submit_btn.click()
            else:
                await self.page.keyboard.press('Enter')
            
            # Wait for navigation
            await self.page.wait_for_load_state('networkidle', timeout=30000)
            await asyncio.sleep(random.uniform(1, 2))
            
            # Verify login success
            content = await self.page.content()
            if PLS_USER in content or 'logout' in content.lower() or 'sign out' in content.lower():
                logger.info("Login successful!")
                return True
            
            logger.error("Login failed - credentials may be wrong or account blocked")
            return False
            
        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    async def enumerate_book_year(self, book: str, year: int) -> list:
        """Enumerate all cases for a book and year."""
        logger.info(f"Enumerating {book} {year}...")
        
        cases = []
        page_num = 1
        
        try:
            # Navigate to citation search
            await self.page.goto(f"{BASE_URL}/Login/CitationSearch", wait_until='networkidle')
            await self._human_delay()
            
            # Select book
            book_select = await self.page.query_selector('select[name="book"], #book')
            if book_select:
                await book_select.select_option(book)
                await asyncio.sleep(random.uniform(0.5, 1))
            
            # Select year
            year_select = await self.page.query_selector('select[name="year"], #year')
            if year_select:
                await year_select.select_option(str(year))
                await asyncio.sleep(random.uniform(0.5, 1))
            
            # Submit search
            search_btn = await self.page.query_selector('input[type="submit"], button[type="submit"]')
            if search_btn:
                await search_btn.click()
                await self.page.wait_for_load_state('networkidle')
            
            # Parse results from all pages
            while True:
                await self._human_delay()
                self.hour_requests += 1
                self.progress.increment_requests()
                
                # Extract case names from current page
                page_cases = await self._extract_case_names()
                if not page_cases:
                    break
                    
                cases.extend(page_cases)
                logger.info(f"  Page {page_num}: found {len(page_cases)} cases (total: {len(cases)})")
                
                # Check for next page
                next_btn = await self.page.query_selector('a.next, .pagination a:has-text("Next"), a:has-text(">")')
                if not next_btn:
                    break
                
                await self._check_hourly_limit()
                await next_btn.click()
                await self.page.wait_for_load_state('networkidle')
                page_num += 1
            
            # Save to progress
            self.progress.mark_enumerated(book, year, cases)
            logger.info(f"Enumerated {len(cases)} cases for {book} {year}")
            
            return cases
            
        except Exception as e:
            logger.error(f"Enumeration error: {e}")
            return cases

    async def _extract_case_names(self) -> list:
        """Extract case names from current page."""
        case_names = []
        
        # Look for case buttons/links with casename attribute
        elements = await self.page.query_selector_all('[casename], [casetypeid], .headNotes, .caseDescription')
        
        for el in elements:
            case_name = await el.get_attribute('casename') or await el.get_attribute('casetypeid')
            if case_name:
                case_names.append(case_name)
        
        return list(set(case_names))

    async def fetch_case(self, case_name: str) -> Optional[dict]:
        """Fetch a single case's full content."""
        if self.progress.is_case_fetched(case_name):
            logger.debug(f"Already fetched: {case_name}")
            return None
        
        logger.info(f"Fetching case: {case_name}")
        
        try:
            await self._check_hourly_limit()
            await self._human_delay()
            
            # Use the GetCaseFile endpoint via JavaScript
            result = await self.page.evaluate(f'''
                async () => {{
                    const formData = new FormData();
                    formData.append('caseName', '{case_name}');
                    
                    const response = await fetch('/Login/GetCaseFile', {{
                        method: 'POST',
                        body: formData,
                        headers: {{ 'X-Requested-With': 'XMLHttpRequest' }}
                    }});
                    
                    return await response.text();
                }}
            ''')
            
            self.hour_requests += 1
            self.progress.increment_requests()
            
            if not result or len(result) < 100:
                logger.warning(f"Empty result for {case_name}")
                return None
            
            # Parse the result
            case_data = self._parse_case_content(result, case_name)
            
            if case_data:
                # Save to file
                case_file = CASES_DIR / f"{case_name}.json"
                with open(case_file, 'w', encoding='utf-8') as f:
                    json.dump(case_data, f, indent=2, ensure_ascii=False)
                
                # Append to JSONL
                self._append_to_jsonl(case_data)
                
                self.progress.mark_case_fetched(case_name)
                logger.info(f"  Saved: {case_data.get('citation', case_name)}")
                
            return case_data
            
        except Exception as e:
            logger.error(f"Error fetching {case_name}: {e}")
            return None

    def _parse_case_content(self, html: str, case_name: str) -> Optional[dict]:
        """Parse case HTML content."""
        try:
            # Try JSON decode first (PLS returns JSON-encoded HTML)
            try:
                html = json.loads(html)
            except:
                pass
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'lxml')
            
            # Extract text content
            text = soup.get_text(separator='\n', strip=True)
            
            # Try to extract citation from text
            citation_match = re.search(r'(\d{4})\s+([A-Z]+(?:\s+[A-Z]+)?)\s+(\d+)', text[:500])
            citation = citation_match.group(0) if citation_match else ""
            
            # Extract title (usually first meaningful line)
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            title = lines[0] if lines else ""
            
            return {
                "caseName": case_name,
                "citation": citation,
                "title": title,
                "content": text,
                "html": html if len(html) < 100000 else "",  # Skip huge HTML
                "fetched_at": datetime.now().isoformat(),
                "char_count": len(text),
            }
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None

    def _append_to_jsonl(self, case_data: dict):
        """Append case to appropriate JSONL file."""
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

    async def fetch_pending_cases(self, limit: int = 50):
        """Fetch pending cases that haven't been fetched yet."""
        unfetched = self.progress.get_unfetched_cases()
        
        if not unfetched:
            logger.info("No pending cases to fetch. Run enumerate first.")
            return
        
        logger.info(f"Found {len(unfetched)} pending cases. Fetching up to {limit}...")
        
        fetched = 0
        for case_name in unfetched[:limit]:
            result = await self.fetch_case(case_name)
            if result:
                fetched += 1
            
            # Random longer break every 10-20 cases
            if fetched > 0 and fetched % random.randint(10, 20) == 0:
                break_time = random.uniform(60, 180)
                logger.info(f"Taking a {break_time/60:.1f} minute break...")
                await asyncio.sleep(break_time)
        
        logger.info(f"Fetched {fetched} cases this session")

    def show_status(self):
        """Show current scraping status."""
        total_enumerated = sum(len(v) for v in self.progress.data.get("enumerated", {}).values())
        fetched = len(self.progress.data.get("fetched_cases", []))
        today_requests = self.progress.get_today_requests()
        
        print("\n" + "=" * 50)
        print("PLS PLAYWRIGHT SCRAPER STATUS")
        print("=" * 50)
        print(f"Cases enumerated: {total_enumerated}")
        print(f"Cases fetched:    {fetched}")
        print(f"Remaining:        {total_enumerated - fetched}")
        print(f"Today's requests: {today_requests}")
        print(f"Session storage:  {SESSION_DIR}")
        print("=" * 50 + "\n")


# ─── CLI ─────────────────────────────────────────────────────────────────────

async def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="PLS Playwright Scraper")
    sub = parser.add_subparsers(dest="command")
    
    sub.add_parser("status", help="Show status")
    sub.add_parser("test-login", help="Test browser login")
    
    enum_p = sub.add_parser("enumerate", help="Enumerate cases")
    enum_p.add_argument("--book", required=True, choices=BOOKS)
    enum_p.add_argument("--year", required=True, type=int)
    
    fetch_p = sub.add_parser("fetch-cases", help="Fetch pending cases")
    fetch_p.add_argument("--limit", type=int, default=50)
    fetch_p.add_argument("--headless", action="store_true", help="Run headless")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    scraper = PLSPlaywrightScraper()
    
    if args.command == "status":
        scraper.show_status()
        return
    
    # Commands that need browser
    headless = getattr(args, 'headless', False)
    
    try:
        await scraper.start_browser(headless=headless)
        
        if args.command == "test-login":
            success = await scraper.login()
            print(f"\nLogin {'SUCCESS' if success else 'FAILED'}")
        
        elif args.command == "enumerate":
            if await scraper.login():
                await scraper.enumerate_book_year(args.book, args.year)
        
        elif args.command == "fetch-cases":
            if await scraper.login():
                await scraper.fetch_pending_cases(limit=args.limit)
        
    finally:
        await scraper.close_browser()


if __name__ == "__main__":
    asyncio.run(main())
