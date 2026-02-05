#!/usr/bin/env python3
"""
PakistanLawSite.com Authenticated Scraper
==========================================
⚠️  Requires credentials in .env file (see .env.example)
Uses discovered API endpoints to extract case law data.
Respects rate limits: 30 requests/day max, random delays.

Endpoints used:
  POST /Login/SearchCaseLaw     — keyword search
  GET  /Login/LoadMoreCaseLaw   — pagination
  POST /Login/GetCaseFile       — full case text
  POST /Login/CitationSearch    — year+book enumeration
  POST /Login/AdvanceSearch     — advanced search

Usage:
  python pls_scraper.py status
  python pls_scraper.py enumerate --book PLD --year 2025
  python pls_scraper.py fetch-cases --limit 10
  python pls_scraper.py search --keyword "murder" --limit 5
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

# Pakistan Standard Time = UTC+5
PKT = timezone(timedelta(hours=5))
SCRAPE_START_HOUR = 7   # 7:00 AM PKT
SCRAPE_END_HOUR = 21    # 9:00 PM PKT

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL = "https://www.pakistanlawsite.com"
LOGIN_URL = f"{BASE_URL}/Login/Check"

# Credentials (from environment)
PLS_USER = os.environ.get("PLS_USER", "")
PLS_PASS = os.environ.get("PLS_PASS", "")

# Rate limiting
DAILY_REQUEST_LIMIT = 500  # Authorized access, scrape responsibly
MIN_DELAY_SECONDS = 8
MAX_DELAY_SECONDS = 15
BREAK_AFTER_REQUESTS = 30
BREAK_DURATION_SECONDS = 120  # 2 minutes

# Data directories
DATA_DIR = Path("data/pakistanlawsite")
CASES_DIR = DATA_DIR / "cases"
HEADNOTES_DIR = DATA_DIR / "headnotes"
HTML_DIR = DATA_DIR / "html"
JSONL_DIR = DATA_DIR / "jsonl"
SEARCH_DIR = DATA_DIR / "search_results"
PROGRESS_FILE = DATA_DIR / "pls_progress.json"

# Available books/journals
BOOKS = [
    "PLD", "SCMR", "MLD", "PCrLJ", "PTD",
    "PLC-Service", "PLC-Labour", "YLR", "CLC", "CLD", "GBLR"
]

# Major courts
MAJOR_COURTS = [
    "SUPREME-COURT",
    "LAHORE-HIGH-COURT-LAHORE",
    "PESHAWAR-HIGH-COURT",
    "ISLAMABAD-HIGH-COURT",
    "QUETTA-HIGH-COURT-BALOCHISTAN",
    "FEDERAL-SHARIAT-COURT",
    "SUPREME-COURT-AZAD-KASHMIR",
    "HIGH-COURT-AZAD-KASHMIR",
    "Gilgit-Baltistan Chief Court",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

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
            "enumerated": {},  # {book_year: [case_names]}
            "fetched_cases": [],  # list of caseName IDs
            "fetched_headnotes": [],
            "total_cases_found": 0,
            "daily_logs": [],
        }

    def save(self):
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.filepath, 'w') as f:
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
        self.data["total_cases_found"] = sum(
            len(v) for v in self.data["enumerated"].values()
        )
        self.save()

    def is_case_fetched(self, case_name: str) -> bool:
        return case_name in self.data["fetched_cases"]

    def mark_case_fetched(self, case_name: str):
        if case_name not in self.data["fetched_cases"]:
            self.data["fetched_cases"].append(case_name)
            self.save()

    def is_headnote_fetched(self, case_name: str) -> bool:
        return case_name in self.data["fetched_headnotes"]

    def mark_headnote_fetched(self, case_name: str):
        if case_name not in self.data["fetched_headnotes"]:
            self.data["fetched_headnotes"].append(case_name)
            self.save()

    def get_unfetched_cases(self) -> list:
        """Return case names that have been enumerated but not yet fetched."""
        all_cases = set()
        for cases in self.data["enumerated"].values():
            all_cases.update(cases)
        fetched = set(self.data["fetched_cases"])
        return list(all_cases - fetched)

    def log_daily(self, action: str, details: dict):
        self.data["daily_logs"].append({
            "date": datetime.now().isoformat(),
            "action": action,
            **details
        })
        self.save()


# ─── PLS Scraper ─────────────────────────────────────────────────────────────

class PLSScraper:
    """Authenticated scraper for pakistanlawsite.com"""

    def __init__(self):
        self.session = requests.Session()
        self.progress = ProgressTracker()
        self.request_count = 0
        self.authenticated = False

        # Create directories
        for d in [DATA_DIR, CASES_DIR, HEADNOTES_DIR, HTML_DIR, JSONL_DIR, SEARCH_DIR]:
            d.mkdir(parents=True, exist_ok=True)

    # ── Auth ──────────────────────────────────────────────────────────────

    def login(self) -> bool:
        """Authenticate with pakistanlawsite.com"""
        logger.info("Logging in to pakistanlawsite.com...")
        try:
            # Step 1: GET the login page to get verification token + cookies
            self.session.headers.update({
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Referer": BASE_URL,
            })

            resp = self.session.get(LOGIN_URL, timeout=30)

            # Parse verification token from the login form
            soup = BeautifulSoup(resp.text, 'lxml')
            login_form = soup.find('form', action='/Login/Login')
            verification_token = None
            if login_form:
                token_input = login_form.find('input', {'name': '__RequestVerificationToken'})
                if token_input:
                    verification_token = token_input.get('value')

            if not verification_token:
                logger.error("Could not find verification token on login page")
                return False

            # Step 2: POST login with correct ASP.NET MVC field names
            login_data = {
                "__RequestVerificationToken": verification_token,
                "Login.UserName": PLS_USER,
                "Login.Password": PLS_PASS,
            }

            resp = self.session.post(
                f"{BASE_URL}/Login/Login",
                data=login_data,
                timeout=30,
                allow_redirects=True
            )

            # Step 3: Verify we got a session cookie
            if 'ASP.NET_SessionId' in {c.name for c in self.session.cookies}:
                self.authenticated = True
                # Add AJAX header for all subsequent requests
                self.session.headers["X-Requested-With"] = "XMLHttpRequest"
                self.session.headers["Referer"] = LOGIN_URL
                logger.info("Login successful! (session cookie acquired)")
                return True

            # Fallback: check if username appears in response
            if PLS_USER in resp.text:
                self.authenticated = True
                self.session.headers["X-Requested-With"] = "XMLHttpRequest"
                self.session.headers["Referer"] = LOGIN_URL
                logger.info("Login successful!")
                return True

            logger.error("Login failed - no session cookie received")
            return False

        except Exception as e:
            logger.error(f"Login error: {e}")
            return False

    # ── Rate Limiting ─────────────────────────────────────────────────────

    def _check_limit(self) -> bool:
        """Check if we can make another request (respects daily limit + PKT hours)."""
        # Check PKT time window (7 AM - 9 PM Pakistan time)
        now_pkt = datetime.now(PKT)
        if not (SCRAPE_START_HOUR <= now_pkt.hour < SCRAPE_END_HOUR):
            logger.warning(
                f"Outside PKT scraping window ({now_pkt.strftime('%H:%M')} PKT). "
                f"Allowed: {SCRAPE_START_HOUR}:00 - {SCRAPE_END_HOUR}:00 PKT."
            )
            return False

        if not self.progress.can_make_request():
            logger.warning(
                f"Daily limit reached ({DAILY_REQUEST_LIMIT}). "
                f"Try again tomorrow."
            )
            return False
        return True

    def _delay(self):
        """Random delay between requests."""
        delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
        logger.debug(f"Waiting {delay:.1f}s...")
        time.sleep(delay)

    def _maybe_break(self):
        """Take a break every N requests."""
        self.request_count += 1
        if self.request_count > 0 and self.request_count % BREAK_AFTER_REQUESTS == 0:
            mins = BREAK_DURATION_SECONDS / 60
            logger.info(f"Taking a {mins:.0f} minute break after {self.request_count} requests...")
            time.sleep(BREAK_DURATION_SECONDS)

    def _request(self, method: str, url: str, data: dict = None,
                 params: dict = None, retries: int = 3) -> Optional[requests.Response]:
        """Make a rate-limited request."""
        if not self._check_limit():
            return None

        self._delay()
        self._maybe_break()

        # Rotate user agent
        self.session.headers["User-Agent"] = random.choice(USER_AGENTS)

        for attempt in range(retries):
            try:
                if method == "GET":
                    resp = self.session.get(url, params=params, timeout=30)
                else:
                    resp = self.session.post(url, data=data, timeout=30)

                if resp.status_code == 429:
                    logger.warning("Rate limited! Waiting 10 minutes...")
                    time.sleep(600)
                    continue

                if resp.status_code == 403:
                    logger.warning("403 Forbidden — session may have expired. Re-authenticating...")
                    self.login()
                    continue

                resp.raise_for_status()
                self.progress.increment_requests()
                return resp

            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(15 * (attempt + 1))

        logger.error(f"Failed after {retries} attempts: {url}")
        return None

    # ── Parsing ───────────────────────────────────────────────────────────

    def _parse_search_results(self, html: str) -> list:
        """Parse case law search results HTML into structured data.
        Handles two formats:
        1. caseLawTable format (from SearchCaseLaw)
        2. DataTable format (from CitationSearch)
        """
        soup = BeautifulSoup(html, 'lxml')
        cases = []

        # ── Format 1: caseLawTable (from keyword search) ──
        cl_tables = soup.find_all('table', class_='caseLawTable')
        if cl_tables:
            for table in cl_tables:
                case = {}
                rows = table.find_all('tr')
                for row in rows:
                    text = row.get_text(strip=True)

                    if 'Citation Name:' in text:
                        citation = text.replace('Citation Name:', '').strip()
                        citation = citation.replace('Bookmark this Case', '').strip()
                        case['citation'] = citation
                        parts = citation.split()
                        if len(parts) >= 3:
                            case['year'] = parts[0]
                            case['book'] = parts[1]
                            case['number'] = parts[2]
                            if len(parts) >= 4:
                                case['court'] = ' '.join(parts[3:])

                    elif 'VS' in text and 'Citation' not in text and 'Head Notes' not in text:
                        if not text.startswith('Ss.') and len(text) < 200:
                            case['parties'] = text

                head_btn = table.find('input', class_='headNotes')
                if head_btn:
                    case['caseName'] = head_btn.get('casename', '')
                else:
                    desc_btn = table.find('input', class_='caseDescription')
                    if desc_btn:
                        case['caseName'] = desc_btn.get('casename', '')

                for row in rows:
                    text = row.get_text(strip=True)
                    if text.startswith('Ss.') or text.startswith('S.') or '---' in text[:50]:
                        case.setdefault('headnote_preview', text[:500])

                if case.get('caseName') or case.get('citation'):
                    cases.append(case)

            return cases

        # ── Format 2: DataTable (from CitationSearch / CourtWise) ──
        data_table = soup.find('table', id='archivedpatientGrid')
        if not data_table:
            data_table = soup.find('table', class_='dataTable')

        if data_table:
            rows = data_table.find_all('tr', class_='caseType')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue

                case = {}
                case['index'] = cells[0].get_text(strip=True)

                # Citation cell
                citation_text = cells[1].get_text(strip=True)
                case['citation'] = citation_text
                parts = citation_text.split()
                if len(parts) >= 3:
                    case['year'] = parts[0]
                    case['book'] = parts[1]
                    case['number'] = parts[2]

                # Title cell (parties + judge + lawyers)
                title_cell = cells[2]
                # Extract parties (text before <br>)
                title_text = title_cell.get_text(separator='|', strip=True)
                title_parts = title_text.split('|')
                if title_parts:
                    case['parties'] = title_parts[0].strip()

                # Extract judge
                judge_span = title_cell.find('span', style=re.compile(r'darkred'))
                if judge_span:
                    case['judge'] = judge_span.get_text(strip=True).replace('Honorable Justice ', '')

                # Extract lawyers
                lawyer_span = title_cell.find('span', style=re.compile(r'darkblue'))
                if lawyer_span:
                    case['lawyers'] = lawyer_span.get_text(strip=True).replace(' , ', ', ')

                # Court cell
                case['court'] = cells[3].get_text(strip=True)

                # casetypeid from Read button
                read_btn = cells[4].find('input', class_='courtWiseSearchBtn')
                if read_btn:
                    case['caseTypeId'] = read_btn.get('casetypeid', '')
                    # Use caseTypeId as caseName for fetching
                    case['caseName'] = read_btn.get('casetypeid', '')

                if case.get('caseName') or case.get('citation'):
                    cases.append(case)

            return cases

        return cases

    def _parse_total_results(self, html: str) -> int:
        """Extract total result count from search page."""
        soup = BeautifulSoup(html, 'lxml')
        # Look for "Your Search returned total XXXX records"
        text = soup.get_text()
        match = re.search(r'total\s+(\d+)\s+records', text, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return 0

    def _parse_case_content(self, response_text: str) -> dict:
        """Parse the case file response into structured text.
        The response from GetCaseFile is a JSON-encoded string containing
        MS Word HTML. We decode the JSON first, then parse the HTML.
        """
        # Step 1: Decode JSON string (the response is a JSON string literal)
        html = response_text
        if html.startswith('"') and html.endswith('"'):
            try:
                html = json.loads(html)
            except json.JSONDecodeError:
                pass

        # Step 2: Parse HTML
        soup = BeautifulSoup(html, 'lxml')

        # Remove script/style tags
        for tag in soup(['script', 'style']):
            tag.decompose()

        # Extract title from <title> tag
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ''

        content = {
            'raw_html': html,
            'title': title,
            'text': soup.get_text(separator='\n', strip=True),
        }

        text = content['text']

        # Find judges (Present: X and Y, JJ)
        judge_match = re.search(r'Present[:\s]*(.+?JJ?\.?)', text, re.IGNORECASE)
        if judge_match:
            content['judges'] = judge_match.group(1).strip()

        # Find date of hearing
        date_match = re.search(
            r'(?:Date of hearing|decided on|Decided on)[:\s]*(.+?)(?:\n|$)',
            text, re.IGNORECASE
        )
        if date_match:
            content['date'] = date_match.group(1).strip().rstrip('.')

        # Find case number
        case_match = re.search(
            r'((?:Civil|Criminal|Constitutional|Writ)\s+(?:Petition|Appeal|Review|Misc\.?)\s*No\.?\s*[\d\-/]+(?:\s*(?:of|\/)\s*\d{4})?)',
            text, re.IGNORECASE
        )
        if case_match:
            content['case_number'] = case_match.group(1).strip()

        return content

    # ── Search Methods ────────────────────────────────────────────────────

    def search_caselaw(self, keyword: str, court: str = "",
                       year_range: int = 200) -> list:
        """Search case law by keyword."""
        logger.info(f"Searching case law: '{keyword}' (court={court or 'all'}, range={year_range}yr)")

        current_year = datetime.now().year
        start_year = current_year - year_range

        resp = self._request("POST", f"{BASE_URL}/Login/SearchCaseLaw", data={
            "year": start_year,
            "book": keyword,
            "code": "",
            "court": court,
            "searchType": "caselaw",
            "judge": "",
            "lawyer": "",
            "party": "",
        })

        if not resp:
            return []

        total = self._parse_total_results(resp.text)
        cases = self._parse_search_results(resp.text)

        logger.info(f"Found {total} total results, parsed {len(cases)} from first page")

        return cases

    def citation_search(self, year: int, book: str, court: str = "",
                        code: str = "") -> list:
        """Search by citation (year + book)."""
        logger.info(f"Citation search: {year} {book} {court or '(all courts)'}")

        resp = self._request("POST", f"{BASE_URL}/Login/CitationSearch", data={
            "year": year,
            "book": book,
            "code": code,
            "court": court,
            "judge": "",
            "lawyer": "",
            "party": "",
        })

        if not resp:
            return []

        cases = self._parse_search_results(resp.text)
        total = self._parse_total_results(resp.text)

        logger.info(f"Citation search {year} {book}: {total} total, {len(cases)} parsed")
        return cases

    def load_more_caselaw(self, keyword: str, court: str = "",
                          offset: int = 50, year: int = None) -> list:
        """Load more search results (pagination)."""
        if year is None:
            year = datetime.now().year - 200

        resp = self._request("GET", f"{BASE_URL}/Login/LoadMoreCaseLaw", params={
            "book": keyword,
            "court": court,
            "row": offset,
            "year": year,
            "caseTypeId": 0,
        })

        if not resp or resp.text.strip() == "-1":
            return []

        return self._parse_search_results(resp.text)

    # ── Case Fetching ─────────────────────────────────────────────────────

    def get_case_file(self, case_name: str, head_notes_only: bool = False) -> dict:
        """Fetch full case content by caseName ID."""
        logger.info(f"Fetching case: {case_name} ({'headnotes' if head_notes_only else 'full text'})")

        resp = self._request("POST", f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": case_name,
            "headNotes": 1 if head_notes_only else 0,
        })

        if not resp or resp.text.strip() == "1" or resp.text.strip() == '"1"':
            logger.warning(f"Failed to fetch case {case_name}")
            return {}

        content = self._parse_case_content(resp.text)
        content['caseName'] = case_name
        content['fetched_at'] = datetime.now().isoformat()
        content['type'] = 'headnotes' if head_notes_only else 'full'

        # Log content stats
        text_len = len(content.get('text', ''))
        logger.info(f"  Got {text_len} chars | title: {content.get('title', '?')[:60]}")

        return content

    # ── JSONL Helpers ──────────────────────────────────────────────────────

    def _get_case_book_year(self, case_name: str) -> tuple:
        """Look up which book_year key contains this case in the enumerated dict.
        Returns (book, year) or (None, None) if not found.
        """
        for key, case_names in self.progress.data["enumerated"].items():
            if case_name in case_names:
                parts = key.rsplit("_", 1)
                if len(parts) == 2:
                    return parts[0], int(parts[1])
        return None, None

    def _append_to_jsonl(self, book: str, year: int, case_data: dict):
        """Append one case record as a JSON line to the appropriate JSONL file."""
        jsonl_path = JSONL_DIR / f"cases_{book}_{year}.jsonl"
        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(case_data, ensure_ascii=False) + "\n")
        logger.info(f"  Appended to {jsonl_path.name}")

    # ── High-Level Operations ─────────────────────────────────────────────

    def enumerate_book_year(self, book: str, year: int) -> list:
        """
        Enumerate all cases for a given book and year.
        Uses citation search + pagination.
        """
        key = f"{book}_{year}"
        existing = self.progress.data["enumerated"].get(key, [])
        if existing:
            logger.info(f"Already enumerated {key}: {len(existing)} cases")
            return existing

        all_cases = []
        page_cases = self.citation_search(year, book)

        if not page_cases:
            logger.info(f"No cases found for {year} {book}")
            return []

        all_cases.extend(page_cases)

        # Paginate if there are more
        offset = 50
        while len(page_cases) >= 10:  # If we got a substantial page, try next
            if not self._check_limit():
                break
            page_cases = self.load_more_caselaw(
                keyword=book, offset=offset, year=year
            )
            if not page_cases:
                break
            all_cases.extend(page_cases)
            offset += 50

        # Extract unique case names
        case_names = list(set(
            c['caseName'] for c in all_cases if c.get('caseName')
        ))

        # Save enumeration
        self.progress.mark_enumerated(book, year, case_names)

        # Save full search results
        results_file = SEARCH_DIR / f"{book}_{year}.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(all_cases, f, indent=2, ensure_ascii=False)

        logger.info(f"Enumerated {book} {year}: {len(case_names)} unique cases")
        return case_names

    def fetch_pending_cases(self, limit: int = 10, headnotes_first: bool = True):
        """Fetch full text for cases that have been enumerated but not fetched.
        Saves combined headnotes+judgment as a single JSONL line per case.
        Also saves raw HTML as backup.
        """
        unfetched = self.progress.get_unfetched_cases()

        if not unfetched:
            logger.info("No pending cases to fetch!")
            return

        logger.info(f"{len(unfetched)} cases pending. Fetching up to {limit}...")
        fetched_count = 0

        for case_name in unfetched[:limit]:
            if not self._check_limit():
                break

            # Skip if already fetched
            if self.progress.is_case_fetched(case_name):
                continue

            # Determine book and year for this case
            book, year = self._get_case_book_year(case_name)
            if not book or not year:
                logger.warning(f"  Cannot determine book/year for {case_name}, skipping")
                continue

            headnotes_text = ""
            headnotes_content = {}

            # Step 1: Fetch headnotes first (if enabled and not already fetched)
            if headnotes_first and not self.progress.is_headnote_fetched(case_name):
                headnotes_content = self.get_case_file(case_name, head_notes_only=True)
                if headnotes_content and headnotes_content.get('text'):
                    headnotes_text = headnotes_content.get('text', '')
                    self.progress.mark_headnote_fetched(case_name)
                if not self._check_limit():
                    break

            # Step 2: Fetch full case text
            content = self.get_case_file(case_name, head_notes_only=False)
            if not content or not content.get('text'):
                logger.warning(f"  Failed to fetch full text for {case_name}")
                continue

            # Save raw HTML as backup
            html_file = HTML_DIR / f"{case_name}.html"
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(content.get('raw_html', ''))

            # Build combined JSONL record
            record = {
                "id": case_name,
                "book": book,
                "year": year,
                "court": content.get("court", ""),
                "parties": content.get("parties", ""),
                "judges": content.get("judges", ""),
                "date": content.get("date", ""),
                "case_number": content.get("case_number", ""),
                "title": content.get("title", ""),
                "headnotes": headnotes_text,
                "judgment": content.get("text", ""),
                "text": content.get("text", ""),
                "scraped_at": datetime.now().isoformat(),
            }

            # Append to JSONL file
            self._append_to_jsonl(book, year, record)

            # Mark as fetched in progress tracker
            self.progress.mark_case_fetched(case_name)
            fetched_count += 1

        self.progress.log_daily("fetch_cases", {
            "fetched": fetched_count,
            "remaining": len(unfetched) - fetched_count,
        })

        logger.info(f"Fetched {fetched_count} cases. {len(unfetched) - fetched_count} remaining.")

    # ── Status ────────────────────────────────────────────────────────────

    def show_status(self):
        """Show current progress."""
        p = self.progress.data
        today_reqs = self.progress.get_today_requests()
        unfetched = len(self.progress.get_unfetched_cases())

        print()
        print("=" * 60)
        print("  PAKISTANLAWSITE.COM SCRAPER - STATUS")
        print("=" * 60)
        print()
        print(f"  Today's requests:   {today_reqs}/{DAILY_REQUEST_LIMIT}")
        print(f"  Total cases found:  {p.get('total_cases_found', 0)}")
        print(f"  Headnotes fetched:  {len(p.get('fetched_headnotes', []))}")
        print(f"  Full cases fetched: {len(p.get('fetched_cases', []))}")
        print(f"  Pending to fetch:   {unfetched}")
        print()

        # Show enumerated books
        enumerated = p.get("enumerated", {})
        if enumerated:
            print("  Enumerated books/years:")
            for key, cases in sorted(enumerated.items()):
                print(f"    {key}: {len(cases)} cases")
        else:
            print("  No books enumerated yet.")

        print()

        # Recent activity
        logs = p.get("daily_logs", [])[-5:]
        if logs:
            print("  Recent activity:")
            for log in logs:
                d = log.get('date', '?')[:16]
                action = log.get('action', '?')
                details = {k:v for k,v in log.items() if k not in ('date','action')}
                print(f"    {d} - {action}: {json.dumps(details)}")

        print()
        print("=" * 60)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="PakistanLawSite.com Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pls_scraper.py status
  python pls_scraper.py enumerate --book PLD --year 2025
  python pls_scraper.py enumerate --book SCMR --year 2024
  python pls_scraper.py fetch-cases --limit 5
  python pls_scraper.py search --keyword "constitutional petition"
  python pls_scraper.py run-daily
        """
    )

    sub = parser.add_subparsers(dest="command", help="Command to run")

    # status
    sub.add_parser("status", help="Show current progress")

    # enumerate
    enum_p = sub.add_parser("enumerate", help="Enumerate cases for a book+year")
    enum_p.add_argument("--book", required=True, choices=BOOKS, help="Journal/book code")
    enum_p.add_argument("--year", required=True, type=int, help="Year (e.g., 2025)")

    # fetch-cases
    fetch_p = sub.add_parser("fetch-cases", help="Fetch full text for pending cases")
    fetch_p.add_argument("--limit", type=int, default=10, help="Max cases to fetch")
    fetch_p.add_argument("--headnotes-only", action="store_true",
                         help="Only fetch headnotes, not full text")

    # search
    search_p = sub.add_parser("search", help="Search case law by keyword")
    search_p.add_argument("--keyword", required=True, help="Search keyword")
    search_p.add_argument("--court", default="", help="Filter by court")
    search_p.add_argument("--limit", type=int, default=50, help="Max results")

    # run-daily
    daily_p = sub.add_parser("run-daily", help="Run daily scrape routine")
    daily_p.add_argument("--book", default=None, help="Specific book to enumerate")
    daily_p.add_argument("--year", type=int, default=None, help="Specific year")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    scraper = PLSScraper()

    if args.command == "status":
        scraper.show_status()
        return

    # All other commands need authentication
    if not scraper.login():
        logger.error("Cannot proceed without authentication.")
        sys.exit(1)

    if args.command == "enumerate":
        cases = scraper.enumerate_book_year(args.book, args.year)
        print(f"\nFound {len(cases)} cases for {args.book} {args.year}")
        if cases:
            print("Sample case names:")
            for c in cases[:10]:
                print(f"  - {c}")

    elif args.command == "fetch-cases":
        scraper.fetch_pending_cases(
            limit=args.limit,
            headnotes_first=not args.headnotes_only
        )

    elif args.command == "search":
        cases = scraper.search_caselaw(args.keyword, args.court)
        print(f"\nFound {len(cases)} cases:")
        for c in cases[:args.limit]:
            print(f"  [{c.get('caseName', '?')}] {c.get('citation', '')} — {c.get('parties', '')}")

    elif args.command == "run-daily":
        # Smart daily routine
        today_reqs = scraper.progress.get_today_requests()
        remaining = DAILY_REQUEST_LIMIT - today_reqs

        if remaining <= 0:
            logger.info("Daily limit already reached. Try again tomorrow.")
            return

        logger.info(f"Starting daily routine. {remaining} requests remaining today.")

        # Priority 1: Fetch pending cases (use 2/3 of budget)
        fetch_budget = max(1, (remaining * 2) // 3)
        unfetched = scraper.progress.get_unfetched_cases()
        if unfetched:
            logger.info(f"Fetching up to {fetch_budget} pending cases...")
            scraper.fetch_pending_cases(limit=fetch_budget)

        # Priority 2: Enumerate new book/year combos (use remaining budget)
        remaining = DAILY_REQUEST_LIMIT - scraper.progress.get_today_requests()
        if remaining > 0:
            book = args.book or random.choice(BOOKS)
            year = args.year or datetime.now().year
            logger.info(f"Enumerating {book} {year} with {remaining} requests...")
            scraper.enumerate_book_year(book, year)

        scraper.show_status()


if __name__ == "__main__":
    main()
