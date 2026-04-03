#!/usr/bin/env python3
"""
Historical Case Law Scraper
============================
Dedicated scraper for pre-2015 cases where the main scraper's session expires.

Key differences from pls_scraper_v2.py:
- Fresh login before EACH year (prevents session expiry)
- Session health check before each reporter
- Detects login redirects and auto-re-authenticates
- Uses same CitationSearch endpoint + fetch_case logic
- Saves in all 4 formats: JSON, Original HTML, Readable HTML, JSONL

Usage:
    python historical_scraper.py --from-year 2014 --to-year 2010
    python historical_scraper.py --year 2014
    python historical_scraper.py --year 2014 --reporter SCMR
"""

import os
import re
import sys
import json
import time
import random
import hashlib
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
PROGRESS_FILE = DATA_DIR / "historical_progress.json"
LOG_DIR = Path(__file__).parent / "logs"

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Delays (optimized Feb 13 — removed redundant stacking)
MIN_DELAY = 2.0
MAX_DELAY = 4.0
READING_DELAY_MIN = 1.0
READING_DELAY_MAX = 2.0
BREAK_INTERVAL = 50  # requests before break
BREAK_MIN = 20
BREAK_MAX = 60  # longer breaks, fewer of them — more human-like

# ══════════════════════════════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════════════════════════════

LOG_DIR.mkdir(parents=True, exist_ok=True)

# Reconfigure stdout for Windows compatibility
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

logger = logging.getLogger("historical_scraper")
logger.setLevel(logging.INFO)

# Console handler
ch = logging.StreamHandler(sys.stderr)
ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S"))
logger.addHandler(ch)

# File handler
fh = logging.FileHandler(LOG_DIR / "historical_stderr.log", encoding='utf-8')
fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
logger.addHandler(fh)


@dataclass
class CaseData:
    citation: str
    case_name: str
    year: int
    reporter: str
    page: str
    court: str
    judges: str
    judgment: str
    judgment_raw: str
    date_decided: str
    headnotes: str
    statutes_cited: List[str]
    cases_cited: List[str]
    fetched_at: str


class HistoricalScraper:
    def __init__(self):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.requests_since_break = 0
        self.last_request_time = 0
        self.progress = self._load_progress()
        self._jsonl_sets: Dict[str, set] = {}
        
        DATA_DIR.mkdir(parents=True, exist_ok=True)

    def _create_session(self) -> Session:
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
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

    def _human_delay(self, reading=False):
        if reading:
            delay = random.uniform(READING_DELAY_MIN, READING_DELAY_MAX)
        else:
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
        delay += random.gauss(0, 0.3)
        delay = max(1.0, delay)
        time.sleep(delay)

    def _maybe_break(self):
        self.requests_since_break += 1
        if self.requests_since_break >= BREAK_INTERVAL:
            dur = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"Taking {dur:.0f}s break...")
            time.sleep(dur)
            self.requests_since_break = 0

    def _request(self, method: str, url: str, retries: int = 3, **kwargs):
        self._maybe_break()
        
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY:
            time.sleep(MIN_DELAY - elapsed)

        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, timeout=15, **kwargs)
                else:
                    resp = self.session.post(url, timeout=15, **kwargs)

                self.last_request_time = time.time()
                self.request_count += 1

                # Detect session expiry — PLS redirects to login page
                if resp.status_code == 200 and self._is_login_page(resp.text):
                    logger.warning(f"Session expired! Re-authenticating...")
                    self.logged_in = False
                    if self.login():
                        # Retry the original request
                        continue
                    return None

                if resp.status_code in (403, 429):
                    backoff = 30 * (attempt + 1)
                    logger.warning(f"{resp.status_code} - backing off {backoff}s")
                    time.sleep(backoff)
                    continue

                if resp.status_code == 503:
                    backoff = 30 * (attempt + 1)
                    logger.warning(f"503 Service Unavailable - backing off {backoff}s (attempt {attempt+1})")
                    time.sleep(backoff)
                    continue

                if resp.status_code == 500:
                    logger.warning(f"500 Server Error (attempt {attempt+1})")
                    time.sleep(15)
                    continue

                if resp.status_code != 200:
                    logger.warning(f"Status {resp.status_code} for {url}")
                    return None

                return resp

            except Exception as e:
                logger.error(f"Request failed (attempt {attempt+1}): {e}")
                time.sleep(10 * (attempt + 1))

        logger.error(f"All {retries} attempts failed for {url}")
        return None

    def _is_login_page(self, html: str) -> bool:
        """Detect if PLS returned the login page instead of data."""
        indicators = [
            'id="txtLoginname"', 'id="txtLoginpassword"', 'Login/LoginCheck',
            'id="username"', 'id="loginPass"', 'Login.UserName', 'Login.Password',
            'name="Login.UserName"',
        ]
        return any(ind in html for ind in indicators)

    def login(self, max_attempts: int = 3) -> bool:
        """Login to PLS with fresh session (ClearLoginHistory flow).
        
        The new PLS site uses ClearLoginHistory which both clears old sessions
        AND logs in the current session. After calling it, we check /Login/Check
        to verify we're logged in. Only fall back to Login/Login if needed.
        """
        username = os.getenv("PLS_USER", "")
        password = os.getenv("PLS_PASS", "")
        if not username or not password:
            logger.error("PLS_USER/PLS_PASS not set in .env")
            return False

        for attempt in range(1, max_attempts + 1):
            # Always create fresh session per attempt
            self.session = self._create_session()
            self.logged_in = False
            self.requests_since_break = 0

            logger.info(f"Logging in to PLS (attempt {attempt}/{max_attempts})...")

            try:
                # 1. Load homepage to get CSRF token
                resp = self.session.get(f"{BASE_URL}/", timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"Homepage returned {resp.status_code} (attempt {attempt})")
                    time.sleep(10 * attempt)
                    continue

                time.sleep(random.uniform(2, 4))

                # 2. Extract CSRF token
                csrf_match = re.search(
                    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                    resp.text
                )
                if not csrf_match:
                    logger.warning(f"CSRF token not found (attempt {attempt})")
                    time.sleep(10 * attempt)
                    continue

                csrf_token = csrf_match.group(1)
                time.sleep(random.uniform(2, 3))

                # 3. ClearLoginHistory — this clears old sessions AND logs us in
                logger.info("  Clearing login history (also logs in)...")
                clear_resp = self.session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
                    "Login.UserName": username,
                    "Login.Password": password,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)

                time.sleep(random.uniform(2, 3))

                # 4. Check if ClearLoginHistory logged us in
                check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check_resp and check_resp.status_code == 200 and "Logout" in check_resp.text:
                    self.logged_in = True
                    logger.info("[OK] Login successful (via ClearLoginHistory)")
                    return True

                # 5. If not logged in yet, try explicit Login/Login
                logger.info("  ClearLoginHistory didn't log in, trying Login/Login...")
                
                # Get fresh CSRF
                resp2 = self.session.get(f"{BASE_URL}/", timeout=30)
                csrf_match2 = re.search(
                    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                    resp2.text
                )
                csrf_token = csrf_match2.group(1) if csrf_match2 else csrf_token
                time.sleep(random.uniform(1, 2))

                login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
                    "Login.UserName": username,
                    "Login.Password": password,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)

                if not login_resp:
                    logger.warning(f"Login POST returned None (attempt {attempt})")
                    time.sleep(10 * attempt)
                    continue

                # Check login response
                if "Logout" in login_resp.text:
                    self.logged_in = True
                    logger.info("[OK] Login successful (via Login/Login)")
                    return True

                # Handle "Account Already In Use" — shouldn't happen after ClearLoginHistory
                if "Account Already In Use" in login_resp.text:
                    logger.warning("Account still in use after ClearLoginHistory, waiting...")
                    time.sleep(10 * attempt)
                    continue

                # Final check via /Login/Check
                time.sleep(2)
                check2 = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check2 and "Logout" in check2.text:
                    self.logged_in = True
                    logger.info("[OK] Login successful (verified via /Login/Check)")
                    return True

                logger.warning(f"Login verification failed (attempt {attempt})")
                time.sleep(10 * attempt)

            except Exception as e:
                logger.warning(f"Login error (attempt {attempt}): {e}")
                time.sleep(10 * attempt)

        logger.error(f"All {max_attempts} login attempts failed")
        return False

    def check_session(self) -> bool:
        """Quick session health check."""
        if not self.logged_in or not self.session:
            return False
        try:
            resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=10, allow_redirects=False)
            if resp.status_code in (301, 302):
                logger.warning("Session dead (redirect), need re-login")
                self.logged_in = False
                return False
            if resp.status_code == 200:
                if self._is_login_page(resp.text) or "Logout" not in resp.text:
                    logger.warning("Session dead (no Logout), need re-login")
                    self.logged_in = False
                    return False
            return True
        except:
            return False

    def citation_search(self, year: int, reporter: str) -> List[Dict]:
        """Search for cases on PLS by year/reporter."""
        if not self.logged_in:
            if not self.login():
                return []

        logger.info(f"Searching: {year} {reporter}")
        # _request() handles delay between calls

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
            logger.warning(f"  No response for {year} {reporter}")
            return []

        cases = self._parse_search_results(resp.text)
        logger.info(f"  Found {len(cases)} cases for {year} {reporter}")
        return cases

    def _parse_search_results(self, html: str) -> List[Dict]:
        """Parse case listings from CitationSearch results."""
        cases = []
        soup = BeautifulSoup(html, 'html.parser')

        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                citation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                btn = row.find('input', attrs={'casetypeid': True})
                case_id = btn.get('casetypeid', '') if btn else ""

                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    case_name_el = cells[0].get_text(strip=True) if cells else ""
                    cases.append({
                        "citation": citation,
                        "case_name": case_id or case_name_el,
                    })

        # Fallback regex
        if not cases:
            citations = re.findall(r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', html)
            case_ids = re.findall(r'casetypeid="([^"]+)"', html)
            for i, cit in enumerate(citations):
                cid = case_ids[i] if i < len(case_ids) else ""
                cases.append({"citation": cit, "case_name": cid})

        # Deduplicate
        seen = set()
        unique = []
        for c in cases:
            if c["citation"] not in seen:
                seen.add(c["citation"])
                unique.append(c)
        return unique

    def fetch_case(self, case_name: str, citation: str) -> Optional[CaseData]:
        """Fetch full case details from PLS using GetCaseFile endpoint."""
        # _request() already enforces MIN_DELAY between calls — no extra delay needed

        resp = self._request("POST", f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": case_name,
            "headNotes": 0,
        })

        if not resp or resp.text.strip() in ["1", '"1"', ""] or len(resp.text) < 100:
            return None

        html = resp.text
        # PLS API returns HTML wrapped as JSON string ("\u003chtml...\u003c/html\u003e")
        # Decode it to get actual HTML with proper < > tags
        if html.startswith('"'):
            try:
                html = json.loads(html)
            except (json.JSONDecodeError, ValueError):
                pass
        soup = BeautifulSoup(html, 'html.parser')

        # Extract metadata
        parts = citation.split()
        year = int(parts[0]) if parts else 0
        reporter = parts[1] if len(parts) > 1 else ""
        page = parts[2] if len(parts) > 2 else ""

        # Extract fields
        court = ""
        judges = ""
        date_decided = ""
        headnotes = ""

        for bold in soup.find_all('b'):
            text = bold.get_text(strip=True).lower()
            next_text = bold.next_sibling
            if next_text and hasattr(next_text, 'strip'):
                next_text = str(next_text).strip().lstrip(':').strip()
            elif next_text and hasattr(next_text, 'get_text'):
                next_text = next_text.get_text(strip=True).lstrip(':').strip()
            else:
                next_text = ""

            if 'court' in text and not court:
                court = next_text
            elif 'judge' in text or 'before' in text:
                judges = next_text
            elif 'date' in text and 'decided' in text:
                date_decided = next_text

        # Headnotes
        hn_div = soup.find('div', class_='headnotes') or soup.find('div', id='headnotes')
        if hn_div:
            headnotes = hn_div.get_text(separator='\n', strip=True)

        # Judgment text
        judgment_div = soup.find('div', class_='judgmentText') or soup.find('div', id='judgmentText')
        if not judgment_div:
            judgment_div = soup.find('div', class_='CaseType')
        
        judgment_raw = str(judgment_div) if judgment_div else html
        judgment_clean = judgment_div.get_text(separator='\n', strip=True) if judgment_div else soup.get_text(separator='\n', strip=True)

        # Extract cited statutes and cases
        statutes_cited = list(set(re.findall(
            r'(?:(?:Section|S\.|Ss\.)\s+[\d\w]+\s+(?:of|,)\s+)?'
            r'(?:the\s+)?([A-Z][a-z][\w\s,]+(?:Act|Ordinance|Order|Rules?|Code|Regulation)\s*,?\s*\d{4})',
            judgment_clean
        )))
        cases_cited = list(set(re.findall(
            r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)',
            judgment_clean
        )))

        return CaseData(
            citation=citation,
            case_name=case_name,
            year=year,
            reporter=reporter,
            page=page,
            court=court,
            judges=judges,
            judgment=judgment_clean,
            judgment_raw=judgment_raw,
            date_decided=date_decided,
            headnotes=headnotes,
            statutes_cited=statutes_cited,
            cases_cited=cases_cited,
            fetched_at=datetime.now(timezone.utc).isoformat(),
        )

    def save_case(self, case: CaseData):
        """Save case in all 4 formats: JSON, Original HTML, Readable HTML, JSONL."""
        safe_citation = re.sub(r'[^\w\-]', '_', case.citation)

        # 1. JSON
        case_dir = DATA_DIR / case.reporter / str(case.year)
        case_dir.mkdir(parents=True, exist_ok=True)
        json_path = case_dir / f"{safe_citation}.json"
        json_path.write_text(json.dumps(asdict(case), indent=2, ensure_ascii=False), encoding='utf-8')

        # 2. Original HTML
        orig_dir = case_dir / "original"
        orig_dir.mkdir(exist_ok=True)
        (orig_dir / f"{safe_citation}.html").write_text(case.judgment_raw, encoding='utf-8')

        # 3. Readable HTML
        html_dir = DATA_DIR / "html" / case.reporter / str(case.year)
        html_dir.mkdir(parents=True, exist_ok=True)
        readable = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{case.citation}</title>
<style>body{{font-family:Georgia,serif;max-width:800px;margin:40px auto;padding:20px;line-height:1.6;color:#333}}
h1{{font-size:1.4rem;border-bottom:2px solid #333;padding-bottom:10px}}
.meta{{background:#f5f5f5;padding:15px;border-radius:5px;margin:15px 0}}
.headnotes{{border-left:3px solid #666;padding-left:15px;margin:20px 0;color:#555}}</style></head>
<body><h1>{case.citation}</h1>
<div class="meta"><b>Court:</b> {case.court}<br><b>Judges:</b> {case.judges}<br>
<b>Date:</b> {case.date_decided}</div>
{f'<div class="headnotes"><h3>Headnotes</h3>{case.headnotes}</div>' if case.headnotes else ''}
<div class="judgment">{case.judgment_raw}</div></body></html>"""
        (html_dir / f"{safe_citation}.html").write_text(readable, encoding='utf-8')

        # 4. JSONL (append)
        jsonl_path = DATA_DIR / f"{case.reporter}_{case.year}.jsonl"
        jsonl_key = str(jsonl_path)
        if jsonl_key not in self._jsonl_sets:
            self._jsonl_sets[jsonl_key] = set()
            if jsonl_path.exists():
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        m = re.search(r'"citation":\s*"([^"]+)"', line)
                        if m:
                            self._jsonl_sets[jsonl_key].add(m.group(1))

        if case.citation not in self._jsonl_sets[jsonl_key]:
            with open(jsonl_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(asdict(case), ensure_ascii=False) + '\n')
            self._jsonl_sets[jsonl_key].add(case.citation)

        # 5. Master JSONL
        master = DATA_DIR / "all_cases.jsonl"
        with open(master, 'a', encoding='utf-8') as f:
            f.write(json.dumps(asdict(case), ensure_ascii=False) + '\n')

    def _load_progress(self) -> Dict:
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
            except:
                pass
        return {"completed": [], "cases_fetched": [], "stats": {}}

    def _save_progress(self):
        self.progress["last_updated"] = datetime.now().isoformat()
        PROGRESS_FILE.write_text(json.dumps(self.progress, indent=2, ensure_ascii=False), encoding='utf-8')

    def verify_year(self, year: int):
        """Verify a completed year against PLS — fresh login, compare counts."""
        try:
            # Fresh login for verification
            if not self.login():
                logger.error(f"Cannot login for verification of {year}")
                return

            logger.info(f"Verifying {year} against PLS...")
            total_pls = 0
            total_local = 0
            total_missing = 0
            results = {}

            for reporter in REPORTERS:
                # Get PLS count
                cases = self.citation_search(year, reporter)
                pls_count = len(cases)
                total_pls += pls_count

                # Get local count
                local_dir = DATA_DIR / reporter / str(year)
                local_count = len(list(local_dir.glob("*.json"))) if local_dir.exists() else 0
                total_local += local_count

                missing = max(0, pls_count - local_count)
                total_missing += missing

                results[reporter] = {
                    "pls": pls_count,
                    "local": local_count,
                    "missing": missing,
                }

                if missing > 0:
                    logger.info(f"  {reporter} {year}: {local_count}/{pls_count} ({missing} missing)")

            # Summary
            coverage = (total_local / total_pls * 100) if total_pls > 0 else 0
            logger.info(f"VERIFICATION {year}: {total_local}/{total_pls} ({coverage:.1f}%) — {total_missing} missing")

            # Save verification result
            self.progress["stats"][str(year)]["verified"] = True
            self.progress["stats"][str(year)]["verification"] = {
                "pls_total": total_pls,
                "local_total": total_local,
                "missing": total_missing,
                "coverage_pct": round(coverage, 1),
                "by_reporter": results,
                "verified_at": datetime.now().isoformat(),
            }
            self._save_progress()

            # Also save to audit dir
            audit_dir = DATA_DIR / "audit"
            audit_dir.mkdir(exist_ok=True)
            audit_file = audit_dir / f"historical_verify_{year}.json"
            audit_file.write_text(json.dumps({
                "year": year,
                "pls_total": total_pls,
                "local_total": total_local,
                "missing": total_missing,
                "coverage_pct": round(coverage, 1),
                "by_reporter": results,
                "verified_at": datetime.now().isoformat(),
            }, indent=2), encoding='utf-8')

            if total_missing > 0:
                logger.warning(f"YEAR {year}: {total_missing} cases still missing after scrape!")
            else:
                logger.info(f"YEAR {year}: PERFECT — 100% coverage!")

        except Exception as e:
            logger.error(f"Verification failed for {year}: {e}")

    def file_exists(self, citation: str) -> bool:
        """Check if case already exists on disk."""
        parts = citation.split()
        if len(parts) >= 3:
            safe = re.sub(r'[^\w\-]', '_', citation)
            return (DATA_DIR / parts[1] / parts[0] / f"{safe}.json").exists()
        return False

    def scrape_year(self, year: int, reporters: List[str] = None):
        """Scrape all cases for a year with fresh login."""
        reporters = reporters or REPORTERS

        logger.info(f"{'='*60}")
        logger.info(f"HISTORICAL SCRAPER - Year {year}")
        logger.info(f"{'='*60}")

        # Fresh login for this year
        if not self.login():
            logger.error(f"Cannot login - skipping year {year}")
            return

        year_fetched = 0
        year_skipped = 0
        year_start = time.time()

        for reporter in reporters:
            search_key = f"{year}-{reporter}"
            if search_key in self.progress["completed"]:
                logger.info(f"Skipping {search_key} (already completed)")
                continue

            # Session health check before each reporter
            if not self.check_session():
                logger.info(f"Re-logging in before {reporter}...")
                if not self.login():
                    logger.error(f"Re-login failed, skipping {reporter}")
                    continue

            # Search
            cases = self.citation_search(year, reporter)
            if not cases:
                # SAFETY: verify session is still alive before marking empty result as "completed"
                # A dead session returns 0 results silently — marking that as "completed" is a false positive
                if not self.check_session():
                    logger.warning(f"Session died during search for {year} {reporter} — NOT marking as completed")
                    if not self.login():
                        logger.error(f"Re-login failed after dead session on {year} {reporter}")
                    continue  # retry this reporter on next run, don't mark completed
                logger.info(f"No cases on PLS for {year} {reporter} (session verified alive)")
                self.progress["completed"].append(search_key)
                self._save_progress()
                continue

            fetched = 0
            skipped = 0

            for i, case_info in enumerate(cases):
                citation = case_info["citation"]
                case_name = case_info["case_name"]

                # Skip if exists
                if self.file_exists(citation):
                    skipped += 1
                    continue

                if citation in self.progress["cases_fetched"]:
                    skipped += 1
                    continue

                # Fetch
                case = self.fetch_case(case_name, citation)
                if case:
                    self.save_case(case)
                    self.progress["cases_fetched"].append(citation)
                    fetched += 1
                    year_fetched += 1

                    if fetched % 10 == 0:
                        self._save_progress()
                        logger.info(f"  {reporter}: {fetched}/{len(cases)-skipped} new ({skipped} skipped)")
                else:
                    logger.warning(f"  Failed to fetch: {citation}")

            year_skipped += skipped
            self.progress["completed"].append(search_key)
            self._save_progress()

            elapsed_rep = time.time() - year_start
            logger.info(f"  {reporter} DONE: +{fetched} new, {skipped} skipped ({len(cases)} on PLS)")

            # Longer cooldown between reporters — looks like a user switching tabs
            between_reporter_pause = random.uniform(30, 90)
            logger.info(f"Pausing {between_reporter_pause:.0f}s between reporters...")
            time.sleep(between_reporter_pause)

        # Year summary
        elapsed = time.time() - year_start
        rate = year_fetched / (elapsed / 3600) if elapsed > 0 else 0
        logger.info(f"\n{'='*60}")
        logger.info(f"YEAR {year} COMPLETE: +{year_fetched} new cases, {year_skipped} skipped")
        logger.info(f"Time: {elapsed/60:.1f}min | Rate: {rate:.0f} cases/hr")
        logger.info(f"{'='*60}\n")

        # Save stats
        self.progress["stats"][str(year)] = {
            "fetched": year_fetched,
            "skipped": year_skipped,
            "elapsed_min": round(elapsed / 60, 1),
            "rate": round(rate),
            "completed_at": datetime.now().isoformat(),
            "verified": False,
        }
        self._save_progress()

        # Auto-verify: fresh login + run verifier
        logger.info(f"Running post-scrape verification for {year}...")
        self.verify_year(year)


def main():
    parser = argparse.ArgumentParser(description="Historical Case Law Scraper")
    parser.add_argument("--year", type=int, help="Single year to scrape")
    parser.add_argument("--from-year", type=int, default=2014, help="Start year (default: 2014)")
    parser.add_argument("--to-year", type=int, default=2010, help="End year (default: 2010)")
    parser.add_argument("--reporter", type=str, help="Single reporter to scrape")
    parser.add_argument("--reset", action="store_true", help="Reset progress file")
    args = parser.parse_args()

    scraper = HistoricalScraper()

    if args.reset:
        PROGRESS_FILE.unlink(missing_ok=True)
        scraper.progress = scraper._load_progress()
        logger.info("Progress reset")

    reporters = [args.reporter] if args.reporter else None

    if args.year:
        scraper.scrape_year(args.year, reporters)
    else:
        for year in range(args.from_year, args.to_year - 1, -1):
            scraper.scrape_year(year, reporters)

    # Final summary
    total = sum(s.get("fetched", 0) for s in scraper.progress["stats"].values())
    logger.info(f"\n{'='*60}")
    logger.info(f"ALL DONE - {total} total new cases fetched")
    for yr, stats in sorted(scraper.progress["stats"].items(), reverse=True):
        logger.info(f"  {yr}: +{stats['fetched']} ({stats['elapsed_min']}min, {stats['rate']} cases/hr)")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
