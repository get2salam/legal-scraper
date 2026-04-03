#!/usr/bin/env python3
"""
AUDIT SCRIPT 4: PLS Browse Full Listing
=========================================
For years with known gaps (from Script 1):
- Use PLS browse/search API to get the complete list of all cases
- Compare 1:1 with our files
- Output exact missing case IDs with their PLS case_name for fetching

Depends on: pls_vs_local_counts.json (Script 1 output)

Output: data_v2/audit/pls_browse_missing.json
"""

import os
import sys
import json
import re
import time
import random
from pathlib import Path
from dotenv import load_dotenv
from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
OUTPUT_FILE = AUDIT_DIR / "pls_browse_missing.json"
PROGRESS_FILE = AUDIT_DIR / "pls_browse_progress.json"
COUNTS_FILE = AUDIT_DIR / "pls_vs_local_counts.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
MIN_DELAY = 2.0
MAX_DELAY = 4.0
BREAK_INTERVAL = 50
BREAK_MIN = 20
BREAK_MAX = 60


class PLSBrowser:
    def __init__(self):
        self.session = None
        self.logged_in = False
        self.request_count = 0
        self.requests_since_break = 0

    def _create_session(self):
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
        return session

    def _is_login_page(self, html):
        return any(ind in html for ind in ['name="Login.UserName"', 'Login/LoginCheck'])

    def _human_delay(self):
        time.sleep(max(1.0, random.uniform(MIN_DELAY, MAX_DELAY) + random.gauss(0, 0.3)))

    def _maybe_break(self):
        self.requests_since_break += 1
        if self.requests_since_break >= BREAK_INTERVAL:
            dur = random.uniform(BREAK_MIN, BREAK_MAX)
            print(f"  [Break: {dur:.0f}s]")
            time.sleep(dur)
            self.requests_since_break = 0

    def login(self, max_attempts=3):
        username = os.getenv("PLS_USER", "")
        password = os.getenv("PLS_PASS", "")
        for attempt in range(1, max_attempts + 1):
            self.session = self._create_session()
            try:
                resp = self.session.get(f"{BASE_URL}/", timeout=30)
                time.sleep(random.uniform(2, 4))
                csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
                if not csrf_match:
                    continue
                csrf_token = csrf_match.group(1)
                time.sleep(random.uniform(2, 3))
                self.session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
                    "Login.UserName": username, "Login.Password": password,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)
                time.sleep(random.uniform(2, 3))
                check = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check and "Logout" in check.text:
                    self.logged_in = True
                    print(f"  Login OK (attempt {attempt})")
                    return True
                # Fallback
                resp2 = self.session.get(f"{BASE_URL}/", timeout=30)
                csrf2 = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp2.text)
                csrf_token = csrf2.group(1) if csrf2 else csrf_token
                login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
                    "Login.UserName": username, "Login.Password": password,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)
                if login_resp and "Logout" in login_resp.text:
                    self.logged_in = True
                    return True
            except Exception as e:
                print(f"  Login error: {e}")
            time.sleep(10 * attempt)
        return False

    def search_year(self, year, reporter):
        """Search PLS for all cases in a year/reporter, return list of (citation, case_id) tuples."""
        self._maybe_break()
        self._human_delay()
        try:
            resp = self.session.post(f"{BASE_URL}/Login/CitationSearch", data={
                "year": year, "book": reporter,
                "code": "", "court": "", "judge": "", "lawyer": "", "party": "",
            }, timeout=30)
            if not resp or self._is_login_page(resp.text):
                self.logged_in = False
                if self.login():
                    return self.search_year(year, reporter)
                return []
            self.request_count += 1
            return self._parse_with_ids(resp.text)
        except:
            return []

    def _parse_with_ids(self, html):
        """Parse results to get (citation, case_id) pairs."""
        results = []
        soup = BeautifulSoup(html, 'html.parser')
        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                citation = cells[1].get_text(strip=True)
                btn = row.find('input', attrs={'casetypeid': True})
                case_id = btn.get('casetypeid', '') if btn else ""
                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    results.append({"citation": citation, "case_id": case_id})
        
        # Fallback
        if not results:
            cits = re.findall(r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', html)
            ids = re.findall(r'casetypeid="([^"]+)"', html)
            for i, c in enumerate(cits):
                cid = ids[i] if i < len(ids) else ""
                results.append({"citation": c, "case_id": cid})
        
        # Deduplicate
        seen = set()
        unique = []
        for r in results:
            if r["citation"] not in seen:
                seen.add(r["citation"])
                unique.append(r)
        return unique


def get_local_citations(reporter, year):
    year_dir = DATA_DIR / reporter / str(year)
    if not year_dir.exists():
        return set()
    citations = set()
    for f in year_dir.glob("*.json"):
        try:
            with open(f, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            cit = data.get("citation", "")
            if cit:
                citations.add(cit)
        except:
            pass
    return citations


def load_progress():
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {"completed": [], "missing": {}}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 4: PLS Browse Full Listing")
    print("=" * 60)

    # Load gaps from Script 1
    if not COUNTS_FILE.exists():
        print("ERROR: pls_vs_local_counts.json not found! Run audit_pls_counts.py first.")
        sys.exit(1)

    with open(COUNTS_FILE, 'r', encoding='utf-8') as f:
        counts_data = json.load(f)

    years_with_gaps = counts_data.get("years_with_gaps", [])
    if not years_with_gaps:
        print("No gaps found in Script 1 output. Nothing to browse.")
        return

    # Sort by most missing first
    years_with_gaps.sort(key=lambda x: -x["missing"])
    print(f"Found {len(years_with_gaps)} year/reporter combinations with gaps")

    progress = load_progress()
    completed_set = set(progress.get("completed", []))
    all_missing = progress.get("missing", {})

    browser = PLSBrowser()
    if not browser.login():
        print("FATAL: Cannot login")
        sys.exit(1)

    start_time = time.time()
    total_missing_found = 0

    for gap in years_with_gaps:
        reporter = gap["reporter"]
        year = gap["year"]
        key = f"{reporter}_{year}"

        if key in completed_set:
            continue

        print(f"\n  Browsing {year} {reporter} (expected {gap['missing']} missing)...")
        
        pls_cases = browser.search_year(year, reporter)
        local_cits = get_local_citations(reporter, year)

        missing_cases = []
        for pls_case in pls_cases:
            if pls_case["citation"] not in local_cits:
                missing_cases.append(pls_case)
                total_missing_found += 1

        if missing_cases:
            all_missing[key] = {
                "reporter": reporter,
                "year": year,
                "pls_total": len(pls_cases),
                "local_total": len(local_cits),
                "missing_count": len(missing_cases),
                "missing_cases": missing_cases,
            }
            print(f"    Found {len(missing_cases)} missing (PLS has {len(pls_cases)}, we have {len(local_cits)})")
        else:
            print(f"    No missing cases (PLS: {len(pls_cases)}, local: {len(local_cits)})")

        completed_set.add(key)
        progress["completed"] = list(completed_set)
        progress["missing"] = all_missing
        save_progress(progress)

    elapsed = time.time() - start_time

    # Build output
    flat_missing = []
    for key, data in all_missing.items():
        for mc in data.get("missing_cases", []):
            flat_missing.append({
                "citation": mc["citation"],
                "case_id": mc["case_id"],
                "reporter": data["reporter"],
                "year": data["year"],
            })

    output = {
        "audit": "pls_browse",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": {
            "gap_years_checked": len(completed_set),
            "total_missing_found": total_missing_found,
            "elapsed_seconds": round(elapsed, 1),
        },
        "missing_by_year": all_missing,
        "all_missing_flat": flat_missing,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')

    print("\n" + "=" * 60)
    print("PLS BROWSE COMPLETE")
    print("=" * 60)
    print(f"Gap years checked: {len(completed_set)}")
    print(f"Total missing cases found: {total_missing_found}")
    print(f"Time: {elapsed:.1f}s")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
