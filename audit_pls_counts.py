#!/usr/bin/env python3
"""
AUDIT SCRIPT 1: PLS API Count Comparison
==========================================
For every reporter and every year (1947-2026):
- Query PLS API to get total number of cases they have
- Count our local JSON files
- Output: pls_vs_local_counts.json with {reporter: {year: {pls: N, local: N, missing: N}}}
- Flag every year where local < pls
- Human-like delays (2-4 seconds between requests)
- Save progress so it can resume if interrupted

Output: data_v2/audit/pls_vs_local_counts.json
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
from collections import defaultdict

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = AUDIT_DIR / "pls_vs_local_counts.json"
PROGRESS_FILE = AUDIT_DIR / "pls_counts_progress.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
START_YEAR = 1947
END_YEAR = 2026

# Rate limiting
MIN_DELAY = 2.0
MAX_DELAY = 4.0
BREAK_INTERVAL = 50
BREAK_MIN = 20
BREAK_MAX = 60


class PLSAuditor:
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

    def _is_login_page(self, html):
        indicators = [
            'id="txtLoginname"', 'id="txtLoginpassword"', 'Login/LoginCheck',
            'id="username"', 'id="loginPass"', 'Login.UserName', 'Login.Password',
            'name="Login.UserName"',
        ]
        return any(ind in html for ind in indicators)

    def _human_delay(self):
        delay = random.uniform(MIN_DELAY, MAX_DELAY) + random.gauss(0, 0.3)
        delay = max(1.0, delay)
        time.sleep(delay)

    def _maybe_break(self):
        self.requests_since_break += 1
        if self.requests_since_break >= BREAK_INTERVAL:
            dur = random.uniform(BREAK_MIN, BREAK_MAX)
            print(f"  [Break: {dur:.0f}s after {BREAK_INTERVAL} requests]")
            time.sleep(dur)
            self.requests_since_break = 0

    def login(self, max_attempts=3):
        username = os.getenv("PLS_USER", "")
        password = os.getenv("PLS_PASS", "")
        if not username or not password:
            print("ERROR: PLS_USER/PLS_PASS not set")
            return False

        for attempt in range(1, max_attempts + 1):
            self.session = self._create_session()
            self.logged_in = False
            self.requests_since_break = 0
            print(f"Logging in (attempt {attempt}/{max_attempts})...")

            try:
                resp = self.session.get(f"{BASE_URL}/", timeout=30)
                if resp.status_code != 200:
                    print(f"  Homepage returned {resp.status_code}")
                    time.sleep(10 * attempt)
                    continue

                time.sleep(random.uniform(2, 4))

                csrf_match = re.search(
                    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text
                )
                if not csrf_match:
                    print(f"  CSRF token not found")
                    time.sleep(10 * attempt)
                    continue

                csrf_token = csrf_match.group(1)
                time.sleep(random.uniform(2, 3))

                # ClearLoginHistory
                clear_resp = self.session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
                    "Login.UserName": username,
                    "Login.Password": password,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)

                time.sleep(random.uniform(2, 3))

                # Check login
                check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check_resp and check_resp.status_code == 200 and "Logout" in check_resp.text:
                    self.logged_in = True
                    print("  Login successful (via ClearLoginHistory)")
                    return True

                # Try explicit login
                resp2 = self.session.get(f"{BASE_URL}/", timeout=30)
                csrf_match2 = re.search(
                    r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp2.text
                )
                csrf_token = csrf_match2.group(1) if csrf_match2 else csrf_token
                time.sleep(random.uniform(1, 2))

                login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
                    "Login.UserName": username,
                    "Login.Password": password,
                    "__RequestVerificationToken": csrf_token,
                }, timeout=30)

                if login_resp and "Logout" in login_resp.text:
                    self.logged_in = True
                    print("  Login successful (via Login/Login)")
                    return True

                time.sleep(2)
                check2 = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
                if check2 and "Logout" in check2.text:
                    self.logged_in = True
                    print("  Login successful (verified)")
                    return True

                print(f"  Login failed (attempt {attempt})")
                time.sleep(10 * attempt)

            except Exception as e:
                print(f"  Login error: {e}")
                time.sleep(10 * attempt)

        print("All login attempts failed")
        return False

    def check_session(self):
        if not self.logged_in or not self.session:
            return False
        try:
            resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=10, allow_redirects=False)
            if resp.status_code in (301, 302):
                self.logged_in = False
                return False
            if resp.status_code == 200 and ("Logout" not in resp.text or self._is_login_page(resp.text)):
                self.logged_in = False
                return False
            return True
        except:
            return False

    def citation_search(self, year, reporter):
        """Search PLS for cases and return the list of citations."""
        self._maybe_break()
        self._human_delay()

        try:
            resp = self.session.post(f"{BASE_URL}/Login/CitationSearch", data={
                "year": year,
                "book": reporter,
                "code": "",
                "court": "",
                "judge": "",
                "lawyer": "",
                "party": "",
            }, timeout=30)

            if not resp or resp.status_code != 200:
                return None

            # Check for session expiry
            if self._is_login_page(resp.text):
                print(f"  Session expired during search, re-logging in...")
                self.logged_in = False
                if self.login():
                    return self.citation_search(year, reporter)
                return None

            self.request_count += 1
            return self._parse_results(resp.text)

        except Exception as e:
            print(f"  Search error {year} {reporter}: {e}")
            return None

    def _parse_results(self, html):
        """Parse citation search results, return list of citation strings."""
        from bs4 import BeautifulSoup
        citations = []
        soup = BeautifulSoup(html, 'html.parser')

        for row in soup.find_all('tr', class_='caseType'):
            cells = row.find_all('td')
            if len(cells) >= 2:
                citation = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                if citation and re.search(r'\d{4}\s+[A-Z]+\s+\d+', citation):
                    citations.append(citation)

        # Fallback
        if not citations:
            citations = re.findall(
                r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', html
            )

        return list(set(citations))

    def get_local_count(self, reporter, year):
        """Count local JSON files for a reporter/year."""
        year_dir = DATA_DIR / reporter / str(year)
        if not year_dir.exists():
            return 0
        return len(list(year_dir.glob("*.json")))

    def get_local_citations(self, reporter, year):
        """Get set of local citation strings for a reporter/year."""
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
    return {"completed": {}, "pls_citations": {}}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 1: PLS API Count Comparison")
    print("=" * 60)

    progress = load_progress()
    auditor = PLSAuditor()
    
    if not auditor.login():
        print("FATAL: Cannot login to PLS")
        sys.exit(1)

    results = progress.get("completed", {})
    pls_citations = progress.get("pls_citations", {})
    start_time = time.time()
    total_queries = 0
    total_gaps = 0
    gaps_found = []

    for reporter in REPORTERS:
        if reporter not in results:
            results[reporter] = {}
        if reporter not in pls_citations:
            pls_citations[reporter] = {}

        print(f"\n--- {reporter} ---")

        for year in range(END_YEAR, START_YEAR - 1, -1):
            year_str = str(year)
            ry_key = f"{reporter}_{year}"

            # Skip already completed
            if year_str in results[reporter]:
                r = results[reporter][year_str]
                if r.get("missing", 0) > 0:
                    total_gaps += r["missing"]
                continue

            # Check session health every 100 queries
            if total_queries > 0 and total_queries % 100 == 0:
                if not auditor.check_session():
                    print(f"  Re-logging in...")
                    if not auditor.login():
                        print("  FATAL: Re-login failed")
                        save_progress({"completed": results, "pls_citations": pls_citations})
                        sys.exit(1)

            # Query PLS
            pls_list = auditor.citation_search(year, reporter)
            total_queries += 1

            if pls_list is None:
                print(f"  {year} {reporter}: QUERY FAILED (skipping)")
                continue

            pls_count = len(pls_list)
            local_count = auditor.get_local_count(reporter, year)
            missing = max(0, pls_count - local_count)

            results[reporter][year_str] = {
                "pls": pls_count,
                "local": local_count,
                "missing": missing,
            }

            # Save PLS citations for Script 2
            if pls_list:
                pls_citations[reporter][year_str] = pls_list

            if missing > 0:
                total_gaps += missing
                gaps_found.append(f"{year} {reporter}: {local_count}/{pls_count} ({missing} missing)")
                print(f"  {year} {reporter}: {local_count}/{pls_count} *** {missing} MISSING ***")
            elif pls_count > 0:
                # Don't print zero-count years
                if total_queries % 20 == 0:
                    print(f"  {year} {reporter}: {local_count}/{pls_count} OK")

            # Save progress periodically
            if total_queries % 50 == 0:
                save_progress({"completed": results, "pls_citations": pls_citations})
                elapsed = time.time() - start_time
                print(f"  [Progress saved: {total_queries} queries, {elapsed:.0f}s elapsed, {total_gaps} gaps so far]")

    # Save final progress
    save_progress({"completed": results, "pls_citations": pls_citations})

    # Build summary
    elapsed = time.time() - start_time
    
    total_pls = 0
    total_local = 0
    reporter_summary = {}
    years_with_gaps = []

    for reporter in REPORTERS:
        r_pls = 0
        r_local = 0
        r_missing = 0
        for year_str, counts in results.get(reporter, {}).items():
            r_pls += counts.get("pls", 0)
            r_local += counts.get("local", 0)
            r_missing += counts.get("missing", 0)
            if counts.get("missing", 0) > 0:
                years_with_gaps.append({
                    "reporter": reporter,
                    "year": int(year_str),
                    "pls": counts["pls"],
                    "local": counts["local"],
                    "missing": counts["missing"],
                })
        total_pls += r_pls
        total_local += r_local
        coverage = (r_local / r_pls * 100) if r_pls > 0 else 100
        reporter_summary[reporter] = {
            "pls_total": r_pls,
            "local_total": r_local,
            "missing": r_missing,
            "coverage_pct": round(coverage, 1),
        }

    years_with_gaps.sort(key=lambda x: -x["missing"])
    overall_coverage = (total_local / total_pls * 100) if total_pls > 0 else 100

    output = {
        "audit": "pls_vs_local_counts",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": {
            "total_pls_cases": total_pls,
            "total_local_cases": total_local,
            "total_missing": total_pls - total_local,
            "overall_coverage_pct": round(overall_coverage, 1),
            "total_queries": total_queries,
            "elapsed_seconds": round(elapsed, 1),
        },
        "per_reporter": reporter_summary,
        "years_with_gaps": years_with_gaps,
        "detailed_counts": results,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')

    # Print summary
    print("\n" + "=" * 60)
    print("PLS COUNT COMPARISON COMPLETE")
    print("=" * 60)
    print(f"Total PLS cases: {total_pls:,}")
    print(f"Total local cases: {total_local:,}")
    print(f"Overall coverage: {overall_coverage:.1f}%")
    print(f"Total missing: {total_pls - total_local:,}")
    print(f"Queries made: {total_queries:,}")
    print(f"Time: {elapsed:.1f}s")
    print()
    print("Per reporter:")
    for reporter, rs in reporter_summary.items():
        print(f"  {reporter}: {rs['local_total']:,}/{rs['pls_total']:,} ({rs['coverage_pct']}%) — {rs['missing']:,} missing")
    print()
    if years_with_gaps:
        print(f"Years with gaps ({len(years_with_gaps)} total):")
        for g in years_with_gaps[:20]:
            print(f"  {g['year']} {g['reporter']}: {g['local']}/{g['pls']} ({g['missing']} missing)")
        if len(years_with_gaps) > 20:
            print(f"  ... and {len(years_with_gaps) - 20} more")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
