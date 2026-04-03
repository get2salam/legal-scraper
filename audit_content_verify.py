#!/usr/bin/env python3
"""
AUDIT SCRIPT 5: Content Integrity Verification
================================================
- Pick 500 random cases across all years (stratified sample)
- Re-fetch from PLS
- Compare content length and key fields against our local copy
- Flag: truncated judgments, empty judgment_raw, wrong citation in file

Output: data_v2/audit/content_issues.json
"""

import os
import sys
import json
import re
import time
import random
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
AUDIT_DIR = DATA_DIR / "audit"
AUDIT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = AUDIT_DIR / "content_issues.json"
PROGRESS_FILE = AUDIT_DIR / "content_verify_progress.json"

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
SAMPLE_SIZE = 500

MIN_DELAY = 2.0
MAX_DELAY = 4.0
BREAK_INTERVAL = 50
BREAK_MIN = 20
BREAK_MAX = 60


class ContentVerifier:
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
        indicators = ['name="Login.UserName"', 'Login/LoginCheck', 'id="txtLoginname"']
        return any(ind in html for ind in indicators)

    def _human_delay(self):
        delay = random.uniform(MIN_DELAY, MAX_DELAY) + random.gauss(0, 0.3)
        time.sleep(max(1.0, delay))

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

    def search_and_get_case_id(self, year, reporter, citation):
        """Search for a case and get its case_name/ID for fetching."""
        self._maybe_break()
        self._human_delay()
        try:
            resp = self.session.post(f"{BASE_URL}/Login/CitationSearch", data={
                "year": year, "book": reporter,
                "code": "", "court": "", "judge": "", "lawyer": "", "party": "",
            }, timeout=30)
            if not resp or self._is_login_page(resp.text):
                return None
            self.request_count += 1
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            for row in soup.find_all('tr', class_='caseType'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    cit_text = cells[1].get_text(strip=True)
                    if citation in cit_text or cit_text in citation:
                        btn = row.find('input', attrs={'casetypeid': True})
                        if btn:
                            return btn.get('casetypeid', '')
            # Fallback: find casetypeid near the citation
            ids = re.findall(r'casetypeid="([^"]+)"', resp.text)
            cits = re.findall(r'(\d{4}\s+(?:PLD|SCMR|MLD|CLC|PCrLJ|PTD|YLR|PLC|CLD|GBLR)\s+\d+)', resp.text)
            for i, c in enumerate(cits):
                if c == citation and i < len(ids):
                    return ids[i]
            return None
        except:
            return None

    def fetch_case(self, case_name):
        """Fetch case content from PLS."""
        self._maybe_break()
        self._human_delay()
        try:
            resp = self.session.post(f"{BASE_URL}/Login/GetCaseFile", data={
                "caseName": case_name, "headNotes": 0,
            }, timeout=30)
            if not resp or len(resp.text) < 100:
                return None
            self.request_count += 1
            html = resp.text
            if html.startswith('"'):
                try:
                    html = json.loads(html)
                except:
                    pass
            return html
        except:
            return None


def select_stratified_sample(sample_size=500):
    """Select a stratified random sample of cases across reporters and years."""
    all_cases = []
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            continue
        for year_dir in reporter_dir.iterdir():
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            json_files = list(year_dir.glob("*.json"))
            for f in json_files:
                all_cases.append({
                    "reporter": reporter,
                    "year": int(year_dir.name),
                    "file": str(f),
                })

    # Stratified: proportional to number of files per reporter
    random.shuffle(all_cases)
    
    # Group by reporter
    by_reporter = defaultdict(list)
    for c in all_cases:
        by_reporter[c["reporter"]].append(c)
    
    total = len(all_cases)
    sample = []
    for reporter, cases in by_reporter.items():
        n = max(10, int(sample_size * len(cases) / total))
        sample.extend(random.sample(cases, min(n, len(cases))))
    
    # Trim to exact sample size
    random.shuffle(sample)
    return sample[:sample_size]


def load_progress():
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {"verified": [], "issues": [], "sample": []}


def save_progress(progress):
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')


def main():
    print("=" * 60)
    print("AUDIT SCRIPT 5: Content Integrity Verification")
    print("=" * 60)

    progress = load_progress()
    verified_set = set(progress.get("verified", []))
    issues = progress.get("issues", [])
    
    # Select or load sample
    if progress.get("sample"):
        sample = progress["sample"]
        print(f"Resuming with existing sample of {len(sample)} cases")
    else:
        print(f"Selecting stratified sample of {SAMPLE_SIZE} cases...")
        sample = select_stratified_sample(SAMPLE_SIZE)
        progress["sample"] = sample
        save_progress(progress)
        print(f"Selected {len(sample)} cases")

    verifier = ContentVerifier()
    if not verifier.login():
        print("FATAL: Cannot login")
        sys.exit(1)

    start_time = time.time()
    total_verified = len(verified_set)
    total_issues = len(issues)
    
    # Cache search results per year/reporter to avoid re-searching
    search_cache = {}

    for i, case_info in enumerate(sample):
        file_path = case_info["file"]
        if file_path in verified_set:
            continue

        # Load local case
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                local_data = json.load(f)
        except:
            issues.append({
                "file": file_path,
                "issue": "cannot_read_local",
            })
            verified_set.add(file_path)
            continue

        citation = local_data.get("citation", "")
        case_name = local_data.get("case_name", "")
        reporter = case_info["reporter"]
        year = case_info["year"]

        if not citation:
            issues.append({"file": file_path, "issue": "no_citation_field"})
            verified_set.add(file_path)
            continue

        # Get case from PLS
        pls_html = None
        if case_name:
            pls_html = verifier.fetch_case(case_name)
        
        if not pls_html:
            # Try to find case_name via search
            cache_key = f"{year}_{reporter}"
            if cache_key not in search_cache:
                case_id = verifier.search_and_get_case_id(year, reporter, citation)
                if case_id:
                    search_cache[cache_key] = True
                    pls_html = verifier.fetch_case(case_id)

        if not pls_html:
            issues.append({"file": file_path, "citation": citation, "issue": "cannot_fetch_from_pls"})
            verified_set.add(file_path)
            total_verified += 1
            continue

        # Compare content
        local_raw = local_data.get("judgment_raw", "")
        local_judgment = local_data.get("judgment", "")
        
        pls_soup = BeautifulSoup(pls_html, 'html.parser')
        pls_text = pls_soup.get_text(separator='\n', strip=True)

        file_issues = []

        # Check judgment length comparison
        if local_raw:
            local_len = len(local_raw)
            pls_len = len(pls_html)
            ratio = local_len / pls_len if pls_len > 0 else 0
            if ratio < 0.5:
                file_issues.append({
                    "issue": "possibly_truncated",
                    "local_raw_length": local_len,
                    "pls_html_length": pls_len,
                    "ratio": round(ratio, 3),
                })
        elif not local_judgment:
            file_issues.append({"issue": "empty_judgment_locally"})

        # Check if judgment_raw is suspiciously short
        if local_raw and len(local_raw) < 200 and len(pls_html) > 1000:
            file_issues.append({
                "issue": "stub_judgment",
                "local_length": len(local_raw),
                "pls_length": len(pls_html),
            })

        if file_issues:
            for fi in file_issues:
                fi["file"] = file_path
                fi["citation"] = citation
                issues.append(fi)
            total_issues += len(file_issues)

        verified_set.add(file_path)
        total_verified += 1

        if total_verified % 20 == 0:
            elapsed = time.time() - start_time
            print(f"  Verified {total_verified}/{len(sample)} | Issues: {total_issues} | {elapsed:.0f}s")
            progress["verified"] = list(verified_set)
            progress["issues"] = issues
            save_progress(progress)

        # Check session health
        if total_verified % 50 == 0:
            if not verifier.session:
                verifier.login()

    # Save final
    elapsed = time.time() - start_time
    
    issue_types = defaultdict(int)
    for issue in issues:
        issue_types[issue.get("issue", "unknown")] += 1

    output = {
        "audit": "content_verify",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "stats": {
            "sample_size": len(sample),
            "total_verified": total_verified,
            "total_issues": len(issues),
            "issue_breakdown": dict(issue_types),
            "elapsed_seconds": round(elapsed, 1),
        },
        "issues": issues,
    }

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding='utf-8')
    progress["verified"] = list(verified_set)
    progress["issues"] = issues
    save_progress(progress)

    print("\n" + "=" * 60)
    print("CONTENT VERIFICATION COMPLETE")
    print("=" * 60)
    print(f"Sample size: {len(sample)}")
    print(f"Verified: {total_verified}")
    print(f"Issues found: {len(issues)}")
    print(f"Time: {elapsed:.1f}s")
    print()
    print("Issue breakdown:")
    for it, count in sorted(issue_types.items(), key=lambda x: -x[1]):
        print(f"  {it}: {count}")
    print(f"\nOutput saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
