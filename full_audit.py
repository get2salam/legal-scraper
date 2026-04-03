#!/usr/bin/env python3
"""
Full Legislation Audit — PLS vs Disk Reconciliation
=====================================================
Logs into PLS, fetches the real statute + section lists,
and compares against every file on disk.

Outputs a detailed report with:
- Missing statutes (on PLS but not on disk)
- Extra files (on disk but not on PLS)
- Missing sections (PLS has more sections than disk)
- Corrupt sections (text is "-1" or empty)
- Quality statistics
"""

import os
import re
import json
import time
import random
import sys
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Optional, List, Dict, Any

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
REPORT_DIR = DATA_DIR / "audit"
REPORT_DIR.mkdir(exist_ok=True)

PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Delays between requests (faster for audit - read-only, no writes)
MIN_DELAY = 0.3
MAX_DELAY = 0.8


class PLSAuditor:
    def __init__(self):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
    
    def _delay(self, min_s=None, max_s=None):
        d = random.uniform(min_s or MIN_DELAY, max_s or MAX_DELAY)
        time.sleep(max(1.0, d))
    
    def _request(self, method, url, retries=3, **kwargs):
        for attempt in range(retries):
            try:
                self._delay()
                if method == "GET":
                    resp = self.session.get(url, timeout=30, **kwargs)
                else:
                    resp = self.session.post(url, timeout=30, **kwargs)
                self.request_count += 1
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (403, 429, 500):
                    backoff = 30 * (attempt + 1)
                    print(f"  [{resp.status_code}] Backing off {backoff}s (attempt {attempt+1})")
                    time.sleep(backoff)
                    continue
                print(f"  Unexpected status {resp.status_code}")
                return None
            except Exception as e:
                print(f"  Request error (attempt {attempt+1}): {e}")
                time.sleep(15 * (attempt + 1))
        return None
    
    def login(self) -> bool:
        print("Logging into PLS...")
        self.session = Session(impersonate=BrowserType.chrome120)
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "X-Requested-With": "XMLHttpRequest",
        })
        
        resp = self.session.get(f"{BASE_URL}/", timeout=30)
        if not resp or resp.status_code != 200:
            print("ERROR: Failed to load homepage")
            return False
        
        csrf = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
        if not csrf:
            print("ERROR: CSRF token not found")
            return False
        
        time.sleep(2)
        
        login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
            "__RequestVerificationToken": csrf.group(1)
        }, timeout=30)
        
        if not login_resp or login_resp.status_code != 200:
            print("ERROR: Login failed")
            return False
        
        time.sleep(2)
        check = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if not check or "pakistanlaws" not in check.text.lower():
            print("ERROR: Login verification failed")
            return False
        
        self.logged_in = True
        print("[OK] Logged in successfully")
        return True
    
    def get_pls_statutes(self, letter: str) -> List[str]:
        """Get all statute names for a letter from PLS."""
        print(f"Fetching PLS statute list for '{letter}'...")
        resp = self._request("GET", f"{BASE_URL}/Login/StatuecharSearch",
                            params={"character": letter})
        if not resp:
            print(f"  ERROR: Could not fetch statutes for '{letter}'")
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        statutes = []
        for row in soup.find_all('tr', class_='caseType'):
            name = row.get('casetypeid', '').strip()
            if name:
                statutes.append(name)
        
        print(f"  Found {len(statutes)} statutes on PLS for '{letter}'")
        return statutes
    
    def get_pls_sections(self, statute_name: str) -> List[Dict]:
        """Get all sections for a statute from PLS."""
        resp = self._request("GET", f"{BASE_URL}/Login/GetStatuesSearch",
                            params={"caseName": statute_name})
        if not resp:
            return []
        
        soup = BeautifulSoup(resp.text, 'html.parser')
        sections = []
        for row in soup.find_all('tr', class_='table_row_hover'):
            cells = row.find_all('td')
            if len(cells) >= 4:
                read_cell = cells[0]
                section_num = cells[1].get_text(strip=True)
                section_id = read_cell.get('casetypeid', '')
                if not section_id:
                    link = read_cell.find(class_='readCaseLaw')
                    if link:
                        section_id = link.get('casetypeid', '')
                sections.append({
                    "section_id": section_id,
                    "number": section_num
                })
        return sections
    
    def audit_letter(self, letter: str, check_sections: bool = True) -> Dict:
        """Full audit for one letter."""
        report = {
            "letter": letter,
            "timestamp": datetime.now().isoformat(),
            "pls_statutes": 0,
            "disk_files": 0,
            "missing_statutes": [],      # On PLS but not on disk
            "extra_files": [],           # On disk but not on PLS
            "matched_statutes": 0,
            "section_audit": {
                "total_checked": 0,
                "total_pls_sections": 0,
                "total_disk_sections": 0,
                "total_corrupt": 0,
                "total_empty": 0,
                "total_valid": 0,
                "mismatches": [],        # Statute where PLS sections != disk sections
                "corrupt_detail": [],    # Files with corrupt sections
            },
            "quality": {
                "files_with_all_valid": 0,
                "files_with_some_corrupt": 0,
                "files_with_all_corrupt": 0,
            }
        }
        
        # 1. Get PLS statute list
        pls_statutes = self.get_pls_statutes(letter)
        report["pls_statutes"] = len(pls_statutes)
        
        # 2. Get disk files
        letter_dir = DATA_DIR / letter
        disk_files = {}  # safe_name -> full_path
        if letter_dir.exists():
            for f in letter_dir.glob("*.json"):
                disk_files[f.stem] = f
        report["disk_files"] = len(disk_files)
        
        # 3. Build name mapping (PLS name -> safe filename)
        def safe_name(title):
            return re.sub(r'[^\w\-]', '_', title)[:100]
        
        pls_name_to_safe = {}
        safe_to_pls_name = {}
        for name in pls_statutes:
            sn = safe_name(name)
            pls_name_to_safe[name] = sn
            safe_to_pls_name[sn] = name
        
        # 4. Find missing and extra statutes
        pls_safe_names = set(pls_name_to_safe.values())
        disk_safe_names = set(disk_files.keys())
        
        missing_safe = pls_safe_names - disk_safe_names
        extra_safe = disk_safe_names - pls_safe_names
        matched_safe = pls_safe_names & disk_safe_names
        
        # Map back to original names
        for sn in sorted(missing_safe):
            orig = safe_to_pls_name.get(sn, sn)
            report["missing_statutes"].append(orig)
        
        for sn in sorted(extra_safe):
            report["extra_files"].append(sn)
        
        report["matched_statutes"] = len(matched_safe)
        
        print(f"\n  PLS: {len(pls_statutes)} | Disk: {len(disk_files)} | Matched: {len(matched_safe)}")
        print(f"  Missing from disk: {len(missing_safe)} | Extra on disk: {len(extra_safe)}")
        
        # 5. Section-level audit (check each file on disk)
        print(f"\n  Auditing sections for {len(disk_files)} files on disk...")
        
        checked = 0
        for sn, fpath in sorted(disk_files.items()):
            try:
                with open(fpath, 'r', encoding='utf-8') as fh:
                    data = json.load(fh)
                
                sections = data.get("sections", [])
                disk_section_count = len(sections)
                
                # Count quality
                valid = 0
                corrupt = 0
                empty = 0
                
                for s in sections:
                    text = s.get("text", "")
                    if isinstance(text, str):
                        text = text.strip()
                    else:
                        text = str(text).strip() if text else ""
                    
                    # Check for "-1" (stored as '"-1"' string)
                    if text in ('"-1"', '-1', '"-1', '-1"') or text == '':
                        if text == '':
                            empty += 1
                        else:
                            corrupt += 1
                    elif len(text) < 5:
                        # Very short but not -1 — might still be valid (e.g. "N/A")
                        corrupt += 1
                    else:
                        valid += 1
                
                report["section_audit"]["total_disk_sections"] += disk_section_count
                report["section_audit"]["total_corrupt"] += corrupt
                report["section_audit"]["total_empty"] += empty
                report["section_audit"]["total_valid"] += valid
                
                if corrupt == 0 and empty == 0:
                    report["quality"]["files_with_all_valid"] += 1
                elif valid == 0 and disk_section_count > 0:
                    report["quality"]["files_with_all_corrupt"] += 1
                else:
                    report["quality"]["files_with_some_corrupt"] += 1
                
                if corrupt > 0 or empty > 0:
                    report["section_audit"]["corrupt_detail"].append({
                        "file": sn[:80],
                        "total_sections": disk_section_count,
                        "valid": valid,
                        "corrupt": corrupt,
                        "empty": empty,
                    })
                
            except Exception as e:
                report["section_audit"]["corrupt_detail"].append({
                    "file": sn[:80],
                    "error": str(e)[:200]
                })
            
            checked += 1
            if checked % 50 == 0:
                print(f"    ... checked {checked}/{len(disk_files)} files")
        
        report["section_audit"]["total_checked"] = checked
        
        # 6. PLS section count comparison (sample to avoid too many requests)
        if check_sections:
            orig_name = safe_to_pls_name.get(sn)
            # Check ALL matched statutes against PLS section counts
            print(f"\n  Comparing section counts against PLS for {len(matched_safe)} statutes...")
            print(f"  (This will make {len(matched_safe)} requests to PLS with delays)")
            
            mismatch_count = 0
            for i, sn in enumerate(sorted(matched_safe)):
                orig_name = safe_to_pls_name.get(sn)
                if not orig_name:
                    continue
                
                # Get PLS section list
                pls_sections = self.get_pls_sections(orig_name)
                pls_count = len(pls_sections)
                
                # Get disk section count
                fpath = disk_files.get(sn)
                if fpath:
                    try:
                        data = json.load(open(fpath, encoding='utf-8'))
                        disk_count = len(data.get("sections", []))
                    except:
                        disk_count = -1
                else:
                    disk_count = 0
                
                report["section_audit"]["total_pls_sections"] += pls_count
                
                if pls_count != disk_count:
                    mismatch_count += 1
                    report["section_audit"]["mismatches"].append({
                        "statute": orig_name[:80],
                        "pls_sections": pls_count,
                        "disk_sections": disk_count,
                        "missing": pls_count - disk_count if pls_count > disk_count else 0,
                    })
                
                if (i + 1) % 25 == 0:
                    print(f"    ... compared {i+1}/{len(matched_safe)} ({mismatch_count} mismatches so far)")
            
            print(f"  Section comparison complete: {mismatch_count} mismatches out of {len(matched_safe)}")
        
        return report
    
    def print_report(self, report: Dict):
        """Print a human-readable report."""
        letter = report["letter"]
        print(f"\n{'='*70}")
        print(f"FULL AUDIT REPORT — LETTER {letter}")
        print(f"{'='*70}")
        
        print(f"\n--- STATUTE COVERAGE ---")
        print(f"  PLS total:              {report['pls_statutes']}")
        print(f"  On disk:                {report['disk_files']}")
        print(f"  Matched:                {report['matched_statutes']}")
        print(f"  Missing from disk:      {len(report['missing_statutes'])}")
        print(f"  Extra on disk:          {len(report['extra_files'])}")
        
        coverage = (report['disk_files'] / report['pls_statutes'] * 100) if report['pls_statutes'] else 0
        print(f"  Coverage:               {coverage:.1f}%")
        
        sa = report["section_audit"]
        print(f"\n--- SECTION QUALITY ---")
        print(f"  Files checked:          {sa['total_checked']}")
        print(f"  Total disk sections:    {sa['total_disk_sections']}")
        if sa['total_pls_sections']:
            print(f"  Total PLS sections:     {sa['total_pls_sections']}")
        print(f"  Valid sections:         {sa['total_valid']}")
        print(f"  Corrupt ('-1'):         {sa['total_corrupt']}")
        print(f"  Empty sections:         {sa['total_empty']}")
        
        total = sa['total_valid'] + sa['total_corrupt'] + sa['total_empty']
        if total:
            valid_pct = sa['total_valid'] / total * 100
            corrupt_pct = (sa['total_corrupt'] + sa['total_empty']) / total * 100
            print(f"  Valid %:                {valid_pct:.1f}%")
            print(f"  Corrupt %:              {corrupt_pct:.1f}%")
        
        q = report["quality"]
        print(f"\n--- FILE QUALITY ---")
        print(f"  All valid:              {q['files_with_all_valid']}")
        print(f"  Some corrupt:           {q['files_with_some_corrupt']}")
        print(f"  All corrupt:            {q['files_with_all_corrupt']}")
        
        if sa['mismatches']:
            print(f"\n--- SECTION COUNT MISMATCHES ({len(sa['mismatches'])}) ---")
            for m in sa['mismatches'][:30]:
                diff = m.get('missing', 0)
                print(f"  {m['statute'][:60]}: PLS={m['pls_sections']}, disk={m['disk_sections']} (missing {diff})")
            if len(sa['mismatches']) > 30:
                print(f"  ... and {len(sa['mismatches']) - 30} more")
        
        if report['missing_statutes']:
            print(f"\n--- MISSING STATUTES ({len(report['missing_statutes'])}) ---")
            for name in report['missing_statutes'][:30]:
                print(f"  - {name[:80]}")
            if len(report['missing_statutes']) > 30:
                print(f"  ... and {len(report['missing_statutes']) - 30} more")
    
    def save_report(self, report: Dict, letter: str):
        """Save JSON report."""
        path = REPORT_DIR / f"audit_{letter}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\nReport saved to: {path}")


def main():
    letters = sys.argv[1:] if len(sys.argv) > 1 else ["A", "B"]
    check_sections = "--no-sections" not in sys.argv
    
    if "--no-sections" in letters:
        letters.remove("--no-sections")
    
    auditor = PLSAuditor()
    if not auditor.login():
        print("FATAL: Cannot log in to PLS")
        sys.exit(1)
    
    for letter in letters:
        letter = letter.upper()
        report = auditor.audit_letter(letter, check_sections=check_sections)
        auditor.print_report(report)
        auditor.save_report(report, letter)
    
    print(f"\nTotal PLS requests made: {auditor.request_count}")


if __name__ == "__main__":
    main()
