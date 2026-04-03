#!/usr/bin/env python3
"""
Section Repair Script
=====================
Scans legislation JSON files for corrupt sections (text is "-1" or empty),
re-fetches the content from PLS, and updates the files in place.

Much faster than full re-scrape — only fetches content for broken sections,
skips statute discovery, section listing, case links, etc.

Usage:
    python repair_sections.py A          # Repair letter A
    python repair_sections.py A B        # Repair A and B
    python repair_sections.py A --dry    # Dry run (no writes)
    python repair_sections.py A --limit 50  # Only fix 50 sections
"""

import os
import re
import json
import time
import random
import sys
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
BACKUP_DIR = DATA_DIR / "audit" / "pre-repair-backup"
REPORT_DIR = DATA_DIR / "audit"

PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing — moderate pace, we're doing many requests
MIN_DELAY = 1.0
MAX_DELAY = 2.5
BREAK_EVERY = 80  # Take a break every N requests
BREAK_MIN = 10
BREAK_MAX = 25
BACKOFF = 30


def is_corrupt(text):
    """Check if section text is corrupt."""
    if not text or not isinstance(text, str):
        return True
    t = text.strip()
    if t == '':
        return True
    if t in ('"-1"', '-1', '"-1', '-1"', '"\\u22121"', '-1.'):
        return True
    if len(t) < 5:
        return True
    return False


class SectionRepairer:
    def __init__(self):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.requests_since_break = 0
        self.stats = {
            "scanned_files": 0,
            "scanned_sections": 0,
            "corrupt_found": 0,
            "repair_attempted": 0,
            "repair_success": 0,
            "repair_failed": 0,  # PLS still returns -1
            "repair_error": 0,   # Network/parse error
            "files_updated": 0,
            "files_backed_up": 0,
        }
    
    def _delay(self, min_s=None, max_s=None):
        d = random.uniform(min_s or MIN_DELAY, max_s or MAX_DELAY)
        d += random.gauss(0, 0.3)
        time.sleep(max(0.5, d))
    
    def _maybe_break(self):
        self.requests_since_break += 1
        if self.requests_since_break >= BREAK_EVERY:
            dur = random.uniform(BREAK_MIN, BREAK_MAX)
            print(f"  [break] Pausing {dur:.0f}s after {BREAK_EVERY} requests...")
            time.sleep(dur)
            self.requests_since_break = 0
    
    def _request(self, method, url, retries=3, **kwargs):
        self._maybe_break()
        self._delay()
        
        for attempt in range(retries):
            try:
                if method == "GET":
                    resp = self.session.get(url, timeout=30, **kwargs)
                else:
                    resp = self.session.post(url, timeout=30, **kwargs)
                
                self.request_count += 1
                
                if resp.status_code == 200:
                    return resp
                
                if resp.status_code in (403, 429, 500):
                    backoff = BACKOFF * (attempt + 1)
                    print(f"  [{resp.status_code}] Backing off {backoff}s (attempt {attempt+1})")
                    time.sleep(backoff)
                    # Re-login on 403
                    if resp.status_code == 403:
                        self.logged_in = False
                        self.login()
                    continue
                
                return None
            except Exception as e:
                print(f"  Request error (attempt {attempt+1}): {e}")
                time.sleep(BACKOFF * (attempt + 1))
                # Try re-login
                if attempt == 1:
                    self.logged_in = False
                    self.login()
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
        print("[OK] Logged in")
        return True
    
    def fetch_section_content(self, section_id: str) -> tuple:
        """Fetch section content from PLS. Returns (html, text) or ("", "") on failure."""
        if not section_id:
            return "", ""
        
        if not self.logged_in:
            if not self.login():
                return "", ""
        
        resp = self._request("POST", f"{BASE_URL}/Login/SearchStatueFile",
                            data={"caseTypeId": section_id})
        
        if not resp:
            return "", ""
        
        raw = resp.text.strip()
        
        # Check for -1 / empty responses
        if raw in ["-1", '"-1"', '"-1', '-1"', ""] or len(raw) < 10:
            return "", ""
        
        # Parse HTML for clean text
        soup = BeautifulSoup(raw, 'html.parser')
        clean = soup.get_text(separator='\n', strip=True)
        
        if len(clean) < 5:
            return "", ""
        
        return raw, clean
    
    def scan_letter(self, letter: str) -> List[Dict]:
        """Scan all files in a letter directory and return corrupt sections."""
        letter_dir = DATA_DIR / letter
        if not letter_dir.exists():
            print(f"No directory for {letter}")
            return []
        
        corrupt_list = []
        files = sorted(letter_dir.glob("*.json"))
        
        print(f"\nScanning {len(files)} files in {letter}...")
        
        for f in files:
            self.stats["scanned_files"] += 1
            try:
                data = json.load(open(f, encoding='utf-8'))
            except:
                continue
            
            sections = data.get("sections", [])
            for i, s in enumerate(sections):
                self.stats["scanned_sections"] += 1
                text = s.get("text", "")
                
                if is_corrupt(text):
                    self.stats["corrupt_found"] += 1
                    corrupt_list.append({
                        "file": f,
                        "section_index": i,
                        "section_id": s.get("section_id", ""),
                        "section_number": s.get("number", "?"),
                        "statute_title": data.get("title", f.stem),
                    })
        
        print(f"  Found {self.stats['corrupt_found']} corrupt sections in {self.stats['scanned_files']} files")
        return corrupt_list
    
    def repair_letter(self, letter: str, dry_run: bool = False, limit: int = 0):
        """Scan and repair all corrupt sections in a letter."""
        corrupt_list = self.scan_letter(letter)
        
        if not corrupt_list:
            print(f"No corrupt sections found in {letter}!")
            return
        
        if limit:
            corrupt_list = corrupt_list[:limit]
            print(f"  Limited to {limit} sections")
        
        # Create backup directory
        BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        
        if dry_run:
            print(f"\n  [DRY RUN] Would repair {len(corrupt_list)} sections")
            return
        
        # Group by file for efficient updates
        by_file = {}
        for item in corrupt_list:
            fpath = str(item["file"])
            if fpath not in by_file:
                by_file[fpath] = []
            by_file[fpath].append(item)
        
        print(f"\n  Repairing {len(corrupt_list)} sections across {len(by_file)} files...")
        print(f"  Estimated time: {len(corrupt_list) * 2.5 / 60:.0f}-{len(corrupt_list) * 4 / 60:.0f} minutes")
        
        total_repaired = 0
        files_updated = 0
        
        for fpath, items in sorted(by_file.items()):
            f = Path(fpath)
            
            # Backup the file first
            backup_path = BACKUP_DIR / f.name
            if not backup_path.exists():
                shutil.copy2(f, backup_path)
                self.stats["files_backed_up"] += 1
            
            # Load current data
            try:
                data = json.load(open(f, encoding='utf-8'))
            except:
                print(f"  ERROR: Cannot read {f.name}")
                continue
            
            file_changed = False
            
            for item in items:
                idx = item["section_index"]
                section_id = item["section_id"]
                sec_num = item["section_number"]
                
                if not section_id:
                    self.stats["repair_error"] += 1
                    continue
                
                self.stats["repair_attempted"] += 1
                
                # Fetch from PLS
                html, text = self.fetch_section_content(section_id)
                
                if text and not is_corrupt(text):
                    # Success! Update the section
                    data["sections"][idx]["text"] = text
                    # Also store raw HTML if there's a field for it
                    if "html" in data["sections"][idx]:
                        data["sections"][idx]["html"] = html
                    
                    self.stats["repair_success"] += 1
                    file_changed = True
                    total_repaired += 1
                else:
                    # PLS still returned -1
                    self.stats["repair_failed"] += 1
                
                # Progress
                total_done = self.stats["repair_success"] + self.stats["repair_failed"] + self.stats["repair_error"]
                if total_done % 25 == 0:
                    print(f"    Progress: {total_done}/{len(corrupt_list)} "
                          f"(fixed: {self.stats['repair_success']}, "
                          f"still-bad: {self.stats['repair_failed']}, "
                          f"errors: {self.stats['repair_error']})")
            
            # Save updated file
            if file_changed:
                # Update scraped_at timestamp
                data["scraped_at"] = datetime.now().isoformat()
                
                with open(f, 'w', encoding='utf-8') as fh:
                    json.dump(data, fh, indent=2, ensure_ascii=False)
                
                self.stats["files_updated"] += 1
                files_updated += 1
                
                # Also regenerate readable HTML if available
                try:
                    from generate_legislation_html import generate_statute_html
                    generate_statute_html(f)
                except:
                    pass
        
        # Print final stats
        print(f"\n{'='*60}")
        print(f"REPAIR COMPLETE — LETTER {letter}")
        print(f"{'='*60}")
        print(f"  Scanned files:      {self.stats['scanned_files']}")
        print(f"  Scanned sections:   {self.stats['scanned_sections']}")
        print(f"  Corrupt found:      {self.stats['corrupt_found']}")
        print(f"  Repair attempted:   {self.stats['repair_attempted']}")
        print(f"  FIXED:              {self.stats['repair_success']}")
        print(f"  Still corrupt:      {self.stats['repair_failed']} (PLS returned -1 again)")
        print(f"  Errors:             {self.stats['repair_error']}")
        print(f"  Files updated:      {self.stats['files_updated']}")
        print(f"  Files backed up:    {self.stats['files_backed_up']}")
        print(f"  PLS requests:       {self.request_count}")
        
        success_rate = (self.stats['repair_success'] / self.stats['repair_attempted'] * 100) if self.stats['repair_attempted'] else 0
        print(f"  Success rate:       {success_rate:.1f}%")
        
        # Save repair report
        report_path = REPORT_DIR / f"repair_{letter}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.stats, f, indent=2)
        print(f"  Report saved to:    {report_path}")


def main():
    args = sys.argv[1:]
    dry_run = "--dry" in args
    limit = 0
    
    if "--limit" in args:
        idx = args.index("--limit")
        if idx + 1 < len(args):
            limit = int(args[idx + 1])
            args.remove(args[idx + 1])
        args.remove("--limit")
    
    if "--dry" in args:
        args.remove("--dry")
    
    letters = [a.upper() for a in args] if args else ["A"]
    
    repairer = SectionRepairer()
    if not repairer.login():
        print("FATAL: Cannot log in to PLS")
        sys.exit(1)
    
    for letter in letters:
        repairer.repair_letter(letter, dry_run=dry_run, limit=limit)


if __name__ == "__main__":
    main()
