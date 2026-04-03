#!/usr/bin/env python3
"""
Fix Bad Sections Script
========================
Scans all statute JSONs for sections with error text/empty content and re-fetches them.

Problem: Some sections were saved with error responses ("-1", "1", empty) instead of actual content.

Features:
- Scans all statutes for bad sections (error text, too short, empty)
- Re-fetches specific sections from PLS (not entire statute)
- Updates JSON files with corrected content
- Regenerates HTML after fixing
- Tracks progress and can resume

Usage:
    python fix_bad_sections.py scan                    # Scan and report bad sections
    python fix_bad_sections.py fix                     # Fix all bad sections
    python fix_bad_sections.py fix --letter A          # Fix only letter A
    python fix_bad_sections.py fix --limit 50          # Fix first 50 bad sections
    python fix_bad_sections.py fix --statute "Act Name" # Fix specific statute
"""

import os
import re
import json
import time
import random
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from html_cleaner import strip_html_to_text, generate_statute_slug
from case_link_enricher import enrich_case_links

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
HTML_DIR = DATA_DIR / "html" / "statutes"
PROGRESS_FILE = DATA_DIR / "fix_sections_progress.json"

PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing
MIN_DELAY = 3.0
MAX_DELAY = 8.0
RATE_LIMIT_BACKOFF = 60
SECTION_RETRY_DELAY = 5.0

# Break simulation
REQUESTS_BEFORE_BREAK = 25
BREAK_MIN = 30
BREAK_MAX = 90

# PLS Operating Hours (PKT = UTC+5)
PLS_OPEN_HOUR = 7
PLS_CLOSE_HOUR = 21
PKT_OFFSET = timedelta(hours=5)

# Bad content detection
ERROR_RESPONSES = ["-1", "1", "-2", "error", "null", "undefined"]
MIN_VALID_CONTENT_LENGTH = 50  # Minimum chars for valid section content

ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

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
class BadSection:
    """Represents a section that needs to be re-fetched."""
    statute_file: Path
    statute_title: str
    section_index: int
    section_number: str
    section_id: str
    current_text: str
    reason: str  # "error_response", "too_short", "empty"


@dataclass
class ScanResult:
    """Result of scanning for bad sections."""
    total_statutes: int = 0
    affected_statutes: int = 0
    total_sections: int = 0
    bad_sections: int = 0
    bad_section_list: List[BadSection] = field(default_factory=list)
    by_letter: Dict[str, int] = field(default_factory=dict)
    by_reason: Dict[str, int] = field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════
# Detection Functions
# ══════════════════════════════════════════════════════════════════════════════

def is_error_response(text: str) -> bool:
    """Check if text is an error response from PLS."""
    if not text:
        return True
    
    text_stripped = text.strip().lower()
    
    # Check for known error responses
    for error in ERROR_RESPONSES:
        if text_stripped == error.lower():
            return True
    
    # Check for very short responses that look like errors
    if len(text_stripped) <= 3 and text_stripped.isdigit():
        return True
    
    return False


def is_content_too_short(text: str) -> bool:
    """Check if content is suspiciously short."""
    if not text:
        return True
    
    # Strip HTML and check length
    clean = strip_html_to_text(text) if '<' in text else text
    return len(clean.strip()) < MIN_VALID_CONTENT_LENGTH


def detect_bad_section(section: Dict) -> Optional[str]:
    """
    Detect if a section has bad content.
    
    Returns:
        Reason string if bad, None if OK
    """
    text = section.get("text", "")
    text_raw = section.get("text_raw", "")
    
    # Use raw if available, otherwise use text
    content = text_raw if text_raw else text
    
    if not content or content.strip() == "":
        return "empty"
    
    if is_error_response(content):
        return "error_response"
    
    # Check the clean text version
    if text and is_error_response(text):
        return "error_response"
    
    # Check if content is too short (might be partial/failed fetch)
    if is_content_too_short(content):
        # But some sections legitimately have short content
        # Check if it looks like an error
        if len(content.strip()) < 10:
            return "too_short"
    
    return None


def scan_statute_for_bad_sections(json_file: Path) -> List[BadSection]:
    """Scan a single statute JSON for bad sections."""
    bad_sections = []
    
    try:
        data = json.loads(json_file.read_text(encoding='utf-8'))
    except Exception as e:
        logger.warning(f"Failed to read {json_file}: {e}")
        return bad_sections
    
    sections = data.get("sections", [])
    title = data.get("title", json_file.stem)
    
    for i, section in enumerate(sections):
        reason = detect_bad_section(section)
        if reason:
            bad_sections.append(BadSection(
                statute_file=json_file,
                statute_title=title,
                section_index=i,
                section_number=section.get("number", f"#{i}"),
                section_id=section.get("section_id", ""),
                current_text=section.get("text", "")[:100],
                reason=reason
            ))
    
    return bad_sections


def scan_all_statutes(letters: List[str] = None) -> ScanResult:
    """Scan all statutes for bad sections."""
    result = ScanResult()
    
    if letters is None:
        letters = ALPHABETS
    
    for letter in letters:
        letter_dir = LEGISLATION_DIR / letter
        if not letter_dir.exists():
            continue
        
        letter_bad = 0
        
        for json_file in sorted(letter_dir.glob("*.json")):
            result.total_statutes += 1
            
            try:
                data = json.loads(json_file.read_text(encoding='utf-8'))
                sections = data.get("sections", [])
                result.total_sections += len(sections)
            except:
                continue
            
            bad_sections = scan_statute_for_bad_sections(json_file)
            
            if bad_sections:
                result.affected_statutes += 1
                result.bad_sections += len(bad_sections)
                result.bad_section_list.extend(bad_sections)
                letter_bad += len(bad_sections)
                
                for bs in bad_sections:
                    result.by_reason[bs.reason] = result.by_reason.get(bs.reason, 0) + 1
        
        if letter_bad > 0:
            result.by_letter[letter] = letter_bad
    
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Fixing Functions
# ══════════════════════════════════════════════════════════════════════════════

class SectionFixer:
    """Fixes bad sections by re-fetching from PLS."""
    
    def __init__(self, ignore_hours: bool = True):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.ignore_hours = ignore_hours
        self.requests_since_break = 0
        self.progress = self._load_progress()
    
    def _load_progress(self) -> Dict:
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
            except:
                pass
        return {
            "fixed_sections": [],
            "failed_sections": [],
            "total_fixed": 0,
            "last_updated": None
        }
    
    def _save_progress(self):
        self.progress["last_updated"] = datetime.now().isoformat()
        PROGRESS_FILE.write_text(json.dumps(self.progress, indent=2), encoding='utf-8')
    
    def _is_pls_open(self) -> bool:
        if self.ignore_hours:
            return True
        
        utc_now = datetime.now(timezone.utc)
        pkt_now = utc_now + PKT_OFFSET
        return PLS_OPEN_HOUR <= pkt_now.hour < PLS_CLOSE_HOUR
    
    def _wait_for_pls_open(self):
        while not self._is_pls_open():
            utc_now = datetime.now(timezone.utc)
            pkt_now = utc_now + PKT_OFFSET
            logger.info(f"PLS closed (PKT: {pkt_now.strftime('%H:%M')}). Waiting...")
            time.sleep(300)
    
    def _maybe_take_break(self):
        self.requests_since_break += 1
        if self.requests_since_break >= REQUESTS_BEFORE_BREAK:
            break_time = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"Taking a {break_time:.0f}s break...")
            time.sleep(break_time)
            self.requests_since_break = 0
    
    def _create_session(self) -> Session:
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "X-Requested-With": "XMLHttpRequest",
        })
        return session
    
    def _human_delay(self, min_s: float = None, max_s: float = None):
        min_s = min_s or MIN_DELAY
        max_s = max_s or MAX_DELAY
        delay = random.uniform(min_s, max_s)
        time.sleep(delay)
    
    def login(self) -> bool:
        if not self._is_pls_open():
            self._wait_for_pls_open()
        
        logger.info("Logging in to PLS...")
        self.session = self._create_session()
        
        resp = self.session.get(f"{BASE_URL}/", timeout=30)
        if not resp or resp.status_code != 200:
            return False
        
        csrf_match = re.search(
            r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
            resp.text
        )
        if not csrf_match:
            return False
        
        self._human_delay(2, 4)
        
        login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
            "__RequestVerificationToken": csrf_match.group(1)
        }, timeout=30)
        
        if not login_resp or login_resp.status_code != 200:
            return False
        
        self._human_delay(2, 3)
        
        check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if not check_resp or "pakistanlaws" not in check_resp.text.lower():
            return False
        
        self.logged_in = True
        self.requests_since_break = 0
        logger.info("[OK] Login successful!")
        return True
    
    def fetch_section_content(self, section_id: str, retries: int = 3) -> Tuple[str, str]:
        """
        Fetch section content with retries.
        
        Returns:
            (raw_html, clean_text) or ("", "") if failed
        """
        if not section_id:
            return "", ""
        
        for attempt in range(retries):
            try:
                if not self._is_pls_open():
                    self._wait_for_pls_open()
                    self.logged_in = False
                    if not self.login():
                        return "", ""
                
                self._maybe_take_break()
                
                # Enforce delay
                elapsed = time.time() - self.last_request_time
                if elapsed < MIN_DELAY:
                    time.sleep(MIN_DELAY - elapsed)
                
                resp = self.session.post(
                    f"{BASE_URL}/Login/SearchStatueFile",
                    data={"caseTypeId": section_id},
                    timeout=30
                )
                
                self.last_request_time = time.time()
                self.request_count += 1
                
                if not resp or resp.status_code != 200:
                    logger.warning(f"  Attempt {attempt + 1}/{retries}: HTTP {resp.status_code if resp else 'None'}")
                    time.sleep(SECTION_RETRY_DELAY * (attempt + 1))
                    continue
                
                raw_html = resp.text
                
                # Check if it's an error response
                if is_error_response(raw_html):
                    logger.warning(f"  Attempt {attempt + 1}/{retries}: Error response '{raw_html[:50]}'")
                    time.sleep(SECTION_RETRY_DELAY * (attempt + 1))
                    continue
                
                # Success!
                clean_text = strip_html_to_text(raw_html)
                
                # Validate content
                if is_content_too_short(clean_text) and len(clean_text) < 20:
                    logger.warning(f"  Attempt {attempt + 1}/{retries}: Content too short ({len(clean_text)} chars)")
                    time.sleep(SECTION_RETRY_DELAY * (attempt + 1))
                    continue
                
                return raw_html, clean_text
                
            except Exception as e:
                logger.warning(f"  Attempt {attempt + 1}/{retries}: {e}")
                time.sleep(SECTION_RETRY_DELAY * (attempt + 1))
        
        return "", ""
    
    def fetch_section_case_links(self, case_type_id: str) -> List[str]:
        """Fetch case links for a section."""
        if not case_type_id:
            return []
        
        try:
            resp = self.session.post(
                f"{BASE_URL}/Login/GetStatuteCaseLaw",
                data={"caseTypeId": case_type_id, "subTopic": ""},
                timeout=30
            )
            
            if not resp or len(resp.text) < 50:
                return []
            
            # Extract citations
            pattern = r'(\d{4})\s+(PLD|SCMR|CLC|PCrLJ|MLD|YLR|PTD|PLC|CLD|GBLR)\s+(\d+)'
            matches = re.findall(pattern, resp.text, re.IGNORECASE)
            
            citations = []
            seen = set()
            for year, reporter, page in matches:
                citation = f"{year} {reporter.upper()} {page}"
                if citation not in seen:
                    seen.add(citation)
                    citations.append(citation)
            
            return citations
            
        except Exception as e:
            logger.warning(f"Failed to fetch case links: {e}")
            return []
    
    def fix_bad_section(self, bad_section: BadSection) -> bool:
        """Fix a single bad section."""
        # Check if already processed
        key = f"{bad_section.statute_file.name}:{bad_section.section_number}"
        if key in self.progress.get("fixed_sections", []):
            logger.debug(f"Already fixed: {key}")
            return True
        if key in self.progress.get("failed_sections", []):
            logger.debug(f"Previously failed: {key}")
            return False
        
        logger.info(f"Fixing: {bad_section.statute_title[:40]} - Section {bad_section.section_number}")
        
        # Fetch new content
        self._human_delay()
        raw_html, clean_text = self.fetch_section_content(bad_section.section_id)
        
        if not raw_html or is_error_response(raw_html):
            logger.warning(f"  Failed to fetch valid content")
            self.progress["failed_sections"].append(key)
            self._save_progress()
            return False
        
        # Load statute JSON
        try:
            data = json.loads(bad_section.statute_file.read_text(encoding='utf-8'))
        except Exception as e:
            logger.error(f"  Failed to load JSON: {e}")
            return False
        
        # Update section
        section = data["sections"][bad_section.section_index]
        section["text"] = clean_text
        section["text_raw"] = raw_html
        
        # Update case links if we have section_id
        # (We'd need case_type_id which we don't have stored - skip for now)
        
        # Recalculate full_text and full_text_raw
        full_text_parts = []
        full_text_raw_parts = []
        for sec in data["sections"]:
            if sec.get("text"):
                full_text_parts.append(f"[Section {sec.get('number', '')}]\n{sec['text']}")
            if sec.get("text_raw"):
                full_text_raw_parts.append(sec["text_raw"])
        
        data["full_text"] = "\n\n".join(full_text_parts)
        data["full_text_raw"] = "\n\n<!-- SECTION BREAK -->\n\n".join(full_text_raw_parts)
        
        # Save updated JSON
        bad_section.statute_file.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
        
        # Regenerate HTML
        try:
            from generate_legislation_html import generate_statute_html
            html_content = generate_statute_html(data)
            letter = data.get("alphabet", bad_section.statute_file.parent.name)
            html_path = HTML_DIR / letter / f"{bad_section.statute_file.stem}.html"
            html_path.parent.mkdir(parents=True, exist_ok=True)
            html_path.write_text(html_content, encoding='utf-8')
        except Exception as e:
            logger.warning(f"  Failed to regenerate HTML: {e}")
        
        # Mark as fixed
        self.progress["fixed_sections"].append(key)
        self.progress["total_fixed"] += 1
        self._save_progress()
        
        logger.info(f"  [OK] Fixed ({len(clean_text)} chars)")
        return True
    
    def fix_all_bad_sections(self, bad_sections: List[BadSection], limit: int = None) -> Tuple[int, int]:
        """
        Fix all bad sections.
        
        Returns:
            (fixed_count, failed_count)
        """
        if not self.logged_in:
            if not self.login():
                logger.error("Failed to login")
                return 0, 0
        
        if limit:
            bad_sections = bad_sections[:limit]
        
        fixed = 0
        failed = 0
        
        for i, bad_section in enumerate(bad_sections):
            try:
                if not self._is_pls_open():
                    self._wait_for_pls_open()
                    self.logged_in = False
                    if not self.login():
                        break
                
                success = self.fix_bad_section(bad_section)
                if success:
                    fixed += 1
                else:
                    failed += 1
                
                if (i + 1) % 10 == 0:
                    logger.info(f"Progress: {i + 1}/{len(bad_sections)} ({fixed} fixed, {failed} failed)")
                
            except KeyboardInterrupt:
                logger.info("Interrupted. Saving progress...")
                self._save_progress()
                break
            except Exception as e:
                logger.error(f"Error fixing section: {e}")
                failed += 1
                time.sleep(RATE_LIMIT_BACKOFF)
        
        logger.info(f"Complete: {fixed} fixed, {failed} failed")
        return fixed, failed


# ══════════════════════════════════════════════════════════════════════════════
# Reporting
# ══════════════════════════════════════════════════════════════════════════════

def print_scan_report(result: ScanResult):
    """Print scan results."""
    print("\n" + "=" * 60)
    print("BAD SECTIONS SCAN REPORT")
    print("=" * 60)
    print(f"Generated: {datetime.now().isoformat()}")
    print()
    print(f"Total statutes scanned:    {result.total_statutes}")
    print(f"Affected statutes:         {result.affected_statutes} ({result.affected_statutes/max(result.total_statutes,1)*100:.1f}%)")
    print(f"Total sections:            {result.total_sections}")
    print(f"Bad sections:              {result.bad_sections} ({result.bad_sections/max(result.total_sections,1)*100:.1f}%)")
    
    if result.by_reason:
        print("\n" + "-" * 40)
        print("BY REASON:")
        print("-" * 40)
        for reason, count in sorted(result.by_reason.items(), key=lambda x: -x[1]):
            print(f"  {reason}: {count}")
    
    if result.by_letter:
        print("\n" + "-" * 40)
        print("BY LETTER:")
        print("-" * 40)
        for letter, count in sorted(result.by_letter.items()):
            print(f"  {letter}: {count} bad sections")
    
    if result.bad_section_list and len(result.bad_section_list) <= 30:
        print("\n" + "-" * 40)
        print("ALL BAD SECTIONS:")
        print("-" * 40)
        for bs in result.bad_section_list:
            print(f"  {bs.statute_title[:40]} - Section {bs.section_number} ({bs.reason})")
    elif result.bad_section_list:
        print("\n" + "-" * 40)
        print("SAMPLE BAD SECTIONS (first 20):")
        print("-" * 40)
        for bs in result.bad_section_list[:20]:
            print(f"  {bs.statute_title[:40]} - Section {bs.section_number} ({bs.reason})")
        print(f"  ... and {len(result.bad_section_list) - 20} more")
    
    print("=" * 60)


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Fix bad sections in legislation data")
    parser.add_argument("command", choices=["scan", "fix", "status"],
                        help="Command to run")
    parser.add_argument("--letter", "-l", help="Only process specific letter")
    parser.add_argument("--limit", "-n", type=int, help="Limit number of sections to fix")
    parser.add_argument("--statute", "-s", help="Fix specific statute by name")
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7)")
    parser.add_argument("--save-report", help="Save scan report to file")
    
    args = parser.parse_args()
    
    if args.command == "scan":
        letters = [args.letter.upper()] if args.letter else None
        logger.info("Scanning for bad sections...")
        result = scan_all_statutes(letters)
        print_scan_report(result)
        
        if args.save_report:
            report = {
                "generated_at": datetime.now().isoformat(),
                "total_statutes": result.total_statutes,
                "affected_statutes": result.affected_statutes,
                "total_sections": result.total_sections,
                "bad_sections": result.bad_sections,
                "by_reason": result.by_reason,
                "by_letter": result.by_letter,
                "bad_section_list": [
                    {
                        "file": str(bs.statute_file),
                        "title": bs.statute_title,
                        "section": bs.section_number,
                        "reason": bs.reason
                    }
                    for bs in result.bad_section_list
                ]
            }
            Path(args.save_report).write_text(json.dumps(report, indent=2), encoding='utf-8')
            logger.info(f"Report saved to {args.save_report}")
    
    elif args.command == "fix":
        letters = [args.letter.upper()] if args.letter else None
        
        logger.info("Scanning for bad sections...")
        result = scan_all_statutes(letters)
        
        if not result.bad_section_list:
            logger.info("No bad sections found!")
            return
        
        logger.info(f"Found {len(result.bad_section_list)} bad sections in {result.affected_statutes} statutes")
        
        # Filter by statute name if specified
        if args.statute:
            result.bad_section_list = [
                bs for bs in result.bad_section_list
                if args.statute.lower() in bs.statute_title.lower()
            ]
            logger.info(f"Filtered to {len(result.bad_section_list)} sections matching '{args.statute}'")
        
        fixer = SectionFixer(ignore_hours=(not args.respect_hours))
        fixed, failed = fixer.fix_all_bad_sections(result.bad_section_list, limit=args.limit)
        
        print(f"\nResult: {fixed} fixed, {failed} failed")
    
    elif args.command == "status":
        if PROGRESS_FILE.exists():
            progress = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
            print(f"Fixed sections: {len(progress.get('fixed_sections', []))}")
            print(f"Failed sections: {len(progress.get('failed_sections', []))}")
            print(f"Total fixed: {progress.get('total_fixed', 0)}")
            print(f"Last updated: {progress.get('last_updated', 'Never')}")
        else:
            print("No progress file found. Run 'fix' to start.")
        
        # Also run a quick scan
        logger.info("Running quick scan...")
        result = scan_all_statutes()
        print(f"\nCurrent bad sections: {result.bad_sections}")
        print(f"Affected statutes: {result.affected_statutes}/{result.total_statutes}")


if __name__ == "__main__":
    main()
