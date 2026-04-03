#!/usr/bin/env python3
"""
PLS Legislation Scraper - Bulletproof Edition
==============================================
Robust scraper with:
- Retry logic with exponential backoff (5s, 15s, 45s)
- Content validation (rejects "-1" errors and short content)
- Progress tracking after EVERY statute
- Full error recovery with traceback logging
- Multi-letter continuation (--start A --end Z)
- Night shift awareness (10PM-5AM PKT by default)
- Batch retry of failed sections at end
- Comprehensive reporting

Usage:
    python bulletproof_scraper.py --start A --end Z
    python bulletproof_scraper.py --letter A --limit 5  # test
    python bulletproof_scraper.py --ignore-hours        # daytime testing
    python bulletproof_scraper.py --until-time "05:00"  # stop at 5 AM PKT
    python bulletproof_scraper.py --resume              # continue from last position
"""

import os
import re
import sys
import json
import time
import random
import hashlib
import logging
import traceback
from pathlib import Path
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Any, Tuple
from collections import defaultdict

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from html_cleaner import (
    strip_html_to_text, 
    clean_statute_html, 
    extract_preamble,
    normalize_citation,
    extract_case_citations,
    generate_statute_slug
)
from case_link_enricher import enrich_case_links

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
HTML_DIR = DATA_DIR / "html" / "statutes"
PROGRESS_FILE = LEGISLATION_DIR / "bulletproof_progress.json"
REPORTS_DIR = Path(__file__).parent / "reports"
STATUTES_JSONL = LEGISLATION_DIR / "all_statutes.jsonl"
LINKS_FILE = LEGISLATION_DIR / "statute_case_links.jsonl"

# Credentials
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Timing (human-like)
MIN_DELAY = 3.0
MAX_DELAY = 8.0
LOGIN_DELAY = 5.0
RATE_LIMIT_BACKOFF = 60
READING_DELAY_MIN = 2.0
READING_DELAY_MAX = 6.0

# Retry configuration
SECTION_RETRY_DELAYS = [5, 15, 45]  # Exponential backoff for sections
MAX_SECTION_RETRIES = 3
MIN_VALID_CONTENT_LENGTH = 10

# Break simulation
REQUESTS_BEFORE_BREAK = 30
BREAK_MIN = 30
BREAK_MAX = 90

# PLS Operating Hours (PKT = UTC+5) - NIGHT SHIFT
PLS_OPEN_HOUR = 22   # 10 PM PKT
PLS_CLOSE_HOUR = 5   # 5 AM PKT
PKT_OFFSET = timedelta(hours=5)

# Alphabet list
ALPHABETS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

# ══════════════════════════════════════════════════════════════════════════════
# Logging Setup
# ══════════════════════════════════════════════════════════════════════════════

def setup_logging(log_file: Path = None):
    """Setup dual logging (console + file)."""
    log_format = '%(asctime)s | %(levelname)-7s | %(message)s'
    date_format = '%H:%M:%S'
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
        force=True
    )
    
    return logging.getLogger(__name__)

# Initialize logger
logger = setup_logging()


# ══════════════════════════════════════════════════════════════════════════════
# Data Classes
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class Section:
    """Represents a section of a statute."""
    number: str
    title: str = ""
    text: str = ""           # Clean text
    text_raw: str = ""       # Raw HTML
    cases_cited: List[Dict] = field(default_factory=list)
    section_id: str = ""
    fetch_status: str = "ok"  # ok, failed, skipped


@dataclass
class Statute:
    """Represents a complete statute with new schema."""
    citation: str
    statute_id: str
    title: str
    short_title: str = ""
    alphabet: str = ""
    jurisdiction: str = "Federal"
    enactment_date: str = ""
    status: str = "in_force"
    preamble: str = ""
    preamble_raw: str = ""
    full_text: str = ""
    full_text_raw: str = ""
    sections: List[Section] = field(default_factory=list)
    statutes_cited: List[str] = field(default_factory=list)
    cases_cited: List[Dict] = field(default_factory=list)
    fetched_at: str = ""
    source_url: str = ""
    scrape_status: str = "complete"  # complete, partial, failed
    failed_sections: List[Dict] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.fetched_at:
            self.fetched_at = datetime.now().isoformat()
        if not self.statute_id:
            self.statute_id = hashlib.md5(self.title.encode()).hexdigest()[:12]
    
    def to_dict(self) -> Dict:
        return {
            "citation": self.citation,
            "statute_id": self.statute_id,
            "title": self.title,
            "short_title": self.short_title,
            "alphabet": self.alphabet,
            "jurisdiction": self.jurisdiction,
            "enactment_date": self.enactment_date,
            "status": self.status,
            "preamble": self.preamble,
            "preamble_raw": self.preamble_raw,
            "full_text": self.full_text,
            "full_text_raw": self.full_text_raw,
            "sections": [asdict(s) if isinstance(s, Section) else s for s in self.sections],
            "statutes_cited": self.statutes_cited,
            "cases_cited": self.cases_cited,
            "fetched_at": self.fetched_at,
            "source_url": self.source_url,
            "scrape_status": self.scrape_status,
            "failed_sections": self.failed_sections,
        }


# ══════════════════════════════════════════════════════════════════════════════
# Progress Manager
# ══════════════════════════════════════════════════════════════════════════════

class ProgressManager:
    """Manages scraping progress with persistence."""
    
    def __init__(self, progress_file: Path = PROGRESS_FILE):
        self.progress_file = progress_file
        self.progress = self._load()
    
    def _load(self) -> Dict:
        """Load progress from file."""
        if self.progress_file.exists():
            try:
                return json.loads(self.progress_file.read_text(encoding='utf-8'))
            except Exception as e:
                logger.warning(f"Failed to load progress: {e}")
        
        return {
            "scraped": [],           # Successfully scraped statute names
            "failed": [],            # Statutes that failed completely
            "pending": [],           # Statutes queued but not yet processed
            "incomplete": {},        # Statutes with some failed sections
            "current_letter": None,  # Current alphabet letter
            "completed_letters": [], # Fully completed letters
            "stats": {
                "total_scraped": 0,
                "total_failed": 0,
                "total_sections": 0,
                "failed_sections": 0,
            },
            "last_updated": None,
            "session_start": None,
            "failed_section_queue": [],  # Sections to retry at end
        }
    
    def save(self):
        """Save progress to file."""
        self.progress["last_updated"] = datetime.now().isoformat()
        self.progress_file.parent.mkdir(parents=True, exist_ok=True)
        self.progress_file.write_text(
            json.dumps(self.progress, indent=2, ensure_ascii=False),
            encoding='utf-8'
        )
    
    def mark_scraped(self, statute_name: str, section_count: int = 0, failed_count: int = 0):
        """Mark statute as successfully scraped."""
        if statute_name not in self.progress["scraped"]:
            self.progress["scraped"].append(statute_name)
        if statute_name in self.progress["failed"]:
            self.progress["failed"].remove(statute_name)
        self.progress["stats"]["total_scraped"] += 1
        self.progress["stats"]["total_sections"] += section_count
        self.progress["stats"]["failed_sections"] += failed_count
        self.save()
    
    def mark_failed(self, statute_name: str, error: str):
        """Mark statute as failed."""
        if statute_name not in self.progress["failed"]:
            self.progress["failed"].append(statute_name)
        self.progress["incomplete"][statute_name] = {
            "error": error,
            "timestamp": datetime.now().isoformat()
        }
        self.progress["stats"]["total_failed"] += 1
        self.save()
    
    def mark_incomplete(self, statute_name: str, failed_sections: List[Dict]):
        """Mark statute as incomplete (some sections failed)."""
        self.progress["incomplete"][statute_name] = {
            "failed_sections": failed_sections,
            "timestamp": datetime.now().isoformat()
        }
        self.save()
    
    def queue_failed_section(self, statute_name: str, section_info: Dict):
        """Queue a failed section for batch retry."""
        self.progress["failed_section_queue"].append({
            "statute": statute_name,
            "section": section_info,
            "timestamp": datetime.now().isoformat()
        })
        self.save()
    
    def is_scraped(self, statute_name: str) -> bool:
        """Check if statute already scraped."""
        return statute_name in self.progress["scraped"]
    
    def set_current_letter(self, letter: str):
        """Set current processing letter."""
        self.progress["current_letter"] = letter
        self.save()
    
    def complete_letter(self, letter: str):
        """Mark letter as completed."""
        if letter not in self.progress["completed_letters"]:
            self.progress["completed_letters"].append(letter)
        self.progress["current_letter"] = None
        self.save()
    
    def is_letter_done(self, letter: str) -> bool:
        """Check if letter is fully completed."""
        return letter in self.progress["completed_letters"]
    
    def get_resume_point(self) -> Tuple[Optional[str], List[str]]:
        """Get resume point: (letter, already_scraped_for_that_letter)."""
        current = self.progress.get("current_letter")
        if current:
            return current, self.progress["scraped"]
        
        # Find first incomplete letter
        for letter in ALPHABETS:
            if letter not in self.progress["completed_letters"]:
                return letter, self.progress["scraped"]
        
        return None, []
    
    def start_session(self):
        """Mark session start."""
        self.progress["session_start"] = datetime.now().isoformat()
        self.save()
    
    def get_stats(self) -> Dict:
        """Get current stats."""
        return self.progress["stats"].copy()


# ══════════════════════════════════════════════════════════════════════════════
# Report Generator
# ══════════════════════════════════════════════════════════════════════════════

class ReportGenerator:
    """Generates scraping reports."""
    
    def __init__(self, reports_dir: Path = REPORTS_DIR):
        self.reports_dir = reports_dir
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        self.session_stats = defaultdict(int)
        self.session_start = datetime.now()
    
    def log_statute(self, statute_name: str, success: bool, sections: int = 0, failed_sections: int = 0):
        """Log a statute scrape result."""
        if success:
            self.session_stats["scraped"] += 1
            self.session_stats["total_sections"] += sections
            self.session_stats["failed_sections"] += failed_sections
        else:
            self.session_stats["failed"] += 1
    
    def log_skip(self, statute_name: str):
        """Log a skipped statute."""
        self.session_stats["skipped"] += 1
    
    def print_summary(self, interval: int = 10):
        """Print progress summary."""
        total = self.session_stats["scraped"] + self.session_stats["failed"] + self.session_stats["skipped"]
        if total > 0 and total % interval == 0:
            elapsed = datetime.now() - self.session_start
            rate = total / max(elapsed.total_seconds() / 3600, 0.001)
            
            logger.info("=" * 60)
            logger.info(f"📊 PROGRESS SUMMARY (after {total} statutes)")
            logger.info(f"   ✅ Scraped: {self.session_stats['scraped']}")
            logger.info(f"   ❌ Failed:  {self.session_stats['failed']}")
            logger.info(f"   ⏭️  Skipped: {self.session_stats['skipped']}")
            logger.info(f"   📄 Sections: {self.session_stats['total_sections']} ({self.session_stats['failed_sections']} failed)")
            logger.info(f"   ⏱️  Rate: {rate:.1f} statutes/hour")
            logger.info("=" * 60)
    
    def save_report(self, progress: ProgressManager = None):
        """Save final report to JSON."""
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        report_file = self.reports_dir / f"{timestamp}.json"
        
        elapsed = datetime.now() - self.session_start
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "session_duration_minutes": elapsed.total_seconds() / 60,
            "summary": {
                "scraped": self.session_stats["scraped"],
                "failed": self.session_stats["failed"],
                "skipped": self.session_stats["skipped"],
                "total_sections": self.session_stats["total_sections"],
                "failed_sections": self.session_stats["failed_sections"],
            },
            "rate_per_hour": self.session_stats["scraped"] / max(elapsed.total_seconds() / 3600, 0.001),
        }
        
        if progress:
            report["cumulative"] = progress.get_stats()
            report["completed_letters"] = progress.progress.get("completed_letters", [])
            report["failed_statutes"] = progress.progress.get("failed", [])
            report["incomplete_statutes"] = list(progress.progress.get("incomplete", {}).keys())
        
        report_file.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding='utf-8')
        logger.info(f"📝 Report saved: {report_file}")
        
        return report


# ══════════════════════════════════════════════════════════════════════════════
# HTML Generator
# ══════════════════════════════════════════════════════════════════════════════

def generate_statute_page_html(statute: Dict) -> str:
    """Generate a clean, readable HTML page for a statute."""
    import html as html_module
    
    title = statute.get("title", "Unknown Statute")
    citation = statute.get("citation", title)
    jurisdiction = statute.get("jurisdiction", "Federal")
    year = statute.get("enactment_date", "")
    sections = statute.get("sections", [])
    preamble = statute.get("preamble", "")
    cases_cited = statute.get("cases_cited", [])
    
    sections_html = []
    toc_items = []
    
    for sec in sections:
        sec_num = sec.get("number", "")
        sec_title = sec.get("title", "")
        sec_text = sec.get("text", "")
        sec_cases = sec.get("cases_cited", [])
        sec_status = sec.get("fetch_status", "ok")
        
        sec_id = re.sub(r'[^a-zA-Z0-9]', '_', str(sec_num or sec_title))
        
        toc_items.append(f'<li><a href="#section-{sec_id}">Section {sec_num}: {html_module.escape(sec_title)}</a></li>')
        
        cases_html = ""
        if sec_cases:
            case_items = []
            for c in sec_cases:
                if isinstance(c, dict):
                    case_items.append(f'<li><a href="#">{html_module.escape(c.get("citation", ""))}</a></li>')
                else:
                    case_items.append(f'<li><a href="#">{html_module.escape(c)}</a></li>')
            cases_html = f'<div class="cited-cases"><strong>Cases:</strong><ul>{"".join(case_items)}</ul></div>'
        
        status_badge = ""
        if sec_status == "failed":
            status_badge = '<span class="status-badge failed">⚠️ Fetch Failed</span>'
        
        sections_html.append(f'''
        <section class="statute-section" id="section-{sec_id}">
            <h3 class="section-header">
                <span class="section-number">{html_module.escape(sec_num)}</span>
                <span class="section-title">{html_module.escape(sec_title)}</span>
                {status_badge}
            </h3>
            <div class="section-content">
                {html_module.escape(sec_text) if sec_text else '<em>Content not available</em>'}
            </div>
            {cases_html}
        </section>
        ''')
    
    preamble_html = ""
    if preamble:
        preamble_html = f'''
        <section class="preamble-section">
            <h2>Preamble</h2>
            <div class="preamble-content">{html_module.escape(preamble)}</div>
        </section>
        '''
    
    return f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html_module.escape(citation)} | Qanoon</title>
    <style>
        body {{ font-family: Georgia, serif; line-height: 1.75; max-width: 900px; margin: 0 auto; padding: 40px 20px; }}
        .header {{ border-bottom: 3px solid #006400; padding-bottom: 25px; margin-bottom: 30px; }}
        .header h1 {{ color: #006400; font-size: 1.5rem; margin: 0 0 10px 0; }}
        .meta {{ display: flex; gap: 20px; color: #4a5568; font-size: 0.95rem; }}
        .meta-item {{ background: #f7fafc; padding: 5px 15px; border-radius: 4px; }}
        .toc {{ background: #f7fafc; padding: 20px; border-radius: 8px; margin-bottom: 30px; }}
        .toc h2 {{ margin-top: 0; color: #006400; }}
        .toc ul {{ columns: 2; list-style: none; padding: 0; }}
        .toc li {{ margin-bottom: 8px; }}
        .toc a {{ color: #228B22; text-decoration: none; }}
        .preamble-section {{ background: #f0fff0; padding: 20px; border-radius: 8px; margin-bottom: 30px; border-left: 4px solid #006400; }}
        .statute-section {{ margin-bottom: 30px; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px; }}
        .section-header {{ display: flex; align-items: center; gap: 15px; margin: 0 0 15px 0; padding-bottom: 10px; border-bottom: 1px solid #e2e8f0; }}
        .section-number {{ background: #006400; color: white; padding: 5px 12px; border-radius: 4px; font-weight: bold; }}
        .section-title {{ font-weight: 600; }}
        .section-content {{ text-align: justify; white-space: pre-wrap; }}
        .cited-cases {{ margin-top: 15px; padding-top: 15px; border-top: 1px dashed #e2e8f0; font-size: 0.9rem; }}
        .cited-cases ul {{ margin: 10px 0 0 0; padding-left: 20px; }}
        .status-badge {{ font-size: 0.8rem; padding: 2px 8px; border-radius: 4px; }}
        .status-badge.failed {{ background: #fff5f5; color: #c53030; }}
    </style>
</head>
<body>
    <header class="header">
        <h1>{html_module.escape(title)}</h1>
        <div class="meta">
            <span class="meta-item">📍 {html_module.escape(jurisdiction)}</span>
            {f'<span class="meta-item">📅 {html_module.escape(year)}</span>' if year else ''}
            <span class="meta-item">📄 {len(sections)} Sections</span>
        </div>
    </header>
    <nav class="toc"><h2>Contents</h2><ul>{''.join(toc_items)}</ul></nav>
    {preamble_html}
    <main>{''.join(sections_html)}</main>
</body>
</html>'''


# ══════════════════════════════════════════════════════════════════════════════
# Bulletproof Scraper
# ══════════════════════════════════════════════════════════════════════════════

class BulletproofScraper:
    """Robust scraper with retry logic and error recovery."""
    
    def __init__(self, ignore_hours: bool = True, until_time: str = None):
        self.session: Optional[Session] = None
        self.logged_in = False
        self.request_count = 0
        self.last_request_time = 0
        self.requests_since_break = 0
        
        self.ignore_hours = ignore_hours
        self.until_time = until_time  # Format: "HH:MM" in PKT
        
        self.progress = ProgressManager()
        self.reporter = ReportGenerator()
        
        # Create directories
        LEGISLATION_DIR.mkdir(parents=True, exist_ok=True)
        HTML_DIR.mkdir(parents=True, exist_ok=True)
        for letter in ALPHABETS:
            (LEGISLATION_DIR / letter).mkdir(exist_ok=True)
            (LEGISLATION_DIR / letter / "original").mkdir(exist_ok=True)
            (HTML_DIR / letter).mkdir(exist_ok=True)
    
    # ── Time Management ───────────────────────────────────────────────────────
    
    def _get_pkt_time(self) -> datetime:
        """Get current PKT time."""
        return datetime.now(timezone.utc) + PKT_OFFSET
    
    def _is_within_operating_hours(self) -> bool:
        """Check if within night shift hours (10 PM - 5 AM PKT)."""
        if self.ignore_hours:
            return True
        
        pkt_now = self._get_pkt_time()
        hour = pkt_now.hour
        
        # Night shift: 22:00 - 05:00
        return hour >= PLS_OPEN_HOUR or hour < PLS_CLOSE_HOUR
    
    def _should_stop_for_time(self) -> bool:
        """Check if we should stop due to --until-time."""
        if not self.until_time:
            return False
        
        try:
            target_hour, target_minute = map(int, self.until_time.split(':'))
            pkt_now = self._get_pkt_time()
            
            # If we're past the target time, stop
            if pkt_now.hour > target_hour or (pkt_now.hour == target_hour and pkt_now.minute >= target_minute):
                return True
        except:
            pass
        
        return False
    
    def _wait_for_operating_hours(self):
        """Wait until operating window opens."""
        while not self._is_within_operating_hours():
            pkt_now = self._get_pkt_time()
            
            # Calculate wait time until 10 PM
            if pkt_now.hour >= PLS_CLOSE_HOUR and pkt_now.hour < PLS_OPEN_HOUR:
                target = pkt_now.replace(hour=PLS_OPEN_HOUR, minute=0, second=0)
                wait_seconds = (target - pkt_now).total_seconds()
            else:
                wait_seconds = 60  # Should not happen
            
            hours, remainder = divmod(int(wait_seconds), 3600)
            minutes = remainder // 60
            
            logger.info(f"⏰ Outside hours (PKT: {pkt_now.strftime('%H:%M')}). Waiting {hours}h {minutes}m...")
            time.sleep(min(300, wait_seconds))  # Sleep in 5 min chunks
    
    def _maybe_take_break(self):
        """Take a random break every N requests."""
        self.requests_since_break += 1
        if self.requests_since_break >= REQUESTS_BEFORE_BREAK:
            break_duration = random.uniform(BREAK_MIN, BREAK_MAX)
            logger.info(f"☕ Taking a {break_duration:.0f}s break...")
            time.sleep(break_duration)
            self.requests_since_break = 0
    
    # ── Session Management ────────────────────────────────────────────────────
    
    def _create_session(self) -> Session:
        """Create a new curl_cffi session with Chrome 120 impersonation."""
        session = Session(impersonate=BrowserType.chrome120)
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Cache-Control": "no-cache",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Upgrade-Insecure-Requests": "1",
            "X-Requested-With": "XMLHttpRequest",
        })
        return session
    
    def _human_delay(self, min_s: float = None, max_s: float = None, reading: bool = False):
        """Wait a random human-like delay."""
        if reading:
            min_s = min_s or READING_DELAY_MIN
            max_s = max_s or READING_DELAY_MAX
        else:
            min_s = min_s or MIN_DELAY
            max_s = max_s or MAX_DELAY
        
        delay = random.uniform(min_s, max_s) + random.gauss(0, 0.5)
        delay = max(1.0, delay)
        time.sleep(delay)
    
    def _request(self, method: str, url: str, retries: int = 3, **kwargs) -> Optional[Any]:
        """Make a request with rate limiting and retries."""
        # Check operating hours
        if not self._is_within_operating_hours():
            self._wait_for_operating_hours()
            self.logged_in = False
            if not self.login():
                return None
        
        # Check stop time
        if self._should_stop_for_time():
            logger.info(f"⏰ Reached --until-time {self.until_time}. Stopping gracefully.")
            raise KeyboardInterrupt("Reached target time")
        
        self._maybe_take_break()
        
        elapsed = time.time() - self.last_request_time
        if elapsed < MIN_DELAY:
            time.sleep(MIN_DELAY - elapsed)
        
        last_error = None
        for attempt in range(retries):
            try:
                if method.upper() == "GET":
                    resp = self.session.get(url, timeout=30, **kwargs)
                else:
                    resp = self.session.post(url, timeout=30, **kwargs)
                
                self.last_request_time = time.time()
                self.request_count += 1
                
                if resp.status_code in [403, 429, 500, 502, 503]:
                    backoff = RATE_LIMIT_BACKOFF * (2 ** attempt)
                    logger.warning(f"⚠️ HTTP {resp.status_code} - backing off {backoff}s...")
                    time.sleep(backoff)
                    continue
                
                if resp.status_code != 200:
                    logger.warning(f"⚠️ Unexpected status {resp.status_code} for {url[:60]}")
                    return None
                
                return resp
                
            except Exception as e:
                last_error = e
                backoff = RATE_LIMIT_BACKOFF * (attempt + 1)
                logger.error(f"❌ Request failed (attempt {attempt + 1}/{retries}): {e}")
                time.sleep(backoff)
        
        logger.error(f"❌ All {retries} attempts failed: {last_error}")
        return None
    
    # ── Login ─────────────────────────────────────────────────────────────────
    
    def login(self) -> bool:
        """Login to PLS with full error handling."""
        if not self._is_within_operating_hours():
            self._wait_for_operating_hours()
        
        logger.info("🔐 Logging in to PLS...")
        
        try:
            self.session = self._create_session()
            
            resp = self.session.get(f"{BASE_URL}/", timeout=30)
            if not resp or resp.status_code != 200:
                logger.error("❌ Failed to load homepage")
                return False
            
            self._human_delay(reading=True)
            
            csrf_match = re.search(
                r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
                resp.text
            )
            if not csrf_match:
                logger.error("❌ CSRF token not found")
                return False
            
            csrf_token = csrf_match.group(1)
            self._human_delay(2, 4)
            
            login_resp = None
            for attempt in range(3):
                try:
                    login_resp = self.session.post(f"{BASE_URL}/Login/Login", data={
                        "Login.UserName": PLS_USER,
                        "Login.Password": PLS_PASS,
                        "__RequestVerificationToken": csrf_token
                    }, timeout=30)
                    if login_resp and login_resp.status_code == 200:
                        break
                    self._human_delay(5, 10)
                except Exception as e:
                    logger.warning(f"Login attempt {attempt + 1} error: {e}")
                    self._human_delay(5, 10)
            
            if not login_resp or login_resp.status_code != 200:
                logger.error("❌ Login request failed")
                return False
            
            self._human_delay(2, 3)
            
            check_resp = self.session.get(f"{BASE_URL}/Login/Check", timeout=30)
            if not check_resp or "pakistanlaws" not in check_resp.text.lower():
                logger.error("❌ Login verification failed")
                return False
            
            self.logged_in = True
            self.requests_since_break = 0
            logger.info("✅ Login successful!")
            self._human_delay(LOGIN_DELAY, LOGIN_DELAY + 3)
            return True
            
        except Exception as e:
            logger.error(f"❌ Login exception: {e}")
            logger.debug(traceback.format_exc())
            return False
    
    # ── Content Validation ────────────────────────────────────────────────────
    
    def _is_error_response(self, text: str) -> bool:
        """Check if response indicates an error."""
        if not text:
            return True
        
        text_stripped = text.strip()
        
        # Known error responses from PLS
        error_patterns = ["-1", "1", "-2", "0", "error", "null", "undefined", ""]
        
        if text_stripped.lower() in error_patterns:
            return True
        
        # Very short numeric responses are likely errors
        if len(text_stripped) <= 5 and text_stripped.lstrip('-').isdigit():
            return True
        
        return False
    
    def _validate_content(self, text: str) -> Tuple[bool, str]:
        """
        Validate section content.
        
        Returns:
            (is_valid, reason)
        """
        if not text:
            return False, "empty"
        
        # Check for error response
        if self._is_error_response(text):
            return False, f"error_response:{text[:20]}"
        
        # Clean HTML to get actual text
        clean_text = strip_html_to_text(text) if '<' in text else text
        
        # Check minimum length
        if len(clean_text.strip()) < MIN_VALID_CONTENT_LENGTH:
            return False, f"too_short:{len(clean_text)}"
        
        return True, "ok"
    
    # ── Section Fetching with Retry ───────────────────────────────────────────
    
    def get_section_content(self, section_id: str) -> Tuple[str, str, str]:
        """
        Get section content with exponential backoff retry.
        
        Returns:
            (raw_html, clean_text, status) where status is "ok", "failed", or error reason
        """
        if not section_id:
            return "", "", "no_section_id"
        
        for attempt, delay in enumerate(SECTION_RETRY_DELAYS):
            try:
                resp = self._request("POST", f"{BASE_URL}/Login/SearchStatueFile",
                                    data={"caseTypeId": section_id})
                
                if not resp:
                    logger.warning(f"   Section {section_id}: No response (attempt {attempt + 1}/{MAX_SECTION_RETRIES})")
                    time.sleep(delay)
                    continue
                
                raw_html = resp.text
                
                # Validate content
                is_valid, reason = self._validate_content(raw_html)
                
                if not is_valid:
                    logger.warning(f"   Section {section_id}: Invalid content ({reason}) (attempt {attempt + 1}/{MAX_SECTION_RETRIES})")
                    time.sleep(delay)
                    continue
                
                # Success!
                clean_text = strip_html_to_text(raw_html)
                return raw_html, clean_text, "ok"
                
            except Exception as e:
                logger.error(f"   Section {section_id}: Exception ({e}) (attempt {attempt + 1}/{MAX_SECTION_RETRIES})")
                logger.debug(traceback.format_exc())
                time.sleep(delay)
        
        # All retries failed
        logger.error(f"   Section {section_id}: All {MAX_SECTION_RETRIES} retries failed")
        return "", "", "max_retries_exceeded"
    
    # ── Statute List ──────────────────────────────────────────────────────────
    
    def get_statutes_by_letter(self, letter: str) -> List[Dict]:
        """Get all statutes starting with a letter."""
        if not self.logged_in:
            if not self.login():
                return []
        
        logger.info(f"📚 Fetching statutes for '{letter}'...")
        
        try:
            resp = self._request("GET", f"{BASE_URL}/Login/StatuecharSearch",
                                params={"character": letter})
            if not resp:
                return []
            
            self._human_delay(reading=True)
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            statutes = []
            
            for row in soup.find_all('tr', class_='caseType'):
                caseid = row.get('casetypeid', '')
                if caseid:
                    statutes.append({
                        "name": caseid.strip(),
                        "alphabet": letter
                    })
            
            logger.info(f"   Found {len(statutes)} statutes")
            return statutes
            
        except Exception as e:
            logger.error(f"❌ Failed to get statutes for '{letter}': {e}")
            logger.debug(traceback.format_exc())
            return []
    
    # ── Statute Sections ──────────────────────────────────────────────────────
    
    def get_statute_sections(self, statute_name: str) -> List[Dict]:
        """Get all sections of a statute."""
        if not self.logged_in:
            if not self.login():
                return []
        
        try:
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
                    act_name = cells[2].get_text(strip=True)
                    definition = cells[3].get_text(strip=True)
                    
                    section_id = read_cell.get('casetypeid', '')
                    if not section_id:
                        link = read_cell.find(class_='readCaseLaw')
                        if link:
                            section_id = link.get('casetypeid', '')
                    
                    case_cell = cells[4] if len(cells) > 4 else None
                    case_id = case_cell.get('casetypeid', '') if case_cell else ''
                    
                    sections.append({
                        "section_id": section_id,
                        "number": section_num,
                        "act_name": act_name,
                        "definition": definition,
                        "case_type_id": case_id,
                    })
            
            return sections
            
        except Exception as e:
            logger.error(f"❌ Failed to get sections for '{statute_name}': {e}")
            logger.debug(traceback.format_exc())
            return []
    
    # ── Case Links ────────────────────────────────────────────────────────────
    
    def get_section_case_links(self, case_type_id: str) -> List[str]:
        """Get case citations for a section."""
        if not case_type_id:
            return []
        
        try:
            resp = self._request("POST", f"{BASE_URL}/Login/GetStatuteCaseLaw",
                                data={"caseTypeId": case_type_id, "subTopic": ""})
            
            if not resp or len(resp.text) < 50:
                return []
            
            citations = extract_case_citations(resp.text)
            normalized = []
            seen = set()
            
            for c in citations:
                norm = normalize_citation(c)
                if norm and norm not in seen:
                    seen.add(norm)
                    normalized.append(norm)
            
            return normalized
            
        except Exception as e:
            logger.debug(f"Failed to get case links: {e}")
            return []
    
    # ── Full Statute Scrape ───────────────────────────────────────────────────
    
    def scrape_statute(self, statute_info: Dict) -> Optional[Tuple[Statute, str]]:
        """
        Scrape a complete statute with full error handling.
        
        Returns:
            (Statute, raw_html) or None if failed
        """
        statute_name = statute_info["name"]
        alphabet = statute_info["alphabet"]
        
        # Skip if already scraped
        if self.progress.is_scraped(statute_name):
            logger.debug(f"⏭️  Skipping {statute_name[:50]} (already scraped)")
            self.reporter.log_skip(statute_name)
            return None
        
        logger.info(f"📜 Scraping: {statute_name[:60]}")
        
        try:
            self._human_delay(1, 2)
            sections = self.get_statute_sections(statute_name)
            
            if not sections:
                logger.warning(f"   No sections found")
                self.progress.mark_scraped(statute_name, 0, 0)
                return None
            
            # Create statute object
            statute = Statute(
                citation=statute_name,
                statute_id="",
                title=statute_name,
                alphabet=alphabet,
                source_url=f"{BASE_URL}/Login/GetStatuesSearch?caseName={statute_name}",
            )
            
            # Extract year
            year_match = re.search(r'(\d{4})$', statute_name)
            if year_match:
                statute.enactment_date = year_match.group(1)
                statute.short_title = statute_name[:statute_name.rfind(year_match.group(1))].strip()
            else:
                statute.short_title = statute_name
            
            # Determine jurisdiction
            jurisdiction_keywords = {
                "Federal": ["Federal", "Pakistan", "National"],
                "Punjab": ["Punjab"],
                "Sindh": ["Sindh", "Karachi"],
                "KPK": ["KPK", "Khyber", "Pakhtunkhwa", "NWFP"],
                "Balochistan": ["Balochistan", "Baluchistan"],
                "AJK": ["Azad Jammu", "Kashmir", "AJK"],
                "Gilgit-Baltistan": ["Gilgit", "Baltistan", "GBLR"],
            }
            for jurisdiction, keywords in jurisdiction_keywords.items():
                if any(kw.lower() in statute_name.lower() for kw in keywords):
                    statute.jurisdiction = jurisdiction
                    break
            
            # Process sections
            all_case_links = []
            full_text_parts = []
            all_raw_html = []
            processed_sections = []
            failed_sections = []
            
            for i, section_info in enumerate(sections):
                if i > 0 and i % 5 == 0:
                    self._human_delay(4, 8)
                else:
                    self._human_delay(1.5, 3)
                
                section_id = section_info.get("section_id", "")
                case_type_id = section_info.get("case_type_id", "")
                section_num = section_info.get("number", "")
                
                # Get section content
                raw_html, clean_text, status = "", "", "skipped"
                if section_id:
                    raw_html, clean_text, status = self.get_section_content(section_id)
                    if raw_html:
                        all_raw_html.append(raw_html)
                        if clean_text:
                            self._human_delay(reading=True)
                    
                    if status != "ok":
                        failed_sections.append({
                            "section_id": section_id,
                            "number": section_num,
                            "reason": status
                        })
                        self.progress.queue_failed_section(statute_name, {
                            "section_id": section_id,
                            "number": section_num,
                        })
                
                # Get case links
                section_ref = f"Section {section_num}" if section_num else ""
                case_citations = []
                if case_type_id:
                    self._human_delay(1, 2)
                    case_citations = self.get_section_case_links(case_type_id)
                
                enriched_cases = enrich_case_links(case_citations, section_ref)
                all_case_links.extend(enriched_cases)
                
                section = Section(
                    number=section_num,
                    title=section_info.get("definition", ""),
                    text=clean_text,
                    text_raw=raw_html,
                    cases_cited=enriched_cases,
                    section_id=section_id,
                    fetch_status=status,
                )
                processed_sections.append(section)
                
                if clean_text:
                    full_text_parts.append(f"[Section {section.number}]\n{clean_text}")
            
            # Finalize statute
            statute.sections = processed_sections
            statute.full_text = "\n\n".join(full_text_parts)
            statute.full_text_raw = "\n\n<!-- SECTION BREAK -->\n\n".join(all_raw_html)
            statute.preamble = extract_preamble([asdict(s) for s in processed_sections]) or ""
            
            for s in processed_sections:
                if s.number.upper() == 'PREAMBLE' or 'preamble' in s.number.lower():
                    statute.preamble_raw = s.text_raw
                    break
            
            # Dedupe case citations
            seen_citations = set()
            unique_cases = []
            for case in all_case_links:
                if isinstance(case, dict):
                    citation = case.get("citation", "")
                    if citation and citation not in seen_citations:
                        seen_citations.add(citation)
                        unique_cases.append(case)
            statute.cases_cited = unique_cases
            
            # Set scrape status
            if failed_sections:
                statute.scrape_status = "partial"
                statute.failed_sections = failed_sections
                self.progress.mark_incomplete(statute_name, failed_sections)
                logger.warning(f"   ⚠️ Partial: {len(failed_sections)} sections failed")
            else:
                statute.scrape_status = "complete"
            
            # Update progress
            self.progress.mark_scraped(statute_name, len(processed_sections), len(failed_sections))
            self.reporter.log_statute(statute_name, True, len(processed_sections), len(failed_sections))
            
            return statute, statute.full_text_raw
            
        except Exception as e:
            logger.error(f"❌ Exception scraping {statute_name}: {e}")
            logger.debug(traceback.format_exc())
            self.progress.mark_failed(statute_name, str(e))
            self.reporter.log_statute(statute_name, False)
            return None
    
    # ── Save Statute ──────────────────────────────────────────────────────────
    
    def save_statute(self, statute: Statute, raw_html: str = ""):
        """Save statute in all required formats."""
        try:
            slug = generate_statute_slug(statute.title)
            letter = statute.alphabet
            
            statute_dir = LEGISLATION_DIR / letter
            html_output_dir = HTML_DIR / letter
            
            # 1. Individual JSON
            json_path = statute_dir / f"{slug}.json"
            json_path.write_text(
                json.dumps(statute.to_dict(), indent=2, ensure_ascii=False),
                encoding='utf-8'
            )
            
            # 2. Raw HTML
            if raw_html:
                html_path = statute_dir / "original" / f"{slug}.html"
                html_path.write_text(raw_html, encoding='utf-8')
            
            # 3. Clean HTML
            clean_html = generate_statute_page_html(statute.to_dict())
            clean_html_path = html_output_dir / f"{slug}.html"
            clean_html_path.write_text(clean_html, encoding='utf-8')
            
            # 4. Letter JSONL
            letter_jsonl = LEGISLATION_DIR / f"{letter}.jsonl"
            with open(letter_jsonl, 'a', encoding='utf-8') as f:
                f.write(json.dumps(statute.to_dict(), ensure_ascii=False) + '\n')
            
            # 5. Master JSONL
            with open(STATUTES_JSONL, 'a', encoding='utf-8') as f:
                f.write(json.dumps(statute.to_dict(), ensure_ascii=False) + '\n')
            
            # 6. Case links
            if statute.cases_cited:
                with open(LINKS_FILE, 'a', encoding='utf-8') as f:
                    for citation in statute.cases_cited:
                        entry = {
                            "statute_id": statute.statute_id,
                            "statute_title": statute.title,
                            "case_citation": citation
                        }
                        f.write(json.dumps(entry, ensure_ascii=False) + '\n')
            
            logger.info(f"   💾 Saved ({len(statute.sections)} sections)")
            
        except Exception as e:
            logger.error(f"❌ Failed to save {statute.title}: {e}")
            logger.debug(traceback.format_exc())
    
    # ── Alphabet Scraping ─────────────────────────────────────────────────────
    
    def scrape_letter(self, letter: str, limit: int = None) -> int:
        """Scrape all statutes for a letter."""
        if self.progress.is_letter_done(letter):
            logger.info(f"⏭️  Skipping '{letter}' (already completed)")
            return 0
        
        logger.info(f"{'='*60}")
        logger.info(f"📖 STARTING LETTER '{letter}'")
        logger.info(f"{'='*60}")
        
        self.progress.set_current_letter(letter)
        
        statutes = self.get_statutes_by_letter(letter)
        if not statutes:
            logger.warning(f"No statutes found for '{letter}'")
            self.progress.complete_letter(letter)
            return 0
        
        if limit:
            statutes = statutes[:limit]
        
        scraped_count = 0
        total = len(statutes)
        
        for i, statute_info in enumerate(statutes):
            try:
                # Check operating hours
                if not self._is_within_operating_hours():
                    self._wait_for_operating_hours()
                    self.logged_in = False
                
                # Check stop time
                if self._should_stop_for_time():
                    logger.info(f"⏰ Reached target time. Saving and stopping.")
                    break
                
                self._human_delay()
                result = self.scrape_statute(statute_info)
                
                if result:
                    statute, raw_html = result
                    self.save_statute(statute, raw_html)
                    scraped_count += 1
                
                # Print summary every 10 statutes
                self.reporter.print_summary(10)
                
            except KeyboardInterrupt:
                logger.info("⛔ Interrupted. Saving progress...")
                self.progress.save()
                raise
            except Exception as e:
                logger.error(f"❌ Unhandled error: {e}")
                logger.debug(traceback.format_exc())
                self.logged_in = False
                time.sleep(RATE_LIMIT_BACKOFF)
        
        self.progress.complete_letter(letter)
        logger.info(f"✅ Completed '{letter}': {scraped_count} statutes")
        
        return scraped_count
    
    # ── Multi-Letter Run ──────────────────────────────────────────────────────
    
    def scrape_range(self, start: str = "A", end: str = "Z", limit_per_letter: int = None):
        """Scrape a range of letters."""
        logger.info(f"🚀 Starting scrape: {start} → {end}")
        
        self.progress.start_session()
        
        if not self.login():
            logger.error("❌ Failed to login. Aborting.")
            return
        
        start_idx = ALPHABETS.index(start) if start in ALPHABETS else 0
        end_idx = ALPHABETS.index(end) if end in ALPHABETS else 25
        
        total_scraped = 0
        
        for letter in ALPHABETS[start_idx:end_idx + 1]:
            try:
                if not self._is_within_operating_hours():
                    self._wait_for_operating_hours()
                    self.logged_in = False
                    if not self.login():
                        break
                
                if self._should_stop_for_time():
                    logger.info(f"⏰ Reached target time. Stopping.")
                    break
                
                count = self.scrape_letter(letter, limit=limit_per_letter)
                total_scraped += count
                
                # Break between letters
                if letter != ALPHABETS[end_idx]:
                    break_time = random.uniform(60, 120)
                    logger.info(f"☕ Break between letters: {break_time:.0f}s")
                    time.sleep(break_time)
                
            except KeyboardInterrupt:
                logger.info("⛔ Interrupted by user.")
                break
        
        # Final report
        logger.info(f"\n{'='*60}")
        logger.info(f"🏁 SCRAPE COMPLETE")
        logger.info(f"{'='*60}")
        
        report = self.reporter.save_report(self.progress)
        
        logger.info(f"📊 Final Stats:")
        logger.info(f"   ✅ Scraped: {report['summary']['scraped']}")
        logger.info(f"   ❌ Failed:  {report['summary']['failed']}")
        logger.info(f"   ⏭️  Skipped: {report['summary']['skipped']}")
        logger.info(f"   📄 Sections: {report['summary']['total_sections']}")
        logger.info(f"   ⏱️  Duration: {report['session_duration_minutes']:.1f} minutes")
    
    def resume(self, limit_per_letter: int = None):
        """Resume from last position."""
        letter, _ = self.progress.get_resume_point()
        
        if not letter:
            logger.info("✅ All letters already completed!")
            return
        
        logger.info(f"🔄 Resuming from letter '{letter}'")
        self.scrape_range(start=letter, limit_per_letter=limit_per_letter)
    
    # ── Retry Failed Sections ─────────────────────────────────────────────────
    
    def retry_failed_sections(self) -> int:
        """Retry all queued failed sections."""
        queue = self.progress.progress.get("failed_section_queue", [])
        
        if not queue:
            logger.info("No failed sections to retry")
            return 0
        
        logger.info(f"🔄 Retrying {len(queue)} failed sections...")
        
        if not self.login():
            return 0
        
        success_count = 0
        still_failed = []
        
        for item in queue:
            section_info = item["section"]
            section_id = section_info.get("section_id")
            
            if not section_id:
                continue
            
            self._human_delay(2, 4)
            raw_html, clean_text, status = self.get_section_content(section_id)
            
            if status == "ok":
                success_count += 1
                logger.info(f"   ✅ Recovered section {section_id}")
                # TODO: Update the statute file with recovered content
            else:
                still_failed.append(item)
        
        # Update queue
        self.progress.progress["failed_section_queue"] = still_failed
        self.progress.save()
        
        logger.info(f"Recovered {success_count}/{len(queue)} sections. {len(still_failed)} still failed.")
        return success_count


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="PLS Legislation Scraper - Bulletproof Edition",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python bulletproof_scraper.py --start A --end Z          # Full run A→Z
  python bulletproof_scraper.py --letter A --limit 5       # Test with 5 statutes
  python bulletproof_scraper.py --resume                   # Continue from last position
  python bulletproof_scraper.py --ignore-hours             # Run during daytime (testing)
  python bulletproof_scraper.py --until-time "05:00"       # Stop at 5 AM PKT
  python bulletproof_scraper.py --retry-failed             # Retry failed sections
        """
    )
    
    parser.add_argument("--start", "-s", default="A", help="Start letter (default: A)")
    parser.add_argument("--end", "-e", default="Z", help="End letter (default: Z)")
    parser.add_argument("--letter", "-l", help="Single letter only")
    parser.add_argument("--limit", "-n", type=int, help="Limit statutes per letter")
    parser.add_argument("--resume", action="store_true", help="Resume from last position")
    parser.add_argument("--respect-hours", action="store_true", help="Respect PLS hours (default: 24/7)")
    parser.add_argument("--until-time", help="Stop at PKT time (HH:MM)")
    parser.add_argument("--retry-failed", action="store_true", help="Retry failed sections")
    parser.add_argument("--status", action="store_true", help="Show progress status")
    parser.add_argument("--log-file", help="Log to file")
    
    args = parser.parse_args()
    
    # Setup logging with file if requested
    if args.log_file:
        global logger
        logger = setup_logging(Path(args.log_file))
    
    if args.status:
        progress = ProgressManager()
        print(f"\n📊 Progress Status")
        print(f"{'='*40}")
        print(f"Scraped:    {len(progress.progress['scraped'])}")
        print(f"Failed:     {len(progress.progress['failed'])}")
        print(f"Incomplete: {len(progress.progress['incomplete'])}")
        print(f"Completed:  {progress.progress['completed_letters']}")
        print(f"Current:    {progress.progress['current_letter']}")
        print(f"Queue:      {len(progress.progress.get('failed_section_queue', []))} sections")
        stats = progress.get_stats()
        print(f"\nStats:")
        print(f"  Total scraped:  {stats['total_scraped']}")
        print(f"  Total sections: {stats['total_sections']}")
        print(f"  Failed sections: {stats['failed_sections']}")
        return
    
    scraper = BulletproofScraper(
        ignore_hours=not args.respect_hours,  # 24/7 by default
        until_time=args.until_time
    )
    
    if args.retry_failed:
        scraper.retry_failed_sections()
    elif args.resume:
        scraper.resume(limit_per_letter=args.limit)
    elif args.letter:
        if not scraper.login():
            return
        scraper.scrape_letter(args.letter, limit=args.limit)
        scraper.reporter.save_report(scraper.progress)
    else:
        scraper.scrape_range(
            start=args.start,
            end=args.end,
            limit_per_letter=args.limit
        )


if __name__ == "__main__":
    main()
