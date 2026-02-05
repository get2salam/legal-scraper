#!/usr/bin/env python3
"""
Stealth Scraper — Mimics natural lawyer browsing patterns.

Scrapes in 4 short segments throughout the day (Pakistan time):
  - Morning:   7:00 - 8:00 AM PKT
  - Midday:   12:00 - 1:00 PM PKT
  - Afternoon: 4:00 - 5:00 PM PKT
  - Evening:   8:00 - 9:00 PM PKT

Each segment fetches ~65 cases then sleeps until the next window.
Total: ~250 cases/day across 14 hours. Looks like a real user.

Run via Windows Task Scheduler at 2:00 AM GMT (7:00 AM PKT).
Script will wait for the first window, then manage its own schedule.
"""

import sys
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from pls_scraper import PLSScraper

# ─── Configuration ───────────────────────────────────────────────────────────

# Pakistan Standard Time = UTC+5
PKT = timezone(timedelta(hours=5))

# Scraping windows (PKT hours) — each is a (start_hour, end_hour) tuple
SCRAPE_WINDOWS = [
    (7, 8),     # Morning:   7:00 AM - 8:00 AM
    (12, 13),   # Midday:   12:00 PM - 1:00 PM
    (16, 17),   # Afternoon: 4:00 PM - 5:00 PM
    (20, 21),   # Evening:   8:00 PM - 9:00 PM
]

# Cases per segment (2 requests each: headnotes + full text)
CASES_PER_SEGMENT = 65
REQUESTS_PER_SEGMENT = CASES_PER_SEGMENT * 2  # ~130 requests per window

# Add random jitter to start times (0-15 minutes)
MAX_JITTER_MINUTES = 15

# ─── Logging ─────────────────────────────────────────────────────────────────

log_file = f"data/pakistanlawsite/stealth_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_file),
    ]
)
logger = logging.getLogger(__name__)

# ─── Helpers ─────────────────────────────────────────────────────────────────

def now_pkt():
    """Current time in Pakistan."""
    return datetime.now(PKT)

def is_in_window(window):
    """Check if current PKT time is within a scraping window."""
    start_h, end_h = window
    current = now_pkt()
    return start_h <= current.hour < end_h

def seconds_until_window(window):
    """Calculate seconds until a scraping window starts."""
    start_h, end_h = window
    current = now_pkt()
    target = current.replace(hour=start_h, minute=0, second=0, microsecond=0)
    
    if current >= target:
        # Window already started or passed today
        if current.hour < end_h:
            return 0  # We're in the window
        else:
            return None  # Window passed
    
    return (target - current).total_seconds()

def get_remaining_windows():
    """Get scraping windows that haven't passed yet today."""
    current = now_pkt()
    remaining = []
    for window in SCRAPE_WINDOWS:
        start_h, end_h = window
        if current.hour < end_h:  # Window hasn't ended yet
            remaining.append(window)
    return remaining

# ─── Main ────────────────────────────────────────────────────────────────────

def run_segment(scraper, segment_num, cases_limit):
    """Run one scraping segment."""
    pkt_time = now_pkt().strftime('%I:%M %p PKT')
    logger.info(f"")
    logger.info(f"{'='*60}")
    logger.info(f"SEGMENT {segment_num} — Starting at {pkt_time}")
    logger.info(f"{'='*60}")
    
    # First ensure all books are enumerated (skip if already done)
    PRIORITY_BOOKS = ["PLD", "SCMR", "MLD", "PCrLJ", "CLC", "YLR", "PLC-Service", "PTD", "CLD", "GBLR"]
    YEARS = [2025, 2024, 2023]
    
    for year in YEARS:
        for book in PRIORITY_BOOKS:
            key = f"{book}_{year}"
            existing = scraper.progress.data["enumerated"].get(key, [])
            if not existing and scraper.progress.can_make_request():
                try:
                    cases = scraper.enumerate_book_year(book, year)
                    if cases:
                        logger.info(f"  Enumerated {key}: {len(cases)} cases")
                except Exception as e:
                    logger.error(f"  Error enumerating {key}: {e}")
    
    # Fetch cases
    unfetched = scraper.progress.get_unfetched_cases()
    if not unfetched:
        logger.info("All cases fetched! Phase 1 complete! 🎉")
        return 0
    
    # Don't shuffle — fetch sequentially to ensure clean book-by-book completion
    # The unfetched list comes from (enumerated - fetched), order doesn't affect coverage
    # Sequential = easier to track progress + verify completeness per book/year
    
    fetch_count = min(cases_limit, len(unfetched))
    logger.info(f"{len(unfetched)} total pending. Fetching {fetch_count} this segment.")
    
    try:
        scraper.fetch_pending_cases(limit=fetch_count, headnotes_first=True)
    except Exception as e:
        logger.error(f"Error during fetch: {e}")
        # Try re-login and continue
        try:
            scraper.login()
            time.sleep(30)
            scraper.fetch_pending_cases(limit=max(1, fetch_count // 2), headnotes_first=True)
        except Exception as e2:
            logger.error(f"Failed after re-login: {e2}")
    
    remaining = len(scraper.progress.get_unfetched_cases())
    pkt_end = now_pkt().strftime('%I:%M %p PKT')
    logger.info(f"Segment {segment_num} complete at {pkt_end}. {remaining} cases remaining.")
    
    return fetch_count


def main():
    logger.info(f"")
    logger.info(f"{'#'*60}")
    logger.info(f"  STEALTH SCRAPER — {now_pkt().strftime('%Y-%m-%d')}")
    logger.info(f"  Pakistan Time: {now_pkt().strftime('%I:%M %p PKT')}")
    logger.info(f"{'#'*60}")
    
    # Get today's remaining windows
    windows = get_remaining_windows()
    
    if not windows:
        logger.info("No scraping windows remaining today. Exiting.")
        return
    
    logger.info(f"Windows remaining today: {len(windows)}")
    for w in windows:
        logger.info(f"  {w[0]:02d}:00 - {w[1]:02d}:00 PKT")
    
    # Login once at the start
    scraper = PLSScraper()
    if not scraper.login():
        logger.error("Login failed! Exiting.")
        sys.exit(1)
    
    total_fetched = 0
    segment_num = 0
    
    for window in windows:
        start_h, end_h = window
        
        # Wait for this window
        wait_secs = seconds_until_window(window)
        
        if wait_secs is None:
            continue  # Window already passed
        
        if wait_secs > 0:
            # Add random jitter (0-15 min) to avoid exact start times
            jitter = random.randint(0, MAX_JITTER_MINUTES * 60)
            total_wait = wait_secs + jitter
            wait_hours = total_wait / 3600
            
            jitter_min = jitter // 60
            logger.info(f"")
            logger.info(f"Waiting for {start_h:02d}:00 PKT window (+{jitter_min}min jitter)...")
            logger.info(f"Sleeping {wait_hours:.1f} hours. See you at ~{start_h:02d}:{jitter_min:02d} PKT.")
            
            time.sleep(total_wait)
        
        # Check if we're still within the window after waiting
        if not is_in_window(window):
            # Might have overshot with jitter — check next window
            continue
        
        # Re-login before each segment (sessions can expire during long waits)
        # But skip re-login for the first window if we just logged in
        if segment_num > 0:
            try:
                scraper.session.cookies.clear()  # Clear old cookies before re-login
                if not scraper.login():
                    logger.warning("Re-login failed, trying with fresh session...")
                    scraper.session = __import__('requests').Session()
                    if not scraper.login():
                        logger.error("Re-login failed completely! Trying next window...")
                        time.sleep(60)
                        continue
            except Exception as e:
                logger.error(f"Re-login error: {e}. Trying next window...")
                continue
        
        segment_num += 1
        fetched = run_segment(scraper, segment_num, CASES_PER_SEGMENT)
        total_fetched += fetched
        
        # Check if all done
        remaining = len(scraper.progress.get_unfetched_cases())
        if remaining == 0:
            logger.info("🎉 ALL CASES FETCHED! Phase 1 complete!")
            break
        
        # Check daily request budget
        today_reqs = scraper.progress.get_today_requests()
        if today_reqs >= 480:  # Leave 20 request buffer
            logger.info(f"Daily budget nearly exhausted ({today_reqs} requests). Stopping.")
            break
    
    # ─── Daily Summary ───
    logger.info(f"")
    logger.info(f"{'='*60}")
    logger.info(f"DAILY SUMMARY — {now_pkt().strftime('%Y-%m-%d')}")
    logger.info(f"{'='*60}")
    logger.info(f"Segments completed: {segment_num}")
    logger.info(f"Cases fetched today: {total_fetched}")
    logger.info(f"Total requests today: {scraper.progress.get_today_requests()}")
    logger.info(f"Cases remaining: {len(scraper.progress.get_unfetched_cases())}")
    
    scraper.progress.log_daily("stealth_scrape", {
        "segments": segment_num,
        "fetched": total_fetched,
        "total_requests": scraper.progress.get_today_requests(),
        "remaining": len(scraper.progress.get_unfetched_cases()),
    })
    
    logger.info("Done. See you tomorrow. 🪽")


if __name__ == "__main__":
    main()
