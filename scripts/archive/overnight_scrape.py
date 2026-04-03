#!/usr/bin/env python3
"""
Daily scraping run — enumerate all major books for recent years,
then fetch as many full cases as possible.

IMPORTANT: Only runs between 7:00 AM - 9:00 PM Pakistan Standard Time (PKT = UTC+5)
to mimic normal user browsing behaviour and stay under the radar.
"""

import sys
import time
import logging
from datetime import datetime, timezone, timedelta
from pls_scraper import PLSScraper, BOOKS

# Pakistan Standard Time = UTC+5
PKT = timezone(timedelta(hours=5))
SCRAPE_START_HOUR = 7   # 7:00 AM PKT
SCRAPE_END_HOUR = 21    # 9:00 PM PKT

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"data/pakistanlawsite/scrape_{datetime.now().strftime('%Y%m%d_%H%M')}.log"),
    ]
)
logger = logging.getLogger(__name__)

# Books in priority order
PRIORITY_BOOKS = ["PLD", "SCMR", "MLD", "PCrLJ", "CLC", "YLR", "PLC-Service", "PTD", "CLD", "GBLR"]

# Years to enumerate (newest first)
YEARS = [2025, 2024, 2023]

def is_within_pkt_hours():
    """Check if current time is between 7 AM and 9 PM PKT."""
    now_pkt = datetime.now(PKT)
    return SCRAPE_START_HOUR <= now_pkt.hour < SCRAPE_END_HOUR

def wait_for_pkt_window():
    """If outside scraping hours, wait until 7 AM PKT."""
    if is_within_pkt_hours():
        return True
    
    now_pkt = datetime.now(PKT)
    logger.info(f"Current PKT time: {now_pkt.strftime('%H:%M')} — outside scraping window (7AM-9PM PKT)")
    
    # Calculate wait time until 7 AM PKT
    if now_pkt.hour >= SCRAPE_END_HOUR:
        # After 9 PM — wait until 7 AM tomorrow
        next_start = now_pkt.replace(hour=SCRAPE_START_HOUR, minute=0, second=0) + timedelta(days=1)
    else:
        # Before 7 AM — wait until 7 AM today
        next_start = now_pkt.replace(hour=SCRAPE_START_HOUR, minute=0, second=0)
    
    wait_seconds = (next_start - now_pkt).total_seconds()
    wait_hours = wait_seconds / 3600
    logger.info(f"Waiting {wait_hours:.1f} hours until {next_start.strftime('%H:%M PKT')}...")
    time.sleep(wait_seconds)
    return True

def main():
    # Check PKT time window
    wait_for_pkt_window()

    scraper = PLSScraper()

    if not scraper.login():
        logger.error("Login failed!")
        sys.exit(1)

    start_time = time.time()
    total_enumerated = 0
    total_fetched = 0

    # ── Phase 1: Enumerate all books × years ──
    logger.info("=" * 60)
    logger.info("PHASE 1: ENUMERATION")
    logger.info("=" * 60)

    for year in YEARS:
        for book in PRIORITY_BOOKS:
            key = f"{book}_{year}"
            existing = scraper.progress.data["enumerated"].get(key, [])
            if existing:
                logger.info(f"  {key}: already enumerated ({len(existing)} cases)")
                total_enumerated += len(existing)
                continue

            if not scraper.progress.can_make_request():
                logger.warning("Daily limit reached during enumeration!")
                break

            if not is_within_pkt_hours():
                logger.info("Reached 9 PM PKT — stopping enumeration for today.")
                break

            try:
                cases = scraper.enumerate_book_year(book, year)
                total_enumerated += len(cases)
                logger.info(f"  {key}: {len(cases)} cases")
            except Exception as e:
                logger.error(f"  Error enumerating {key}: {e}")
                time.sleep(30)
                # Re-login in case session expired
                scraper.login()

        if not scraper.progress.can_make_request():
            break

    logger.info(f"\nTotal cases enumerated: {total_enumerated}")

    # ── Phase 2: Fetch full cases ──
    logger.info("=" * 60)
    logger.info("PHASE 2: FETCHING CASES")
    logger.info("=" * 60)

    unfetched = scraper.progress.get_unfetched_cases()
    remaining_budget = 500 - scraper.progress.get_today_requests()

    if not unfetched:
        logger.info("No pending cases to fetch!")
    elif not is_within_pkt_hours():
        logger.info("Reached 9 PM PKT — skipping case fetching for today.")
    else:
        # Each case costs 2 requests (headnotes + full)
        max_cases = min(len(unfetched), remaining_budget // 2)
        logger.info(f"{len(unfetched)} pending. Budget for {max_cases} cases ({remaining_budget} requests left).")

        try:
            scraper.fetch_pending_cases(limit=max_cases, headnotes_first=True)
        except Exception as e:
            logger.error(f"Error during fetch: {e}")
            # Try to re-login and continue
            scraper.login()
            time.sleep(30)
            remaining = 500 - scraper.progress.get_today_requests()
            if remaining > 2:
                scraper.fetch_pending_cases(limit=remaining // 2)

    # ── Summary ──
    elapsed = (time.time() - start_time) / 60
    logger.info("=" * 60)
    logger.info("OVERNIGHT SCRAPE COMPLETE")
    logger.info("=" * 60)

    scraper.show_status()

    logger.info(f"\nElapsed: {elapsed:.1f} minutes")
    logger.info(f"Total requests today: {scraper.progress.get_today_requests()}")

    # Save summary
    scraper.progress.log_daily("overnight_scrape", {
        "enumerated": total_enumerated,
        "elapsed_min": round(elapsed, 1),
        "total_requests": scraper.progress.get_today_requests(),
    })


if __name__ == "__main__":
    main()
