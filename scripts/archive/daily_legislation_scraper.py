#!/usr/bin/env python3
"""
Daily Legislation Scraper
=========================
Scrapes one alphabet per day from PLS.
Designed for Windows Task Scheduler.

Schedule:
- Runs once per day
- Scrapes next incomplete alphabet
- Tracks progress in daily_schedule.json
- Respects PLS operating hours (7 AM - 9 PM PKT)
"""

import os
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, time as dt_time
import pytz

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from legislation_scraper import LegislationScraper, ALPHABETS, DATA_DIR

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

SCHEDULE_FILE = Path(__file__).parent / "daily_legislation_schedule.json"
LOG_FILE = Path(__file__).parent / "daily_legislation.log"

# PLS Operating Hours (Pakistan Time)
PKT = pytz.timezone('Asia/Karachi')
OPERATING_START = dt_time(7, 0)   # 7:00 AM PKT
OPERATING_END = dt_time(21, 0)    # 9:00 PM PKT

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Schedule Management
# ══════════════════════════════════════════════════════════════════════════════

def load_schedule() -> dict:
    """Load the daily schedule from file."""
    if SCHEDULE_FILE.exists():
        try:
            return json.loads(SCHEDULE_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {
        "started_at": None,
        "completed_alphabets": [],
        "current_alphabet": None,
        "last_run": None,
        "last_run_result": None,
        "runs": []
    }


def save_schedule(schedule: dict):
    """Save the daily schedule to file."""
    SCHEDULE_FILE.write_text(json.dumps(schedule, indent=2, ensure_ascii=False), encoding='utf-8')


def get_next_alphabet(schedule: dict) -> str:
    """Get the next alphabet to scrape."""
    completed = set(schedule.get("completed_alphabets", []))
    for letter in ALPHABETS:
        if letter not in completed:
            return letter
    return None  # All done!


def is_operating_hours() -> bool:
    """Check if PLS is within operating hours."""
    now_pkt = datetime.now(PKT).time()
    return OPERATING_START <= now_pkt <= OPERATING_END


# ══════════════════════════════════════════════════════════════════════════════
# Main Runner
# ══════════════════════════════════════════════════════════════════════════════

def run_daily_scrape():
    """Run the daily scrape for one alphabet."""
    logger.info("=" * 60)
    logger.info("Daily Legislation Scraper Starting")
    logger.info("=" * 60)
    
    # Check operating hours
    if not is_operating_hours():
        now_pkt = datetime.now(PKT)
        logger.warning(f"Outside PLS operating hours. Current PKT time: {now_pkt.strftime('%H:%M')}")
        logger.warning("PLS operates 7 AM - 9 PM PKT. Exiting.")
        return
    
    # Load schedule
    schedule = load_schedule()
    
    if not schedule["started_at"]:
        schedule["started_at"] = datetime.now().isoformat()
    
    # Get next alphabet to scrape
    letter = get_next_alphabet(schedule)
    
    if not letter:
        logger.info("All alphabets completed! Nothing to do.")
        return
    
    logger.info(f"Today's target: Alphabet '{letter}'")
    schedule["current_alphabet"] = letter
    schedule["last_run"] = datetime.now().isoformat()
    save_schedule(schedule)
    
    # Create scraper and run
    scraper = LegislationScraper()
    
    try:
        if not scraper.login():
            logger.error("Login failed!")
            schedule["last_run_result"] = "login_failed"
            save_schedule(schedule)
            return
        
        # Scrape the alphabet
        count = scraper.scrape_alphabet(letter)
        
        # Mark as completed
        if letter not in schedule["completed_alphabets"]:
            schedule["completed_alphabets"].append(letter)
        
        schedule["current_alphabet"] = None
        schedule["last_run_result"] = f"success_{count}_statutes"
        schedule["runs"].append({
            "date": datetime.now().isoformat(),
            "alphabet": letter,
            "statutes_scraped": count,
            "result": "success"
        })
        
        logger.info(f"Completed alphabet '{letter}': {count} statutes scraped")
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        schedule["last_run_result"] = "interrupted"
        schedule["runs"].append({
            "date": datetime.now().isoformat(),
            "alphabet": letter,
            "result": "interrupted"
        })
        
    except Exception as e:
        logger.error(f"Error during scrape: {e}")
        schedule["last_run_result"] = f"error_{str(e)[:50]}"
        schedule["runs"].append({
            "date": datetime.now().isoformat(),
            "alphabet": letter,
            "result": f"error: {str(e)[:100]}"
        })
    
    finally:
        save_schedule(schedule)
        scraper._save_progress()
    
    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Daily Scrape Summary")
    logger.info("=" * 60)
    logger.info(f"Completed alphabets: {schedule['completed_alphabets']}")
    remaining = [l for l in ALPHABETS if l not in schedule['completed_alphabets']]
    logger.info(f"Remaining alphabets: {remaining}")
    logger.info(f"Progress: {len(schedule['completed_alphabets'])}/26 alphabets")


def show_status():
    """Show current schedule status."""
    schedule = load_schedule()
    
    print("\n=== Daily Legislation Scraper Status ===\n")
    print(f"Started at: {schedule.get('started_at', 'Not started')}")
    print(f"Last run: {schedule.get('last_run', 'Never')}")
    print(f"Last result: {schedule.get('last_run_result', 'N/A')}")
    print(f"Current alphabet: {schedule.get('current_alphabet', 'None')}")
    print(f"Completed: {schedule.get('completed_alphabets', [])}")
    
    remaining = [l for l in ALPHABETS if l not in schedule.get('completed_alphabets', [])]
    print(f"Remaining: {remaining}")
    print(f"Progress: {26 - len(remaining)}/26 alphabets")
    
    print(f"\nNext alphabet: {get_next_alphabet(schedule) or 'All done!'}")
    print(f"Operating hours check: {'Yes' if is_operating_hours() else 'No'}")
    print(f"Current PKT time: {datetime.now(PKT).strftime('%Y-%m-%d %H:%M:%S')}")
    
    if schedule.get('runs'):
        print(f"\nRecent runs:")
        for run in schedule['runs'][-5:]:
            print(f"  {run.get('date', '?')}: {run.get('alphabet', '?')} -> {run.get('result', '?')}")


def reset_schedule():
    """Reset the schedule (start fresh)."""
    if SCHEDULE_FILE.exists():
        SCHEDULE_FILE.unlink()
    print("Schedule reset. Next run will start from 'A'.")


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Daily Legislation Scraper")
    parser.add_argument("command", nargs="?", default="run",
                        choices=["run", "status", "reset"],
                        help="Command to execute")
    parser.add_argument("--force", "-f", action="store_true",
                        help="Force run even outside operating hours")
    
    args = parser.parse_args()
    
    if args.command == "run":
        if args.force:
            # Temporarily override operating hours check
            def always_true():
                return True
            global is_operating_hours
            is_operating_hours = always_true
        run_daily_scrape()
    
    elif args.command == "status":
        show_status()
    
    elif args.command == "reset":
        reset_schedule()
