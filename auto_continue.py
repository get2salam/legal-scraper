#!/usr/bin/env python3
"""
Auto-Continue Script for PLS Scraper
=====================================
Monitors scraper progress and automatically starts next year when current completes.
Designed to be run via Task Scheduler or cron.
"""

import json
import subprocess
import sys
import os
from pathlib import Path
from datetime import datetime

# Configuration
SCRAPER_DIR = Path(__file__).parent
DATA_DIR = SCRAPER_DIR / "data_v2"
PROGRESS_FILE = DATA_DIR / "progress.json"
LOG_FILE = SCRAPER_DIR / "auto_continue.log"

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Years to scrape (in order of priority)
YEARS_TO_SCRAPE = [2025, 2024, 2023, 2022, 2021, 2020]


def log(msg: str):
    """Log message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    try:
        print(line)
    except UnicodeEncodeError:
        print(line.encode('ascii', 'replace').decode())
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def load_progress() -> dict:
    """Load progress from JSON file."""
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"completed_searches": [], "cases_fetched": [], "total_cases": 0}


def is_year_complete(progress: dict, year: int) -> bool:
    """Check if all reporters for a year are marked complete."""
    completed = progress.get("completed_searches", [])
    for reporter in REPORTERS:
        search_key = f"{year}-{reporter}"
        if search_key not in completed:
            return False
    return True


def get_next_year_to_scrape(progress: dict) -> int | None:
    """Find the next year that needs scraping."""
    for year in YEARS_TO_SCRAPE:
        if not is_year_complete(progress, year):
            return year
    return None


def is_scraper_running() -> bool:
    """Check if pls_scraper_v2.py is currently running."""
    try:
        # Windows: use tasklist
        result = subprocess.run(
            ["tasklist", "/FI", "IMAGENAME eq python.exe", "/FO", "CSV"],
            capture_output=True, text=True
        )
        # Check if our scraper is in the process list
        # This is a heuristic - checks for python processes
        if "python" in result.stdout.lower():
            # More specific check: look for our script in wmic
            wmic = subprocess.run(
                ["wmic", "process", "where", "name='python.exe'", "get", "commandline"],
                capture_output=True, text=True
            )
            if "pls_scraper_v2.py" in wmic.stdout:
                return True
        return False
    except Exception as e:
        log(f"Error checking if scraper running: {e}")
        return True  # Assume running if we can't check (safer)


def start_scraper(year: int):
    """Start the scraper for a specific year."""
    log(f"Starting scraper for year {year}...")
    
    # Change to scraper directory and start
    os.chdir(SCRAPER_DIR)
    
    # Start in background (detached)
    if sys.platform == "win32":
        # Windows: use START command
        subprocess.Popen(
            f'start "PLS Scraper {year}" python pls_scraper_v2.py scrape --year {year}',
            shell=True,
            cwd=SCRAPER_DIR
        )
    else:
        # Unix: use nohup
        subprocess.Popen(
            ["nohup", "python", "pls_scraper_v2.py", "scrape", "--year", str(year)],
            stdout=open(f"scraper_{year}.log", "w", encoding='utf-8'),
            stderr=subprocess.STDOUT,
            cwd=SCRAPER_DIR,
            start_new_session=True
        )
    
    log(f"Scraper started for year {year}")


def main():
    log("=" * 60)
    log("Auto-continue check started")
    
    # Load progress
    progress = load_progress()
    total_cases = progress.get("total_cases", 0)
    completed_searches = len(progress.get("completed_searches", []))
    
    log(f"Current stats: {total_cases} cases, {completed_searches} searches completed")
    
    # Check year completion status
    for year in YEARS_TO_SCRAPE:
        status = "COMPLETE" if is_year_complete(progress, year) else "incomplete"
        log(f"  {year}: {status}")
    
    # Check if scraper is running
    if is_scraper_running():
        log("Scraper is currently running. No action needed.")
        return
    
    log("Scraper is NOT running.")
    
    # Find next year to scrape
    next_year = get_next_year_to_scrape(progress)
    
    if next_year is None:
        log("All priority years complete! Nothing to do.")
        return
    
    log(f"Next year to scrape: {next_year}")
    start_scraper(next_year)
    log("Done!")


if __name__ == "__main__":
    main()
