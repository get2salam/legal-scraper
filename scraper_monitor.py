#!/usr/bin/env python3
"""
Scraper Monitor - Keeps scrapers running non-stop
Checks every 5 minutes via Task Scheduler
Includes PRE and POST verification
"""

import os
import sys
import json
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data_v2"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "monitor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Target years to scrape
TARGET_YEARS = [2022, 2021, 2020, 2019, 2018]
EXPECTED_CASES_PER_YEAR = 1800  # Approximate
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD']


def is_scraper_running():
    """Check if scraper process is running"""
    try:
        result = subprocess.run(
            ['powershell', '-Command', 
             'Get-Process python -ErrorAction SilentlyContinue | Where-Object {$_.StartTime -gt (Get-Date).AddHours(-2)} | Measure-Object | Select-Object -ExpandProperty Count'],
            capture_output=True, text=True, timeout=10
        )
        count = int(result.stdout.strip() or 0)
        return count > 0
    except:
        return False


def get_year_counts():
    """Get case counts by year"""
    data_dir = BASE_DIR / "data_v2"
    counts = {}
    for year in TARGET_YEARS:
        total = 0
        for reporter in ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD']:
            year_dir = data_dir / reporter / str(year)
            if year_dir.exists():
                total += len(list(year_dir.glob("*.json")))
        counts[year] = total
    return counts


def find_incomplete_year(counts):
    """Find first incomplete year"""
    for year in TARGET_YEARS:
        if counts.get(year, 0) < EXPECTED_CASES_PER_YEAR:
            return year
    return None


def run_pre_verify(year):
    """Run pre-scrape verification"""
    logger.info(f"Running PRE-VERIFICATION for {year}...")
    result = subprocess.run(
        ['python', 'verify_year.py', 'pre', str(year)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=120
    )
    logger.info(f"Pre-verify output:\n{result.stdout}")
    return result.returncode == 0


def run_post_verify(year):
    """Run post-scrape verification"""
    logger.info(f"Running POST-VERIFICATION for {year}...")
    result = subprocess.run(
        ['python', 'verify_year.py', 'post', str(year)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        timeout=120
    )
    logger.info(f"Post-verify output:\n{result.stdout}")
    return "VERIFICATION PASSED" in result.stdout


def check_year_complete(year):
    """Check if a year appears complete based on file counts"""
    import glob
    total = 0
    for r in REPORTERS:
        total += len(glob.glob(str(DATA_DIR / r / str(year) / "*.json")))
    return total >= EXPECTED_CASES_PER_YEAR


def start_scraper(years):
    """Start scraper for multiple years with pre-verification"""
    years_str = ", ".join(str(y) for y in years)
    logger.info(f"Starting scraper for years: {years_str}")
    
    # Run pre-verification for first year
    run_pre_verify(years[0])
    
    years_code = "\n        ".join([f"scraper.scrape_reporter_year(r, {y})" for y in years])
    
    script = f'''
import sys
sys.path.insert(0, r"{BASE_DIR}")
from pls_scraper_v2 import PLSScraperV2
import subprocess
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s", datefmt="%H:%M:%S", 
                    handlers=[logging.FileHandler(r"{LOG_DIR}/scraper_{"_".join(str(y) for y in years)}.log"), logging.StreamHandler()])
scraper = PLSScraperV2()
if scraper.login():
    reporters = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD"]
    for r in reporters:
        {years_code}
    print("SCRAPER COMPLETE")
    # Run post-verification
    for year in {years}:
        subprocess.run(["python", "verify_year.py", "post", str(year)], cwd=r"{BASE_DIR}")
'''
    
    script_file = LOG_DIR / "current_scrape.py"
    script_file.write_text(script)
    
    log_file = LOG_DIR / f"scraper_{'_'.join(str(y) for y in years)}.log"
    
    subprocess.Popen(
        ['python', str(script_file)],
        stdout=open(log_file, 'a'),
        stderr=subprocess.STDOUT,
        cwd=str(BASE_DIR)
    )
    logger.info(f"Scraper started! Logging to {log_file.name}")


def main():
    logger.info("=" * 50)
    logger.info("SCRAPER MONITOR - NON-STOP MODE")
    logger.info("=" * 50)
    
    # Check if running
    if is_scraper_running():
        logger.info("✓ Scraper is running")
        counts = get_year_counts()
        for year, count in counts.items():
            logger.info(f"  {year}: {count} cases")
        return
    
    logger.info("✗ No scraper running - starting one...")
    
    # Get current counts
    counts = get_year_counts()
    for year, count in counts.items():
        logger.info(f"  {year}: {count} cases")
    
    # Find incomplete years
    incomplete = [y for y in TARGET_YEARS if counts.get(y, 0) < EXPECTED_CASES_PER_YEAR]
    
    if incomplete:
        # Start with first 2 incomplete years
        to_scrape = incomplete[:2]
        start_scraper(to_scrape)
    else:
        logger.info("All target years complete!")


if __name__ == "__main__":
    main()
