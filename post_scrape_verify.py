"""
Post-scrape verification — waits for the scraper to finish, then verifies 2019-2021.
Run this alongside the scraper. It monitors PID and triggers verification when done.
"""
import subprocess
import time
import sys
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

SCRAPER_DIR = Path(__file__).parent
YEARS_TO_VERIFY = [2021, 2020, 2019]


def is_scraper_running():
    """Check if any pls_scraper_v2.py process is running."""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-WmiObject Win32_Process -Filter \"Name='python.exe'\" | "
             "Where-Object { $_.CommandLine -like '*pls_scraper_v2*' } | "
             "Select-Object ProcessId, CommandLine | Format-List"],
            capture_output=True, text=True, timeout=15
        )
        return 'pls_scraper_v2' in result.stdout
    except Exception:
        return False


def run_verifier(year):
    """Run verify_scraper.py for a specific year with --fix."""
    logger.info(f"{'═' * 60}")
    logger.info(f"  VERIFYING YEAR {year} (with --fix)")
    logger.info(f"{'═' * 60}")
    
    cmd = [
        sys.executable,
        str(SCRAPER_DIR / "verify_scraper.py"),
        "--year", str(year),
        "--fix"
    ]
    
    try:
        result = subprocess.run(
            cmd,
            cwd=str(SCRAPER_DIR),
            capture_output=True, text=True,
            timeout=1800  # 30 min per year
        )
        
        # Print output
        if result.stdout:
            for line in result.stdout.strip().split('\n'):
                logger.info(f"  [VERIFY] {line}")
        if result.stderr:
            for line in result.stderr.strip().split('\n')[-20:]:
                if 'INFO' in line or 'WARNING' in line or 'ERROR' in line:
                    logger.info(f"  [LOG] {line}")
        
        logger.info(f"  Verification for {year} finished (exit code: {result.returncode})")
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error(f"  Verifier timed out for {year}")
        return False
    except Exception as e:
        logger.error(f"  Verifier failed for {year}: {e}")
        return False


def main():
    logger.info("Post-scrape verifier started")
    logger.info(f"Will verify years: {YEARS_TO_VERIFY}")
    logger.info("Waiting for scraper to finish...")
    
    # Poll every 30 seconds until scraper is done
    check_count = 0
    while is_scraper_running():
        check_count += 1
        if check_count % 10 == 1:  # Log every 5 minutes
            logger.info(f"  Scraper still running... (checked {check_count} times)")
        time.sleep(30)
    
    logger.info(f"Scraper finished! Starting verification at {datetime.now().strftime('%H:%M:%S')}")
    
    # Run verifier for each year
    results = {}
    for year in YEARS_TO_VERIFY:
        success = run_verifier(year)
        results[year] = "✅ PASS" if success else "❌ ISSUES"
        time.sleep(5)  # Brief pause between years
    
    # Summary
    logger.info(f"{'═' * 60}")
    logger.info(f"  VERIFICATION SUMMARY")
    logger.info(f"{'═' * 60}")
    for year, status in results.items():
        logger.info(f"  {year}: {status}")
    logger.info(f"{'═' * 60}")
    logger.info("Post-scrape verification complete!")


if __name__ == "__main__":
    main()
