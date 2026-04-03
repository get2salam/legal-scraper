#!/usr/bin/env python3
"""
Scraper Chain — Automated multi-phase scraping pipeline
=========================================================
Monitors running scraper, then chains: verify → gap fill → next batch.

Phase 1: Wait for current run (2013→2010) to finish
Phase 2: Verify completed years (2014, 2013, 2012, 2011, 2010)
Phase 3: Fill 2015 gaps (YLR, CLD, GBLR only)
Phase 4: Scrape 2009→2006 (critical gaps)
Phase 5: Scrape 2005→2000 (deep historical)
Phase 6: Scrape 1999→1990
Phase 7: Scrape 1989→1947
"""

import os
import sys
import json
import time
import subprocess
import logging
from pathlib import Path
from datetime import datetime

# Setup
SCRAPER_DIR = Path(__file__).parent
DATA_DIR = SCRAPER_DIR / "data_v2"
LOG_DIR = SCRAPER_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(LOG_DIR / "scraper_chain.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("scraper_chain")


def count_cases(year: int) -> int:
    """Count JSON files for a given year across all reporters."""
    total = 0
    for r in REPORTERS:
        d = DATA_DIR / r / str(year)
        if d.exists():
            total += len(list(d.glob("*.json")))
    return total


def count_all() -> dict:
    """Count cases for all years."""
    counts = {}
    for year in range(1947, 2027):
        c = count_cases(year)
        if c > 0:
            counts[year] = c
    return counts


def is_scraper_running() -> bool:
    """Check if historical_scraper.py is running."""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'historical_scraper' -and $_.Name -match 'python' } | Select-Object ProcessId"],
            capture_output=True, text=True, timeout=10
        )
        return "ProcessId" in result.stdout and len(result.stdout.strip().split("\n")) > 2
    except:
        return False


def wait_for_scraper():
    """Wait for running scraper to finish."""
    logger.info("Waiting for current scraper to finish...")
    check_interval = 60  # Check every minute
    while is_scraper_running():
        time.sleep(check_interval)
    logger.info("Scraper finished!")
    time.sleep(10)  # Brief pause


def run_scraper(from_year: int, to_year: int, extra_args: list = None):
    """Run historical_scraper.py and wait for completion."""
    args = ["python", "historical_scraper.py", "--from-year", str(from_year), "--to-year", str(to_year)]
    if extra_args:
        args.extend(extra_args)

    logger.info(f"Starting scraper: {' '.join(args)}")
    start = time.time()

    # Count before
    before = sum(count_cases(y) for y in range(to_year, from_year + 1))

    proc = subprocess.Popen(
        args,
        cwd=str(SCRAPER_DIR),
        stdout=subprocess.DEVNULL,
        stderr=open(LOG_DIR / "historical_stderr.log", "w", encoding="utf-8"),
    )

    # Wait for completion
    proc.wait()
    elapsed = time.time() - start

    # Count after
    after = sum(count_cases(y) for y in range(to_year, from_year + 1))
    new_cases = after - before

    logger.info(f"Scraper finished: {from_year}→{to_year} | +{new_cases} new cases | {elapsed/60:.0f}min")
    return new_cases


def run_scraper_single(year: int, reporter: str = None):
    """Run scraper for a single year, optionally a single reporter."""
    args = ["python", "historical_scraper.py", "--year", str(year)]
    if reporter:
        args.extend(["--reporter", reporter])

    logger.info(f"Starting scraper: {' '.join(args)}")
    start = time.time()
    before = count_cases(year)

    proc = subprocess.Popen(
        args,
        cwd=str(SCRAPER_DIR),
        stdout=subprocess.DEVNULL,
        stderr=open(LOG_DIR / "historical_stderr.log", "w", encoding="utf-8"),
    )
    proc.wait()
    elapsed = time.time() - start

    after = count_cases(year)
    new_cases = after - before
    logger.info(f"Scraper finished: {year} {reporter or 'ALL'} | +{new_cases} new | {elapsed/60:.0f}min")
    return new_cases


def run_verify(year: int):
    """Run verify_scraper.py for a year."""
    logger.info(f"Verifying year {year}...")
    try:
        result = subprocess.run(
            ["python", "verify_scraper.py", "--year", str(year)],
            cwd=str(SCRAPER_DIR),
            capture_output=True, text=True, timeout=300,
            encoding="utf-8", errors="replace"
        )
        logger.info(f"Verification {year}: {result.stdout[-200:] if result.stdout else 'no output'}")
    except Exception as e:
        logger.warning(f"Verification {year} failed: {e}")
    time.sleep(5)  # Brief pause between verifications


def phase_1_wait():
    """Phase 1: Wait for current 2013→2010 run."""
    logger.info("=" * 60)
    logger.info("PHASE 1: Waiting for current scraper (2013→2010)")
    logger.info("=" * 60)
    wait_for_scraper()

    # Report current state
    for year in [2013, 2012, 2011, 2010]:
        c = count_cases(year)
        logger.info(f"  {year}: {c} cases")


def phase_2_verify():
    """Phase 2: Verify recently completed years."""
    logger.info("=" * 60)
    logger.info("PHASE 2: Verifying completed years")
    logger.info("=" * 60)

    for year in [2014, 2013, 2012, 2011, 2010]:
        run_verify(year)

    logger.info("Verification complete. Cooling down 60s before next phase...")
    time.sleep(60)


def phase_3_fill_2015():
    """Phase 3: Fill 2015 gaps (YLR, CLD, GBLR)."""
    logger.info("=" * 60)
    logger.info("PHASE 3: Filling 2015 gaps (YLR, CLD, GBLR)")
    logger.info("=" * 60)

    total_new = 0
    for reporter in ["YLR", "CLD", "GBLR"]:
        new = run_scraper_single(2015, reporter)
        total_new += new
        # Cool down between reporters
        time.sleep(30)

    logger.info(f"Phase 3 complete: +{total_new} new cases for 2015")
    logger.info(f"2015 total: {count_cases(2015)}")
    time.sleep(60)


def phase_4_critical():
    """Phase 4: Scrape 2009→2006 (critical gaps)."""
    logger.info("=" * 60)
    logger.info("PHASE 4: Scraping 2009→2006 (critical gaps)")
    logger.info("=" * 60)

    new = run_scraper(2009, 2006)
    logger.info(f"Phase 4 complete: +{new} new cases (2009-2006)")
    time.sleep(120)  # Longer cooldown before deep historical


def phase_5_2000s():
    """Phase 5: Scrape 2005→2000."""
    logger.info("=" * 60)
    logger.info("PHASE 5: Scraping 2005→2000 (deep historical)")
    logger.info("=" * 60)

    new = run_scraper(2005, 2000)
    logger.info(f"Phase 5 complete: +{new} new cases (2005-2000)")
    time.sleep(120)


def phase_6_90s():
    """Phase 6: Scrape 1999→1990."""
    logger.info("=" * 60)
    logger.info("PHASE 6: Scraping 1999→1990")
    logger.info("=" * 60)

    new = run_scraper(1999, 1990)
    logger.info(f"Phase 6 complete: +{new} new cases (1999-1990)")
    time.sleep(120)


def phase_7_deep():
    """Phase 7: Scrape 1989→1947."""
    logger.info("=" * 60)
    logger.info("PHASE 7: Scraping 1989→1947 (independence era)")
    logger.info("=" * 60)

    # Break into chunks to avoid super long runs
    for start, end in [(1989, 1980), (1979, 1970), (1969, 1960), (1959, 1947)]:
        new = run_scraper(start, end)
        logger.info(f"  {start}→{end}: +{new} new cases")
        time.sleep(120)  # Cooldown between decades


def main():
    logger.info("=" * 60)
    logger.info("SCRAPER CHAIN — Full Pipeline Started")
    logger.info(f"Time: {datetime.now().isoformat()}")
    logger.info("=" * 60)

    grand_start = time.time()
    initial_total = sum(count_all().values())
    logger.info(f"Starting total: {initial_total} cases")

    # Execute phases
    phase_1_wait()
    phase_2_verify()
    phase_3_fill_2015()
    phase_4_critical()
    phase_5_2000s()
    phase_6_90s()
    phase_7_deep()

    # Final report
    final_total = sum(count_all().values())
    elapsed = time.time() - grand_start
    logger.info("")
    logger.info("=" * 60)
    logger.info("SCRAPER CHAIN — ALL PHASES COMPLETE")
    logger.info(f"Started: {initial_total} cases")
    logger.info(f"Final:   {final_total} cases")
    logger.info(f"Added:   +{final_total - initial_total} cases")
    logger.info(f"Runtime: {elapsed/3600:.1f} hours")
    logger.info("=" * 60)

    # Save summary
    summary = {
        "started_at": datetime.now().isoformat(),
        "initial_total": initial_total,
        "final_total": final_total,
        "cases_added": final_total - initial_total,
        "runtime_hours": round(elapsed / 3600, 1),
        "counts": count_all(),
    }
    summary_path = DATA_DIR / "audit" / "scraper_chain_summary.json"
    summary_path.parent.mkdir(exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info(f"Summary saved to {summary_path}")


if __name__ == "__main__":
    main()
