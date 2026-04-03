#!/usr/bin/env python3
"""
Comprehensive Gap Filler — waits for scraper_chain.py to finish, then:
1. Verify 2010 → fill gaps (YLR/CLD/GBLR critically low from PLS timeouts)
2. Verify 2014 → fill gaps (never verified)
3. Fill 2008→2006 (Phase 4 failed — laptop hibernated)
4. Fill 2005 gaps (81 missing: PTD 63, CLC 6, MLD 4, others)
5. Fill 2004 gaps (63 missing: PLC 30, YLR 29, others)
6. Fill 2011 GBLR (still 0)
7. Fill 2009 CLD/GBLR (low counts)
8. Final verification sweep
"""

import os
import sys
import time
import subprocess
import logging
import json
from pathlib import Path
from datetime import datetime

SCRAPER_DIR = Path(__file__).parent
LOG_DIR = SCRAPER_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(LOG_DIR / "fill_gaps.log", encoding="utf-8"),
    ]
)
logger = logging.getLogger("fill_gaps")

REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']


def count_cases(year):
    """Count JSON files for a given year across all reporters."""
    total = 0
    for r in REPORTERS:
        d = SCRAPER_DIR / "data_v2" / r / str(year)
        if d.is_dir():
            total += len([f for f in d.iterdir() if f.suffix == '.json'])
    return total


def count_reporter_year(reporter, year):
    """Count JSON files for a specific reporter/year."""
    d = SCRAPER_DIR / "data_v2" / reporter / str(year)
    if d.is_dir():
        return len([f for f in d.iterdir() if f.suffix == '.json'])
    return 0


def is_chain_running():
    """Check if scraper_chain.py is still running."""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'scraper_chain' -and $_.Name -match 'python' } | Select-Object ProcessId"],
            capture_output=True, text=True, timeout=15
        )
        lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip() and l.strip() != "ProcessId" and not l.strip().startswith("-")]
        return len(lines) > 0
    except:
        return False


def is_any_scraper_running():
    """Check if ANY python scraper is running (historical, pls_scraper, etc.)."""
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-CimInstance Win32_Process | Where-Object { ($_.CommandLine -match 'historical_scraper|pls_scraper|scraper_chain') -and $_.Name -match 'python' -and $_.ProcessId -ne " + str(os.getpid()) + " } | Select-Object ProcessId"],
            capture_output=True, text=True, timeout=15
        )
        lines = [l.strip() for l in result.stdout.strip().split("\n") if l.strip() and l.strip() != "ProcessId" and not l.strip().startswith("-")]
        return len(lines) > 0
    except:
        return False


def run_scraper(args_list, description=""):
    """Run historical_scraper.py with given args and wait for completion."""
    args = ["python", "historical_scraper.py"] + args_list
    logger.info(f"{'[' + description + '] ' if description else ''}Running: {' '.join(args)}")
    
    log_file = LOG_DIR / "historical_stderr.log"
    proc = subprocess.Popen(
        args, cwd=str(SCRAPER_DIR),
        stdout=subprocess.DEVNULL,
        stderr=open(log_file, "a", encoding="utf-8"),
    )
    proc.wait()
    exit_code = proc.returncode
    logger.info(f"{'[' + description + '] ' if description else ''}Finished (exit {exit_code}): {' '.join(args)}")
    time.sleep(30)  # Cool down between runs
    return exit_code


def run_verifier(year, fix=False):
    """Run verify_scraper.py for a given year. Returns (missing_count, details)."""
    args = ["python", "verify_scraper.py", "--year", str(year)]
    if fix:
        args.append("--fix")
    
    logger.info(f"Verifying year {year} (fix={fix})...")
    proc = subprocess.Popen(
        args, cwd=str(SCRAPER_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    stdout, stderr = proc.communicate(timeout=1800)
    
    # Log output
    for line in (stdout + stderr).strip().split("\n"):
        if line.strip():
            logger.info(f"  verify {year}: {line.strip()}")
    
    time.sleep(30)  # Cool down after verification
    return proc.returncode


def log_snapshot():
    """Log current case counts for all years."""
    logger.info("--- Current Case Count Snapshot ---")
    grand_total = 0
    for year in range(2025, 1999, -1):
        c = count_cases(year)
        if c > 0:
            grand_total += c
            flag = " ⚠️" if (year >= 2006 and c < 2000) else ""
            logger.info(f"  {year}: {c:,} cases{flag}")
    logger.info(f"  Grand total: {grand_total:,}")
    logger.info("---")
    return grand_total


def main():
    logger.info("=" * 70)
    logger.info("COMPREHENSIVE GAP FILLER — Started")
    logger.info("=" * 70)
    
    start_total = log_snapshot()
    
    # Wait for scraper_chain to finish
    logger.info("Waiting for scraper_chain.py to finish...")
    wait_count = 0
    while is_chain_running():
        wait_count += 1
        if wait_count % 15 == 0:  # Log every ~30 minutes
            logger.info(f"  Still waiting for scraper_chain... ({wait_count * 2} min)")
        time.sleep(120)
    
    logger.info("scraper_chain.py finished! Cooling down 60s...")
    time.sleep(60)
    
    # Also wait for any lingering scraper subprocesses
    while is_any_scraper_running():
        logger.info("  Waiting for child scrapers to finish...")
        time.sleep(60)
    
    logger.info("All scrapers done. Starting comprehensive gap fill.")
    log_snapshot()
    
    # ================================================================
    # STEP 1: Verify & fix 2010 (known critical gaps from PLS timeouts)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 1: Verify 2010 (YLR ~12/500+, CLD ~9/200+, GBLR 0)")
    before = count_cases(2010)
    
    # First verify to identify gaps
    run_verifier(2010, fix=True)
    
    # Also targeted re-scrape of known weak reporters
    for reporter in ["YLR", "CLD", "GBLR"]:
        before_r = count_reporter_year(reporter, 2010)
        run_scraper(["--year", "2010", "--reporter", reporter], f"2010 {reporter}")
        after_r = count_reporter_year(reporter, 2010)
        logger.info(f"  2010 {reporter}: {before_r} → {after_r} (+{after_r - before_r})")
    
    after = count_cases(2010)
    logger.info(f"STEP 1 DONE: 2010 {before} → {after} (+{after - before})")
    
    # ================================================================
    # STEP 2: Verify & fix 2014 (never verified)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 2: Verify 2014 (unverified)")
    before = count_cases(2014)
    
    run_verifier(2014, fix=True)
    
    after = count_cases(2014)
    logger.info(f"STEP 2 DONE: 2014 {before} → {after} (+{after - before})")
    
    # ================================================================
    # STEP 3: Fill 2008→2006 (Phase 4 failed — laptop hibernated)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 3: Scraping 2008→2006 (critical — only 73/81/51 linked cases)")
    before = {y: count_cases(y) for y in [2008, 2007, 2006]}
    
    run_scraper(["--from-year", "2008", "--to-year", "2006"], "2008→2006 full scrape")
    
    for y in [2008, 2007, 2006]:
        after_y = count_cases(y)
        logger.info(f"  {y}: {before[y]} → {after_y} (+{after_y - before[y]})")
    
    # ================================================================
    # STEP 4: Fill 2005 gaps (81 missing: PTD 63, CLC 6, MLD 4)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 4: Filling 2005 gaps (81 missing)")
    before = count_cases(2005)
    
    # Verify with --fix to auto-fill
    run_verifier(2005, fix=True)
    
    # Targeted re-scrape of weakest reporters
    for reporter in ["PTD", "CLC", "MLD"]:
        before_r = count_reporter_year(reporter, 2005)
        run_scraper(["--year", "2005", "--reporter", reporter], f"2005 {reporter}")
        after_r = count_reporter_year(reporter, 2005)
        logger.info(f"  2005 {reporter}: {before_r} → {after_r} (+{after_r - before_r})")
    
    after = count_cases(2005)
    logger.info(f"STEP 4 DONE: 2005 {before} → {after} (+{after - before})")
    
    # ================================================================
    # STEP 5: Fill 2004 gaps (63 missing: PLC 30, YLR 29)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 5: Filling 2004 gaps (63 missing)")
    before = count_cases(2004)
    
    run_verifier(2004, fix=True)
    
    for reporter in ["PLC", "YLR"]:
        before_r = count_reporter_year(reporter, 2004)
        run_scraper(["--year", "2004", "--reporter", reporter], f"2004 {reporter}")
        after_r = count_reporter_year(reporter, 2004)
        logger.info(f"  2004 {reporter}: {before_r} → {after_r} (+{after_r - before_r})")
    
    after = count_cases(2004)
    logger.info(f"STEP 5 DONE: 2004 {before} → {after} (+{after - before})")
    
    # ================================================================
    # STEP 6: Fill 2011 GBLR (still 0)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 6: Filling 2011 GBLR")
    before_r = count_reporter_year("GBLR", 2011)
    run_scraper(["--year", "2011", "--reporter", "GBLR"], "2011 GBLR")
    after_r = count_reporter_year("GBLR", 2011)
    logger.info(f"STEP 6 DONE: 2011 GBLR {before_r} → {after_r} (+{after_r - before_r})")
    
    # ================================================================
    # STEP 7: Fill 2009 CLD/GBLR (low counts)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 7: Filling 2009 CLD/GBLR")
    for reporter in ["CLD", "GBLR"]:
        before_r = count_reporter_year(reporter, 2009)
        run_scraper(["--year", "2009", "--reporter", reporter], f"2009 {reporter}")
        after_r = count_reporter_year(reporter, 2009)
        logger.info(f"  2009 {reporter}: {before_r} → {after_r} (+{after_r - before_r})")
    
    # ================================================================
    # STEP 8: Re-scrape 2000-2002 (only 9/6/9 linked cases — likely session death)
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 8: Re-scraping 2000-2002 (suspected session death, only 9/6/9)")
    before = {y: count_cases(y) for y in [2002, 2001, 2000]}
    
    run_scraper(["--from-year", "2002", "--to-year", "2000"], "2002→2000 re-scrape")
    
    for y in [2002, 2001, 2000]:
        after_y = count_cases(y)
        logger.info(f"  {y}: {before[y]} → {after_y} (+{after_y - before[y]})")
    
    # ================================================================
    # STEP 9: Final verification sweep
    # ================================================================
    logger.info("=" * 50)
    logger.info("STEP 9: Final verification sweep (2000-2014)")
    for year in [2000, 2001, 2002, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014]:
        run_verifier(year, fix=False)
    
    # ================================================================
    # FINAL REPORT
    # ================================================================
    logger.info("=" * 70)
    logger.info("COMPREHENSIVE GAP FILLER — COMPLETE")
    logger.info("=" * 70)
    end_total = log_snapshot()
    logger.info(f"Total gained: +{end_total - start_total:,} cases")
    logger.info(f"Started at: {start_total:,} → Ended at: {end_total:,}")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
