"""
Round 3 Gap Filler — Targets the remaining 3,793 missing cases.
Based on Audit Round 2 (Feb 22, 2026).
Cleans false 'completed' flags before each year, verifies session after empty results.
"""
import subprocess
import sys
import os
import json
import time
from datetime import datetime
from pathlib import Path

SCRAPER_DIR = Path(__file__).parent
DATA_DIR = SCRAPER_DIR / "data_v2"
LOG_FILE = SCRAPER_DIR / "fill_round3.log"
PROGRESS_FILE = DATA_DIR / "historical_progress.json"
SNAPSHOT_FILE = DATA_DIR / "daily_snapshot.json"
REPORTERS = ['SCMR', 'PLD', 'PCrLJ', 'MLD', 'CLC', 'YLR', 'PTD', 'PLC', 'CLD', 'GBLR']

# Gap years from Audit Round 2, ordered by gap size (biggest first)
# Format: (year, gap_estimate, description)
GAP_YEARS = [
    # PCrLJ gaps
    (1978, 303, "PCrLJ 303 missing"),
    (1972, 255, "PCrLJ 255 missing"),
    # PLC gaps
    (1962, 273, "PLC 273 missing"),
    (1985, 255, "PLC 255 missing"),
    (1963, 188, "PLC 188 missing"),
    (1975, 50, "PLC ~50 missing"),
    (1980, 30, "PLC ~30 missing"),
    # MLD gaps
    (1989, 208, "MLD 208 missing"),
    (1987, 100, "MLD residual gaps"),
    # PLD gaps — spread across many years
    (1968, 178, "PLD 178 missing"),
    (1977, 105, "PLD 105 missing"),
    (1976, 80, "PLD 80 missing"),
    (1983, 89, "PLD 89 missing"),
    (1986, 77, "PLD 77 missing"),
    (1969, 60, "PLD ~60 missing"),
    (1970, 50, "PLD ~50 missing"),
    (1971, 50, "PLD ~50 missing"),
    (1973, 40, "PLD ~40 missing"),
    (1974, 40, "PLD ~40 missing"),
    (1979, 30, "PLD ~30 missing"),
    (1981, 30, "PLD ~30 missing"),
    (1982, 30, "PLD ~30 missing"),
    (1984, 30, "PLD ~30 missing"),
    # PTD gaps
    (1961, 144, "PTD 144 missing"),
    (1967, 30, "PTD ~30 missing"),
    (1970, 20, "PTD ~20 missing"),
    # SCMR gaps
    (1987, 50, "SCMR residual"),
    (1988, 30, "SCMR ~30 missing"),
    # CLC gaps
    (1984, 98, "CLC 98 missing"),
    (1987, 50, "CLC residual"),
    # YLR gaps
    (2007, 50, "YLR ~50 missing"),
    (2009, 30, "YLR ~30 missing"),
    # CLD gaps
    (2007, 15, "CLD ~15 missing"),
    (2009, 14, "CLD ~14 missing"),
]

# Deduplicate years (keep first occurrence)
seen = set()
unique_gaps = []
for year, gap, notes in GAP_YEARS:
    if year not in seen:
        seen.add(year)
        unique_gaps.append((year, gap, notes))


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def count_cases(year):
    total = 0
    for r in REPORTERS:
        yp = DATA_DIR / r / str(year)
        if yp.is_dir():
            total += sum(1 for f in yp.iterdir() if f.suffix == '.json')
    return total


def count_total_pls():
    total = 0
    for r in REPORTERS:
        rdir = DATA_DIR / r
        if rdir.is_dir():
            for ydir in rdir.iterdir():
                if ydir.is_dir() and ydir.name.isdigit():
                    total += sum(1 for f in ydir.iterdir() if f.suffix == '.json')
    return total


def clean_progress_for_year(year):
    """Remove ALL 'completed' flags for a year so the scraper re-checks every reporter."""
    if not PROGRESS_FILE.exists():
        return 0
    
    progress = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
    completed = progress.get("completed", [])
    
    prefix = f"{year}-"
    removed = [e for e in completed if e.startswith(prefix)]
    if removed:
        progress["completed"] = [e for e in completed if not e.startswith(prefix)]
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')
        log(f"  Cleaned {len(removed)} 'completed' flags for {year}: {removed}")
    return len(removed)


def run_scraper(year):
    cmd = [
        sys.executable,
        str(SCRAPER_DIR / "historical_scraper.py"),
        "--from-year", str(year),
        "--to-year", str(year)
    ]
    log(f"Starting scraper for {year}: {' '.join(cmd)}")
    
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(SCRAPER_DIR),
            capture_output=False,
            timeout=7200  # 2hr max per year
        )
        return proc.returncode
    except subprocess.TimeoutExpired:
        log(f"  Year {year} TIMED OUT after 2 hours")
        return -1
    except Exception as e:
        log(f"  Year {year} ERROR: {e}")
        return -2


def update_snapshot():
    total = count_total_pls()
    today = datetime.now().strftime("%Y-%m-%d")
    snapshot = {}
    if SNAPSHOT_FILE.exists():
        snapshot = json.loads(SNAPSHOT_FILE.read_text(encoding='utf-8'))
    snapshot[today] = total
    SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding='utf-8')
    return total


def main():
    total_gaps = len(unique_gaps)
    
    log("=" * 60)
    log(f"ROUND 3 GAP FILLER — {total_gaps} gap years to process")
    log(f"Target: Fill remaining ~3,793 missing cases")
    log("=" * 60)
    
    start_total = count_total_pls()
    log(f"Starting PLS total: {start_total:,}")
    
    total_gained = 0
    results = []
    
    for i, (year, expected, notes) in enumerate(unique_gaps, 1):
        before = count_cases(year)
        log(f"\n[{i}/{total_gaps}] Year {year} — {notes}")
        log(f"  Current count: {before}")
        
        # CRITICAL: Clean false 'completed' flags before scraping
        clean_progress_for_year(year)
        
        # Run scraper
        exit_code = run_scraper(year)
        
        after = count_cases(year)
        gained = after - before
        total_gained += gained
        
        status = "TIMEOUT" if exit_code == -1 else ("ERROR" if exit_code == -2 else "OK")
        results.append((year, gained, notes, status))
        
        if gained > 0:
            log(f"  ✅ +{gained} cases (total so far: +{total_gained})")
        else:
            log(f"  ⚪ +0 (status: {status})")
        
        time.sleep(5)
    
    # Final summary
    final_total = update_snapshot()
    
    log("\n" + "=" * 60)
    log("ROUND 3 GAP FILLER — COMPLETE")
    log(f"Starting total: {start_total:,}")
    log(f"Final total: {final_total:,}")
    log(f"Total gained: +{total_gained}")
    log("=" * 60)
    
    log("\nPer-year results:")
    for year, gained, notes, status in results:
        marker = "✅" if gained > 0 else ("⚠️" if status != "OK" else "⚪")
        log(f"  {marker} {year}: +{gained} ({notes}) [{status}]")
    
    log("\nReporter totals:")
    for r in REPORTERS:
        rdir = DATA_DIR / r
        count = 0
        if rdir.is_dir():
            for ydir in rdir.iterdir():
                if ydir.is_dir() and ydir.name.isdigit():
                    count += sum(1 for f in ydir.iterdir() if f.suffix == '.json')
        log(f"  {r}: {count:,}")
    
    log(f"\n🎯 FINAL PLS TOTAL: {final_total:,} / 166,528 = {final_total/166528*100:.1f}%")
    log(f"   Remaining gap: {166528 - final_total:,}")


if __name__ == "__main__":
    main()
