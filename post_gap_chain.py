"""
Post-Gap-Filler Chain — Runs after fill_audit_gaps_resume.py finishes.
1. Wait for gap filler to finish (polls every 60s)
2. Clean false "completed" flags for 1987
3. Re-scrape 1987 with NO timeout
4. Count final totals
5. Update daily_snapshot.json
"""
import subprocess
import sys
import os
import json
import time
import psutil
from datetime import datetime
from pathlib import Path

SCRAPER_DIR = Path(__file__).parent
DATA_DIR = SCRAPER_DIR / "data_v2"
LOG_FILE = SCRAPER_DIR / "post_gap_chain.log"
PROGRESS_FILE = DATA_DIR / "historical_progress.json"
SNAPSHOT_FILE = DATA_DIR / "daily_snapshot.json"
REPORTERS = ['SCMR', 'PLD', 'PCrLJ', 'MLD', 'CLC', 'YLR', 'PTD', 'PLC', 'CLD', 'GBLR']


def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def is_gap_filler_running():
    """Check if fill_audit_gaps_resume.py or fill_audit_gaps.py is still running."""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmdline = proc.info.get('cmdline') or []
            cmd_str = ' '.join(cmdline).lower()
            if 'fill_audit_gaps' in cmd_str and 'python' in proc.info.get('name', '').lower():
                # Don't match ourselves
                if proc.pid != os.getpid():
                    return True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return False


def count_pls_cases():
    """Count total PLS JSON files."""
    total = 0
    for reporter in REPORTERS:
        rdir = DATA_DIR / reporter
        if rdir.is_dir():
            for ydir in rdir.iterdir():
                if ydir.is_dir() and ydir.name.isdigit():
                    total += sum(1 for f in ydir.iterdir() if f.suffix == '.json')
    return total


def clean_1987_progress():
    """Remove false 'completed' flags for 1987 reporters so they get re-scraped."""
    if not PROGRESS_FILE.exists():
        log("No progress file found")
        return

    progress = json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
    completed = progress.get("completed", [])

    removed = []
    new_completed = []
    for entry in completed:
        if entry.startswith("1987-"):
            removed.append(entry)
        else:
            new_completed.append(entry)

    if removed:
        progress["completed"] = new_completed
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding='utf-8')
        log(f"Removed {len(removed)} false 'completed' flags for 1987: {removed}")
    else:
        log("No 1987 entries found in progress (already clean)")


def run_1987_scraper():
    """Re-scrape 1987 with NO timeout — the BLACK HOLE year."""
    cmd = [
        sys.executable,
        str(SCRAPER_DIR / "historical_scraper.py"),
        "--from-year", "1987",
        "--to-year", "1987"
    ]
    before = count_cases_for_year(1987)
    log(f"Starting 1987 re-scrape (current count: {before})")
    log(f"Command: {' '.join(cmd)}")

    try:
        # NO timeout — let it run as long as needed
        proc = subprocess.run(cmd, cwd=str(SCRAPER_DIR), capture_output=False)
        after = count_cases_for_year(1987)
        gained = after - before
        log(f"1987 re-scrape done: exit={proc.returncode}, before={before}, after={after}, gained=+{gained}")
        return gained
    except Exception as e:
        log(f"1987 re-scrape error: {e}")
        return 0


def count_cases_for_year(year):
    """Count cases for a specific year across all reporters."""
    total = 0
    for reporter in REPORTERS:
        ydir = DATA_DIR / reporter / str(year)
        if ydir.is_dir():
            total += sum(1 for f in ydir.iterdir() if f.suffix == '.json')
    return total


def update_daily_snapshot():
    """Update daily_snapshot.json with current PLS total."""
    total = count_pls_cases()
    today = datetime.now().strftime("%Y-%m-%d")

    snapshot = {}
    if SNAPSHOT_FILE.exists():
        snapshot = json.loads(SNAPSHOT_FILE.read_text(encoding='utf-8'))

    snapshot[today] = total
    SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False), encoding='utf-8')
    log(f"Updated daily_snapshot.json: {today} = {total}")
    return total


def print_reporter_totals():
    """Print per-reporter totals."""
    log("\nReporter Totals:")
    grand = 0
    for reporter in REPORTERS:
        rdir = DATA_DIR / reporter
        count = 0
        if rdir.is_dir():
            for ydir in rdir.iterdir():
                if ydir.is_dir() and ydir.name.isdigit():
                    count += sum(1 for f in ydir.iterdir() if f.suffix == '.json')
        grand += count
        log(f"  {reporter}: {count:,}")
    log(f"  TOTAL: {grand:,}")
    return grand


def main():
    log("=" * 60)
    log("POST-GAP-FILLER CHAIN — Starting")
    log("=" * 60)

    # Phase 1: Wait for gap filler to finish
    log("\nPhase 1: Waiting for gap filler to finish...")
    checks = 0
    while is_gap_filler_running():
        checks += 1
        if checks % 5 == 0:  # Log every 5 minutes
            log(f"  Gap filler still running (checked {checks} times)...")
        time.sleep(60)

    log(f"Gap filler finished! (waited {checks} minutes)")
    time.sleep(10)  # Brief pause

    # Phase 2: Count current state
    log("\nPhase 2: Current state before 1987 re-scrape")
    total_before = count_pls_cases()
    y1987_before = count_cases_for_year(1987)
    log(f"  PLS Total: {total_before:,}")
    log(f"  1987 count: {y1987_before}")

    # Phase 3: Clean 1987 progress flags
    log("\nPhase 3: Cleaning false 'completed' flags for 1987")
    clean_1987_progress()

    # Phase 4: Re-scrape 1987 (NO timeout)
    log("\nPhase 4: Re-scraping 1987 (BLACK HOLE) — NO timeout")
    gained_1987 = run_1987_scraper()

    # Phase 5: Final count + snapshot update
    log("\nPhase 5: Final counts")
    final_total = update_daily_snapshot()
    print_reporter_totals()

    y1987_after = count_cases_for_year(1987)
    log(f"\n1987: {y1987_before} → {y1987_after} (+{y1987_after - y1987_before})")
    log(f"PLS Total: {total_before:,} → {final_total:,} (+{final_total - total_before})")

    log("\n" + "=" * 60)
    log("POST-GAP-FILLER CHAIN — COMPLETE")
    log(f"Final PLS Total: {final_total:,}")
    log("=" * 60)


if __name__ == "__main__":
    main()
