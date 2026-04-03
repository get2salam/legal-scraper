"""
Autonomous gap filler — runs historical_scraper.py for all gap years back-to-back.
Waits for current 2010→2006 scraper to finish, then fills remaining gaps.

Gap years (top-down):
  2005→2000  (2004-2001 all have gaps)
  1989       (MLD 208 missing + more)
  1988→1980  (1980-1987 nearly empty, 1988 partial)
"""
import subprocess
import sys
import os
import time
import json
from datetime import datetime

SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
SCRAPER = os.path.join(SCRAPER_DIR, 'historical_scraper.py')

# Define gap ranges to fill (in order)
GAP_RANGES = [
    (2005, 2000, "2005→2000 gaps"),
    (1989, 1980, "1989→1980 dead zone"),
]

LOG_FILE = os.path.join(SCRAPER_DIR, 'data_v2', 'gap_fill_log.json')

def log_event(event, details=""):
    """Append to gap fill log."""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event": event,
        "details": details
    }
    log = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE, 'r') as f:
                log = json.load(f)
        except:
            pass
    log.append(entry)
    with open(LOG_FILE, 'w') as f:
        json.dump(log, f, indent=2)
    print(f"{datetime.now().strftime('%H:%M:%S')} | {event}: {details}")

def wait_for_existing_scraper():
    """Wait for the 2010→2006 scraper (PID 40652) to finish."""
    print("Checking for running scrapers...")
    while True:
        try:
            result = subprocess.run(
                ['powershell', '-Command',
                 "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'historical_scraper|pls_scraper|scraper_chain' -and $_.Name -match 'python' -and $_.ProcessId -ne " + str(os.getpid()) + " } | Select-Object ProcessId"],
                capture_output=True, text=True, timeout=30
            )
            lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip() and l.strip().isdigit()]
            if not lines:
                print("No other scrapers running. Starting gap fill...")
                return
            print(f"Scrapers still running (PIDs: {', '.join(lines)}). Waiting 5 min...")
            time.sleep(300)
        except Exception as e:
            print(f"Error checking processes: {e}. Waiting 2 min...")
            time.sleep(120)

def run_scraper(from_year, to_year, label):
    """Run historical_scraper.py for a year range."""
    log_event("STARTING", f"{label} (years {from_year}→{to_year})")
    
    cmd = [sys.executable, '-u', SCRAPER, '--from-year', str(from_year), '--to-year', str(to_year)]
    
    start = time.time()
    result = subprocess.run(cmd, cwd=SCRAPER_DIR)
    elapsed = (time.time() - start) / 60
    
    if result.returncode == 0:
        log_event("COMPLETED", f"{label} in {elapsed:.0f} min")
    else:
        log_event("ERROR", f"{label} exited with code {result.returncode} after {elapsed:.0f} min")
    
    # Brief pause between ranges
    time.sleep(30)
    return result.returncode

def main():
    log_event("GAP FILLER STARTED", f"Ranges: {len(GAP_RANGES)}")
    
    # Wait for current scraper to finish
    wait_for_existing_scraper()
    
    total_start = time.time()
    results = []
    
    for from_year, to_year, label in GAP_RANGES:
        rc = run_scraper(from_year, to_year, label)
        results.append((label, rc))
    
    total_min = (time.time() - total_start) / 60
    
    # Summary
    summary = f"All gap ranges complete in {total_min:.0f} min. "
    for label, rc in results:
        status = "✅" if rc == 0 else "❌"
        summary += f"{status} {label}; "
    
    log_event("ALL DONE", summary)
    print(f"\n{'='*60}")
    print(f"GAP FILLER COMPLETE — {total_min:.0f} minutes total")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
