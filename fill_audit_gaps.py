"""
Gap Filler — Based on Feb 21 PLS Audit
Targets the 10,224 missing cases in priority order.
Uses historical_scraper.py for each gap year.
"""
import subprocess
import sys
import os
import json
import time
from datetime import datetime

SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRAPER_DIR, "fill_audit_gaps.log")

# Priority-ordered gap years based on audit
# Format: (year, expected_missing, notes)
GAP_YEARS = [
    # 1987 is the BLACK HOLE — ~3,900 missing
    (1987, 3900, "BLACK HOLE - biggest gap"),
    # PCrLJ/PLC gaps in 1970s-1980s
    (1972, 255, "PCrLJ 255 missing"),
    (1978, 303, "PCrLJ 303 missing"),
    (1988, 950, "PCrLJ 486 + PLC 308 + PTD 157"),
    # CLD gaps
    (2007, 424, "CLD 244 + YLR 424"),
    (2009, 468, "CLD 241 + YLR 227"),
    (2010, 224, "CLD 224"),
    # PLD/SCMR historical gaps
    (1968, 344, "SCMR 166 + PLD 178"),
    (1977, 105, "PLD 105"),
    (1983, 89, "PLD 89"),
    (1976, 80, "PLD 80"),
    (1986, 77, "PLD 77"),
    (1984, 98, "CLC 98"),
    (1985, 255, "PLC 255"),
    (1989, 208, "MLD 208"),
    # PTD missing years
    (1961, 144, "PTD 144"),
    (1962, 0, "PTD - check if PLS has data"),
    (1967, 0, "PTD - check if PLS has data"),
    (1970, 0, "PTD - check if PLS has data"),
    (1974, 0, "PTD - check if PLS has data"),
    (1975, 0, "PTD + PLC gaps"),
    (1979, 0, "PTD - check if PLS has data"),
    # PLC missing years
    (1963, 188, "PLC 188"),
    (1980, 0, "PLC - check if PLS has data"),
    # GBLR
    (2010, 126, "GBLR 126 - new reporter year"),
    (2011, 0, "GBLR - check"),
    (2012, 0, "GBLR - check"),
    (2013, 0, "GBLR - check"),
    (2017, 0, "GBLR - check"),
    (2018, 0, "GBLR - check"),
    (2019, 0, "GBLR - check"),
    (2020, 0, "GBLR - check"),
    (2021, 0, "GBLR - check"),
    (2022, 0, "GBLR - check"),
    (2023, 0, "GBLR - check"),
    (2024, 0, "GBLR - check"),
    (2025, 0, "GBLR - check"),
]

# Deduplicate years (keep first occurrence / highest priority)
seen = set()
unique_gaps = []
for year, missing, notes in GAP_YEARS:
    if year not in seen:
        seen.add(year)
        unique_gaps.append((year, missing, notes))

def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def count_cases(year):
    """Count local JSON files for a specific year across all reporters."""
    reporters = ['SCMR','PLD','PCrLJ','MLD','CLC','YLR','PTD','PLC','CLD','GBLR']
    base = os.path.join(SCRAPER_DIR, "data_v2")
    total = 0
    for r in reporters:
        yp = os.path.join(base, r, str(year))
        if os.path.isdir(yp):
            total += sum(1 for f in os.listdir(yp) if f.endswith('.json'))
    return total

def run_scraper(year):
    """Run historical_scraper.py for a single year."""
    cmd = [
        sys.executable, 
        os.path.join(SCRAPER_DIR, "historical_scraper.py"),
        "--from-year", str(year),
        "--to-year", str(year)
    ]
    log(f"Starting scraper for {year}: {' '.join(cmd)}")
    
    before = count_cases(year)
    
    proc = subprocess.run(
        cmd,
        cwd=SCRAPER_DIR,
        capture_output=False,
        timeout=7200  # 2 hour max per year
    )
    
    after = count_cases(year)
    gained = after - before
    
    log(f"Year {year} done: exit={proc.returncode}, before={before}, after={after}, gained={gained}")
    return gained

def main():
    log("=" * 60)
    log("AUDIT GAP FILLER — Starting")
    log(f"Total gap years to process: {len(unique_gaps)}")
    log("=" * 60)
    
    total_gained = 0
    results = []
    
    for i, (year, expected, notes) in enumerate(unique_gaps, 1):
        before = count_cases(year)
        log(f"\n[{i}/{len(unique_gaps)}] Year {year} — {notes}")
        log(f"  Current local count: {before}")
        
        try:
            gained = run_scraper(year)
            total_gained += gained
            results.append((year, gained, notes))
            log(f"  ✅ Gained {gained} cases (total so far: +{total_gained})")
        except subprocess.TimeoutExpired:
            log(f"  ⚠️ Year {year} timed out after 2 hours")
            results.append((year, -1, "TIMEOUT"))
        except Exception as e:
            log(f"  ❌ Year {year} failed: {e}")
            results.append((year, -1, str(e)))
        
        # Brief pause between years
        time.sleep(5)
    
    log("\n" + "=" * 60)
    log("AUDIT GAP FILLER — COMPLETE")
    log(f"Total new cases: +{total_gained}")
    log("=" * 60)
    
    # Summary
    log("\nResults by year:")
    for year, gained, notes in results:
        status = f"+{gained}" if gained >= 0 else "FAILED"
        log(f"  {year}: {status} ({notes})")
    
    # Save results
    results_file = os.path.join(SCRAPER_DIR, "data_v2", "audit", "gap_fill_results.json")
    with open(results_file, "w") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "total_gained": total_gained,
            "results": [{"year": y, "gained": g, "notes": n} for y, g, n in results]
        }, f, indent=2)
    log(f"Results saved to {results_file}")

if __name__ == "__main__":
    main()
