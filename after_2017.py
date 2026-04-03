"""
After 2017 scraping completes:
1. Report 2017 results
2. Run a STRICT audit of 2018 (exclude linked cases)
3. If missing cases found, scrape them
4. Then start 2016
"""
import json
import sys
import time
from pathlib import Path
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path("data_v2")
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

def count_year(year):
    total = 0
    by_reporter = {}
    for r in REPORTERS:
        d = DATA_DIR / r / str(year)
        if d.exists():
            c = len(list(d.glob("*.json")))
            by_reporter[r] = c
            total += c
    return total, by_reporter

def strict_audit_2018():
    """Compare 2018 local cases vs PLS, excluding linked cases."""
    print(f"\n{'='*60}")
    print(f"STRICT AUDIT: 2018 CASE LAW")
    print(f"{'='*60}")
    
    # Load what main scraper tracked
    progress = json.loads((DATA_DIR / "progress.json").read_text(encoding="utf-8"))
    cases_fetched = set(progress.get("cases_fetched", []))
    
    from verify_scraper import PLSVerifier
    verifier = PLSVerifier(ignore_hours=True)
    
    results_2018 = verifier.verify_year(2018, fix=False)
    
    total_pls = 0
    total_local = 0
    total_missing = 0
    total_extra = 0
    linked_in_2018 = 0
    
    for r in results_2018:
        total_pls += r.pls_count
        total_local += r.local_count
        total_missing += len(r.missing_cases)
        total_extra += len(r.extra_cases)
        
        # Count how many local cases are linked (not from main scraper)
        local_cases = verifier.get_local_cases(2018, r.reporter)
        for cit in local_cases:
            if cit not in cases_fetched:
                linked_in_2018 += 1
        
        status = "[OK]" if not r.missing_cases else f"[MISSING {len(r.missing_cases)}]"
        extra_str = f" (+{len(r.extra_cases)} extra/linked)" if r.extra_cases else ""
        print(f"  {r.reporter}: {r.local_count}/{r.pls_count} {status}{extra_str}")
        
        if r.missing_cases:
            for mc in r.missing_cases[:5]:
                print(f"    -> Missing: {mc['citation']}")
            if len(r.missing_cases) > 5:
                print(f"    -> ... and {len(r.missing_cases)-5} more")
    
    print(f"\nSummary:")
    print(f"  PLS total: {total_pls}")
    print(f"  Local total: {total_local}")
    print(f"  Missing: {total_missing}")
    print(f"  Extra/linked: {total_extra}")
    print(f"  Linked cases in 2018 dirs: {linked_in_2018}")
    print(f"  Main scraper cases: {total_local - linked_in_2018}")
    
    # If missing, scrape them
    if total_missing > 0:
        print(f"\n>>> {total_missing} cases missing! Starting fix...")
        for r in results_2018:
            if r.missing_cases:
                fixed = verifier.fix_missing_cases(r)
                print(f"  {r.reporter}: fixed {fixed}/{len(r.missing_cases)}")
        
        # Re-count after fix
        new_total, new_by = count_year(2018)
        print(f"\nAfter fix: {new_total} total cases for 2018")
    else:
        print(f"\n>>> 2018 is COMPLETE - no missing cases!")
    
    # Save audit
    report = verifier.generate_report(results_2018)
    filename = f"{datetime.now().strftime('%Y-%m-%d')}_2018_strict_audit.json"
    verifier.save_report(report, filename)
    print(f"Report saved: data_v2/audit/{filename}")
    
    return total_missing


def start_2016_scraper():
    """Fire up 2016 scraper as detached process."""
    import subprocess, os
    print(f"\n>>> Starting 2016 case law scraper...")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    subprocess.Popen(
        ["python", "-u", "pls_scraper_v2.py", "scrape", "--year", "2016"],
        cwd=str(Path(__file__).parent),
        stdout=open("logs/case_2016_stdout.log", "w"),
        stderr=open("logs/case_2016_stderr.log", "w"),
        env=env,
        creationflags=0x00000008  # DETACHED_PROCESS on Windows
    )
    print("2016 scraper launched in background!")


if __name__ == "__main__":
    # Step 1: Report 2017 results
    print(f"[{datetime.now().strftime('%H:%M:%S')}] 2017 scraping results")
    total_2017, by_2017 = count_year(2017)
    print(f"2017 total on disk: {total_2017}")
    for r, c in sorted(by_2017.items()):
        print(f"  {r}: {c}")
    
    # Step 2: Strict audit + fix 2018
    missing = strict_audit_2018()
    
    # Step 3: Start 2016
    start_2016_scraper()
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Done. 2016 scraper running in background.")
