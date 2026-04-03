# -*- coding: utf-8 -*-
"""
Post-gap-fill verification chain.
Waits for fill_all_gaps.py to finish, then verifies years 2010-2000.
Saves results to verification_summary.json and prints report.
"""
import subprocess
import sys
import os
import time
import json
from datetime import datetime

SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))
VERIFIER = os.path.join(SCRAPER_DIR, 'verify_scraper.py')
YEARS_TO_VERIFY = list(range(2010, 1999, -1))  # 2010, 2009, ..., 2000
SUMMARY_FILE = os.path.join(SCRAPER_DIR, 'data_v2', 'verification_summary.json')

def is_gap_filler_running():
    """Check if fill_all_gaps.py or historical_scraper.py is still running."""
    try:
        result = subprocess.run(
            ['powershell', '-Command',
             "Get-CimInstance Win32_Process | Where-Object { "
             "($_.CommandLine -match 'fill_all_gaps|historical_scraper') -and "
             "$_.Name -match 'python' -and "
             f"$_.ProcessId -ne {os.getpid()} "
             "} | Select-Object ProcessId"],
            capture_output=True, text=True, timeout=30, encoding='utf-8', errors='replace'
        )
        lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip() and l.strip().isdigit()]
        return len(lines) > 0
    except:
        return False

def wait_for_gap_filler():
    """Wait until fill_all_gaps.py finishes."""
    print(f"[{datetime.now().strftime('%H:%M')}] Waiting for gap filler to finish...")
    checks = 0
    while is_gap_filler_running():
        checks += 1
        if checks % 12 == 0:  # Every 10 minutes
            print(f"[{datetime.now().strftime('%H:%M')}] Gap filler still running... (checked {checks} times)")
        time.sleep(50)
    print(f"[{datetime.now().strftime('%H:%M')}] Gap filler finished. Starting verification...")
    # Brief pause to let files settle
    time.sleep(30)

def verify_year(year):
    """Run verify_scraper.py for a single year."""
    print(f"\n[{datetime.now().strftime('%H:%M')}] Verifying {year}...")
    try:
        result = subprocess.run(
            [sys.executable, '-u', VERIFIER, '--year', str(year)],
            cwd=SCRAPER_DIR,
            capture_output=True, text=True, timeout=600,
            encoding='utf-8', errors='replace'
        )
        output = result.stdout
        
        # Parse coverage from output
        coverage = None
        total_pls = 0
        total_scraped = 0
        missing = 0
        
        for line in output.split('\n'):
            if 'Coverage:' in line:
                try:
                    coverage = float(line.split(':')[1].strip().replace('%', ''))
                except:
                    pass
            if 'Total cases on PLS:' in line:
                try:
                    total_pls = int(line.split(':')[1].strip().replace(',', ''))
                except:
                    pass
            if 'Total cases scraped:' in line:
                try:
                    total_scraped = int(line.split(':')[1].strip().replace(',', ''))
                except:
                    pass
            if 'Missing cases:' in line:
                try:
                    missing = int(line.split(':')[1].strip().replace(',', ''))
                except:
                    pass
        
        status = "PASS" if coverage and coverage >= 95 else "GAPS" if coverage else "ERROR"
        print(f"  {year}: {total_scraped}/{total_pls} ({coverage}%) - {missing} missing [{status}]")
        
        return {
            "year": year,
            "pls_total": total_pls,
            "scraped": total_scraped,
            "coverage": coverage,
            "missing": missing,
            "status": status,
            "verified_at": datetime.now().isoformat()
        }
    except subprocess.TimeoutExpired:
        print(f"  {year}: TIMEOUT")
        return {"year": year, "status": "TIMEOUT", "verified_at": datetime.now().isoformat()}
    except Exception as e:
        print(f"  {year}: ERROR - {e}")
        return {"year": year, "status": "ERROR", "error": str(e), "verified_at": datetime.now().isoformat()}

def main():
    print("=" * 60)
    print("  POST-GAP-FILL VERIFICATION")
    print(f"  Years: {YEARS_TO_VERIFY[0]} down to {YEARS_TO_VERIFY[-1]}")
    print("=" * 60)
    
    # Wait for gap filler
    wait_for_gap_filler()
    
    # Verify each year
    results = []
    for year in YEARS_TO_VERIFY:
        r = verify_year(year)
        results.append(r)
        time.sleep(5)  # Brief pause between years
    
    # Save results
    with open(SUMMARY_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "years_verified": len(results),
            "results": results
        }, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 60)
    print("  VERIFICATION SUMMARY")
    print("=" * 60)
    
    total_pls = 0
    total_scraped = 0
    total_missing = 0
    
    for r in results:
        icon = "OK" if r.get('status') == 'PASS' else "!!" if r.get('status') == 'GAPS' else "XX"
        cov = f"{r.get('coverage', 0):.1f}%" if r.get('coverage') else "N/A"
        print(f"  [{icon}] {r['year']}: {r.get('scraped', '?')}/{r.get('pls_total', '?')} ({cov}) - {r.get('missing', '?')} missing")
        total_pls += r.get('pls_total', 0)
        total_scraped += r.get('scraped', 0)
        total_missing += r.get('missing', 0)
    
    overall = (total_scraped / total_pls * 100) if total_pls > 0 else 0
    print(f"\n  TOTAL: {total_scraped:,}/{total_pls:,} ({overall:.1f}%) - {total_missing:,} missing")
    print(f"\n  Report saved: {SUMMARY_FILE}")

if __name__ == '__main__':
    main()
