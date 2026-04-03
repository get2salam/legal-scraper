"""Run full case law verification against PLS. Detached-friendly."""
import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Force line-buffered UTF-8
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

os.chdir(str(Path(__file__).parent))

from verify_scraper import PLSVerifier

print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting full case law verification...")
print(f"This queries PLS for every reporter/year we have data for.")
print(f"Expected: ~80 years x 10 reporters = ~800 queries (2-4s each = ~30-50 min)")
print()

verifier = PLSVerifier(ignore_hours=True)
results = verifier.verify_all()

if results:
    report = verifier.generate_report(results)
    filename = f"{datetime.now().strftime('%Y-%m-%d')}_full_caselaw_verification.json"
    filepath = verifier.save_report(report, filename)
    verifier.print_summary(report)
    
    # By-year breakdown
    print()
    print("-" * 50)
    print("BY YEAR (descending):")
    print("-" * 50)
    for year_str in sorted(report.summary_by_year.keys(), reverse=True):
        stats = report.summary_by_year[year_str]
        if stats['pls'] == 0 and stats['local'] == 0:
            continue
        coverage = stats['local'] / max(stats['pls'], 1) * 100
        issues = []
        if stats['missing']:
            issues.append(f"{stats['missing']} missing")
        if stats['empty']:
            issues.append(f"{stats['empty']} empty")
        issue_str = f" ({', '.join(issues)})" if issues else ""
        extra = stats['local'] - stats['pls']
        extra_str = f" [+{extra} extra/linked]" if extra > 0 else ""
        print(f"  {year_str}: {stats['local']:5}/{stats['pls']:5} ({coverage:5.1f}%){issue_str}{extra_str}")
    
    print(f"\nReport saved: {filepath}")

print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Verification complete.")
