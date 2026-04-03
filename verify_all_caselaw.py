#!/usr/bin/env python3
"""
Full case law verification across all years.
Runs verify_scraper.py --all --report with proper encoding.
Also identifies linked/legislation cases vs main scraper cases.
"""
import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Force UTF-8 with line buffering (so output flushes to redirected files)
sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)

DATA_DIR = Path(__file__).parent / "data_v2"
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]

def identify_case_sources():
    """Identify which cases came from main scraper vs linked/legislation scraper."""
    print("=" * 60)
    print("CASE SOURCE ANALYSIS")
    print("=" * 60)
    
    # Load progress to see what the main scraper tracked
    progress_file = DATA_DIR / "progress.json"
    progress = json.loads(progress_file.read_text(encoding='utf-8')) if progress_file.exists() else {}
    cases_fetched = set(progress.get("cases_fetched", []))
    
    # Check for linked cases marker
    linked_dir = DATA_DIR / "linked"
    linked_log = Path(__file__).parent / "linked_cases_log.json"
    linked_citations = set()
    if linked_log.exists():
        linked_data = json.loads(linked_log.read_text(encoding='utf-8'))
        linked_citations = set(linked_data.get("fetched", []))
    
    stats = {}
    for reporter in REPORTERS:
        reporter_dir = DATA_DIR / reporter
        if not reporter_dir.exists():
            continue
        for year_dir in sorted(reporter_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            year = year_dir.name
            json_files = list(year_dir.glob("*.json"))
            
            main_count = 0
            linked_count = 0
            unknown_count = 0
            
            for f in json_files:
                try:
                    data = json.loads(f.read_text(encoding='utf-8'))
                    citation = data.get("citation", "")
                    source = data.get("source", "")
                    
                    if source == "linked" or citation in linked_citations:
                        linked_count += 1
                    elif citation in cases_fetched or source in ("", "main", "scraper"):
                        main_count += 1
                    else:
                        unknown_count += 1
                except:
                    unknown_count += 1
            
            key = f"{year}-{reporter}"
            total = len(json_files)
            if linked_count > 0 or total > 0:
                stats[key] = {
                    "total": total,
                    "main": main_count,
                    "linked": linked_count,
                    "unknown": unknown_count
                }
    
    # Print years with linked cases
    has_linked = {k: v for k, v in stats.items() if v["linked"] > 0}
    if has_linked:
        print(f"\nYears with LINKED cases mixed in:")
        for key in sorted(has_linked.keys()):
            v = has_linked[key]
            print(f"  {key}: {v['total']} total ({v['main']} main, {v['linked']} linked, {v['unknown']} unknown)")
    else:
        print("\nNo linked cases found mixed into reporter directories.")
        print("(Linked cases may not have a 'source' field to distinguish them)")
    
    # Summary by decade
    print(f"\nCases by decade:")
    for decade_start in range(1940, 2030, 10):
        decade_total = 0
        decade_linked = 0
        for y in range(decade_start, decade_start + 10):
            for r in REPORTERS:
                key = f"{y}-{r}"
                if key in stats:
                    decade_total += stats[key]["total"]
                    decade_linked += stats[key]["linked"]
        if decade_total > 0:
            linked_str = f" ({decade_linked} linked)" if decade_linked > 0 else ""
            print(f"  {decade_start}s: {decade_total}{linked_str}")
    
    print()
    return stats


def run_full_verification():
    """Run the full PLS verification."""
    from verify_scraper import PLSVerifier
    
    print("=" * 60)
    print("FULL CASE LAW VERIFICATION vs PLS")
    print("=" * 60)
    print(f"Started: {datetime.now().isoformat()}")
    print()
    
    verifier = PLSVerifier(ignore_hours=True)
    results = verifier.verify_all()
    
    if results:
        report = verifier.generate_report(results)
        filename = f"{datetime.now().strftime('%Y-%m-%d')}_full_caselaw_verification.json"
        filepath = verifier.save_report(report, filename)
        verifier.print_summary(report)
        
        # Also print by-year breakdown
        print("\n" + "-" * 40)
        print("BY YEAR:")
        print("-" * 40)
        for year_str in sorted(report.summary_by_year.keys(), reverse=True):
            stats = report.summary_by_year[year_str]
            coverage = stats['local'] / max(stats['pls'], 1) * 100
            issues = []
            if stats['missing']: issues.append(f"{stats['missing']} missing")
            if stats['empty']: issues.append(f"{stats['empty']} empty")
            issue_str = f" ({', '.join(issues)})" if issues else ""
            print(f"  {year_str}: {stats['local']:5}/{stats['pls']:5} ({coverage:5.1f}%){issue_str}")
        
        print(f"\nReport saved: {filepath}")
        print(f"Completed: {datetime.now().isoformat()}")


if __name__ == "__main__":
    # Step 1: Identify case sources (fast, no PLS queries)
    identify_case_sources()
    
    # Step 2: Full PLS verification (slow, queries PLS)
    run_full_verification()
