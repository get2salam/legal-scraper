#!/usr/bin/env python3
"""
Year Verifier - Pre and Post scrape verification
Usage:
  python verify_year.py pre 2021   # Before scraping - shows what to expect
  python verify_year.py post 2021  # After scraping - confirms completion
"""

import sys
import glob
import json
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD']


def get_pls_counts(year):
    """Query PLS for case counts by reporter"""
    from pls_scraper_v2 import PLSScraperV2
    
    scraper = PLSScraperV2()
    if not scraper.login():
        logger.error("Failed to login to PLS")
        return None
    
    counts = {}
    total = 0
    for r in REPORTERS:
        cases = scraper.citation_search(year, r)
        count = len(cases) if cases else 0
        counts[r] = count
        total += count
    counts['TOTAL'] = total
    return counts


def get_local_counts(year):
    """Count local files by reporter"""
    counts = {}
    total = 0
    for r in REPORTERS:
        count = len(glob.glob(str(DATA_DIR / r / str(year) / "*.json")))
        counts[r] = count
        total += count
    counts['TOTAL'] = total
    return counts


def check_formats(year):
    """Check all 4 formats are in sync"""
    results = {}
    for r in REPORTERS:
        json_count = len(glob.glob(str(DATA_DIR / r / str(year) / "*.json")))
        orig_count = len(glob.glob(str(DATA_DIR / r / str(year) / "original" / "*.html")))
        read_count = len(glob.glob(str(DATA_DIR / "html" / r / str(year) / "*.html")))
        
        if json_count > 0:
            synced = json_count == orig_count == read_count
            results[r] = {
                'json': json_count,
                'original': orig_count,
                'readable': read_count,
                'synced': synced
            }
    return results


def pre_verify(year):
    """Pre-scrape verification - what to expect from PLS"""
    logger.info("=" * 60)
    logger.info(f"PRE-SCRAPE VERIFICATION: {year}")
    logger.info("=" * 60)
    
    pls_counts = get_pls_counts(year)
    local_counts = get_local_counts(year)
    
    if not pls_counts:
        return False
    
    print(f"\n{'Reporter':<10} {'PLS':<8} {'Local':<8} {'To Scrape':<10}")
    print("-" * 40)
    
    total_to_scrape = 0
    for r in REPORTERS:
        pls = pls_counts.get(r, 0)
        local = local_counts.get(r, 0)
        to_scrape = max(0, pls - local)
        total_to_scrape += to_scrape
        print(f"{r:<10} {pls:<8} {local:<8} {to_scrape:<10}")
    
    print("-" * 40)
    print(f"{'TOTAL':<10} {pls_counts['TOTAL']:<8} {local_counts['TOTAL']:<8} {total_to_scrape:<10}")
    
    # Save report
    report = {
        'type': 'pre',
        'year': year,
        'timestamp': datetime.now().isoformat(),
        'pls_counts': pls_counts,
        'local_counts': local_counts,
        'to_scrape': total_to_scrape
    }
    
    report_dir = DATA_DIR / "verification_reports"
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f"pre_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_file.write_text(json.dumps(report, indent=2))
    
    logger.info(f"\nReport saved: {report_file.name}")
    logger.info(f"Total to scrape: {total_to_scrape} cases")
    
    return True


def post_verify(year):
    """Post-scrape verification - confirm completion"""
    logger.info("=" * 60)
    logger.info(f"POST-SCRAPE VERIFICATION: {year}")
    logger.info("=" * 60)
    
    pls_counts = get_pls_counts(year)
    local_counts = get_local_counts(year)
    format_check = check_formats(year)
    
    if not pls_counts:
        return False
    
    print(f"\n{'Reporter':<10} {'PLS':<8} {'Local':<8} {'Status':<15} {'Formats'}")
    print("-" * 60)
    
    all_complete = True
    all_synced = True
    
    for r in REPORTERS:
        pls = pls_counts.get(r, 0)
        local = local_counts.get(r, 0)
        
        if pls == 0:
            status = "N/A"
        elif local >= pls:
            status = "COMPLETE"
        else:
            status = f"MISSING {pls - local}"
            all_complete = False
        
        fmt = format_check.get(r, {})
        if fmt:
            fmt_status = "SYNCED" if fmt.get('synced') else f"GAP J:{fmt['json']} O:{fmt['original']} R:{fmt['readable']}"
            if not fmt.get('synced'):
                all_synced = False
        else:
            fmt_status = "-"
        
        print(f"{r:<10} {pls:<8} {local:<8} {status:<15} {fmt_status}")
    
    print("-" * 60)
    pct = (local_counts['TOTAL'] / pls_counts['TOTAL'] * 100) if pls_counts['TOTAL'] > 0 else 0
    print(f"{'TOTAL':<10} {pls_counts['TOTAL']:<8} {local_counts['TOTAL']:<8} {pct:.1f}%")
    
    # Summary
    print("\n" + "=" * 60)
    if all_complete and all_synced:
        print(f"✅ {year} VERIFICATION PASSED - All cases scraped, all formats synced")
    else:
        if not all_complete:
            print(f"❌ {year} INCOMPLETE - Missing {pls_counts['TOTAL'] - local_counts['TOTAL']} cases")
        if not all_synced:
            print(f"⚠️  FORMAT MISMATCH - Run gen_readable_html.py to fix")
    print("=" * 60)
    
    # Save report
    report = {
        'type': 'post',
        'year': year,
        'timestamp': datetime.now().isoformat(),
        'pls_counts': pls_counts,
        'local_counts': local_counts,
        'format_check': format_check,
        'complete': all_complete,
        'synced': all_synced,
        'coverage_pct': pct
    }
    
    report_dir = DATA_DIR / "verification_reports"
    report_dir.mkdir(exist_ok=True)
    report_file = report_dir / f"post_{year}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_file.write_text(json.dumps(report, indent=2))
    
    logger.info(f"Report saved: {report_file.name}")
    
    return all_complete and all_synced


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python verify_year.py <pre|post> <year>")
        sys.exit(1)
    
    mode = sys.argv[1].lower()
    year = int(sys.argv[2])
    
    if mode == 'pre':
        pre_verify(year)
    elif mode == 'post':
        post_verify(year)
    else:
        print(f"Unknown mode: {mode}. Use 'pre' or 'post'")
        sys.exit(1)
