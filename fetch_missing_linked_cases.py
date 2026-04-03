#!/usr/bin/env python3
"""
Fetch Missing Linked Cases
===========================
Scans all statute JSONs for cases with exists_locally=False and fetches them from PLS.

Features:
- Scans all statute JSON files for missing case references
- Fetches cases from PLS using the existing scraper
- Updates statute JSONs with case metadata after fetching
- Respects rate limits and PLS operating hours
- Tracks progress and can resume interrupted runs

Usage:
    python fetch_missing_linked_cases.py scan              # Scan and report missing cases
    python fetch_missing_linked_cases.py fetch             # Fetch all missing cases
    python fetch_missing_linked_cases.py fetch --limit 50  # Fetch first 50 missing cases
    python fetch_missing_linked_cases.py update            # Update statute JSONs with case metadata
"""

import os
import re
import json
import time
import random
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set

from dotenv import load_dotenv

from case_link_enricher import (
    scan_all_missing_cases,
    update_case_links_after_fetch,
    parse_citation,
    get_case_local_path,
)

# Pipeline status reporting (optional)
try:
    from pipeline_status import PipelineStatusReporter, ScriptType
    _status_reporter = PipelineStatusReporter(ScriptType.SCRAPER, "fetch_missing_linked_cases")
    HAS_STATUS_REPORTER = True
except ImportError:
    _status_reporter = None
    HAS_STATUS_REPORTER = False

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
# Configuration
# ══════════════════════════════════════════════════════════════════════════════

DATA_DIR = Path(__file__).parent / "data_v2"
LEGISLATION_DIR = DATA_DIR / "legislation"
PROGRESS_FILE = DATA_DIR / "fetch_linked_cases_progress.json"

# Timing (conservative for linked case fetching)
MIN_DELAY = 4.0
MAX_DELAY = 10.0
RATE_LIMIT_BACKOFF = 120

# Break simulation
REQUESTS_BEFORE_BREAK = 20
BREAK_MIN = 60
BREAK_MAX = 180

# PLS Operating Hours (PKT = UTC+5) - use day shift
PLS_OPEN_HOUR = 7
PLS_CLOSE_HOUR = 21
PKT_OFFSET = timedelta(hours=5)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Progress Management
# ══════════════════════════════════════════════════════════════════════════════

def load_progress() -> Dict:
    """Load progress from file."""
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
        except:
            pass
    return {
        "fetched_cases": [],
        "failed_cases": [],
        "total_fetched": 0,
        "last_updated": None
    }


def save_progress(progress: Dict):
    """Save progress to file."""
    progress["last_updated"] = datetime.now().isoformat()
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding='utf-8')


# ══════════════════════════════════════════════════════════════════════════════
# Case Fetching
# ══════════════════════════════════════════════════════════════════════════════

def fetch_cases(missing_cases: List[Dict], limit: int = None, ignore_hours: bool = True) -> int:
    """
    Fetch missing cases from PLS.
    
    Args:
        missing_cases: List of case dicts with citation info
        limit: Maximum number of cases to fetch (None = all)
        ignore_hours: Ignore PLS operating hours check
        
    Returns:
        Number of cases successfully fetched
    """
    # Import the case scraper
    try:
        from pls_scraper_v2 import PLSScraperV2
    except ImportError:
        logger.error("Cannot import pls_scraper_v2. Make sure it exists.")
        return 0
    
    progress = load_progress()
    already_fetched = set(progress.get("fetched_cases", []))
    already_failed = set(progress.get("failed_cases", []))
    
    # Filter out already processed cases
    to_fetch = []
    for case in missing_cases:
        citation = case.get("citation", "")
        if citation and citation not in already_fetched and citation not in already_failed:
            to_fetch.append(case)
    
    if limit:
        to_fetch = to_fetch[:limit]
    
    if not to_fetch:
        logger.info("No new cases to fetch")
        return 0
    
    logger.info(f"Fetching {len(to_fetch)} missing cases...")
    
    if HAS_STATUS_REPORTER and _status_reporter:
        _status_reporter.start(task=f"Fetching {len(to_fetch)} linked cases")
    
    # Initialize scraper
    scraper = PLSScraperV2(ignore_hours=ignore_hours)
    if not scraper.login():
        logger.error("Failed to login to PLS")
        return 0
    
    fetched = 0
    failed = 0
    requests_since_break = 0
    
    for i, case in enumerate(to_fetch):
        citation = case.get("citation", "")
        year = case.get("year", "")
        reporter = case.get("reporter", "")
        page = case.get("page", "")
        
        logger.info(f"[{i+1}/{len(to_fetch)}] Fetching: {citation}")
        
        try:
            # Check operating hours
            if not ignore_hours and not scraper._is_pls_open():
                scraper._wait_for_pls_open()
                if not scraper.login():
                    logger.error("Failed to re-login after waiting")
                    break
            
            # Take breaks periodically
            requests_since_break += 1
            if requests_since_break >= REQUESTS_BEFORE_BREAK:
                break_time = random.uniform(BREAK_MIN, BREAK_MAX)
                logger.info(f"Taking a {break_time:.0f}s break...")
                time.sleep(break_time)
                requests_since_break = 0
            
            # Human-like delay
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            time.sleep(delay)
            
            # Search for the case by citation
            # The scraper needs casetypeid, so we search for it first
            search_resp = scraper._request("POST", f"{scraper.session.headers.get('Origin', 'https://www.pakistanlawsite.com')}/Login/CitationSearch", data={
                "year": year,
                "book": reporter,
                "code": "",
                "court": "",
                "judge": "",
                "lawyer": "",
                "party": "",
            })
            
            if not search_resp:
                logger.warning(f"  Search failed for {citation}")
                progress["failed_cases"].append(citation)
                failed += 1
                save_progress(progress)
                continue
            
            # Parse search results to find our case
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(search_resp.text, 'html.parser')
            
            # Look for matching citation
            case_id = None
            for row in soup.find_all('tr', class_='caseType'):
                cells = row.find_all('td')
                if len(cells) >= 2:
                    row_citation = cells[1].get_text(strip=True)
                    # Normalize and compare
                    row_citation_norm = re.sub(r'\s+', ' ', row_citation.strip())
                    target_citation_norm = f"{year} {reporter} {page}"
                    
                    if target_citation_norm in row_citation_norm:
                        btn = row.find('input', attrs={'casetypeid': True})
                        if btn:
                            case_id = btn.get('casetypeid', '')
                            break
            
            if not case_id:
                logger.warning(f"  Case not found in search results: {citation}")
                progress["failed_cases"].append(citation)
                failed += 1
                save_progress(progress)
                continue
            
            # Fetch the case
            time.sleep(random.uniform(1.5, 3))
            case_data = scraper.fetch_case(case_id, citation)
            
            if case_data:
                scraper._save_case(case_data)
                progress["fetched_cases"].append(citation)
                progress["total_fetched"] += 1
                fetched += 1
                logger.info(f"  [OK] Fetched: {citation}")
            else:
                logger.warning(f"  Failed to fetch case content: {citation}")
                progress["failed_cases"].append(citation)
                failed += 1
            
            save_progress(progress)
            
            # Update status
            if HAS_STATUS_REPORTER and _status_reporter and (i + 1) % 10 == 0:
                _status_reporter.progress_update(i + 1, len(to_fetch), f"Fetched {fetched}, failed {failed}")
                
        except KeyboardInterrupt:
            logger.info("Interrupted. Saving progress...")
            save_progress(progress)
            break
        except Exception as e:
            logger.error(f"  Error fetching {citation}: {e}")
            progress["failed_cases"].append(citation)
            failed += 1
            save_progress(progress)
            time.sleep(RATE_LIMIT_BACKOFF)
    
    logger.info(f"Fetching complete: {fetched} fetched, {failed} failed")
    
    if HAS_STATUS_REPORTER and _status_reporter:
        _status_reporter.complete(success=True, message=f"{fetched} fetched, {failed} failed")
    
    return fetched


def update_all_statute_case_links() -> int:
    """
    Update all statute JSONs with newly fetched case metadata.
    
    Returns:
        Total number of case links updated
    """
    total_updated = 0
    
    for letter_dir in sorted(LEGISLATION_DIR.iterdir()):
        if not letter_dir.is_dir() or letter_dir.name in ["original"]:
            continue
        
        for json_file in letter_dir.glob("*.json"):
            updated = update_case_links_after_fetch(json_file)
            if updated > 0:
                logger.info(f"Updated {updated} case links in {json_file.name}")
                total_updated += updated
    
    return total_updated


# ══════════════════════════════════════════════════════════════════════════════
# Reporting
# ══════════════════════════════════════════════════════════════════════════════

def generate_missing_cases_report(missing_cases: List[Dict]) -> str:
    """Generate a report of missing cases."""
    
    # Group by reporter
    by_reporter = {}
    for case in missing_cases:
        reporter = case.get("reporter", "Unknown")
        if reporter not in by_reporter:
            by_reporter[reporter] = []
        by_reporter[reporter].append(case)
    
    # Group by year
    by_year = {}
    for case in missing_cases:
        year = case.get("year", "Unknown")
        if year not in by_year:
            by_year[year] = []
        by_year[year].append(case)
    
    lines = [
        "=" * 60,
        "MISSING LINKED CASES REPORT",
        "=" * 60,
        f"Generated: {datetime.now().isoformat()}",
        f"Total missing cases: {len(missing_cases)}",
        "",
        "-" * 40,
        "BY REPORTER:",
        "-" * 40,
    ]
    
    for reporter in sorted(by_reporter.keys()):
        cases = by_reporter[reporter]
        lines.append(f"  {reporter}: {len(cases)} cases")
    
    lines.extend([
        "",
        "-" * 40,
        "BY YEAR (top 10):",
        "-" * 40,
    ])
    
    for year in sorted(by_year.keys(), reverse=True)[:10]:
        cases = by_year[year]
        lines.append(f"  {year}: {len(cases)} cases")
    
    if len(missing_cases) <= 50:
        lines.extend([
            "",
            "-" * 40,
            "ALL MISSING CASES:",
            "-" * 40,
        ])
        for case in missing_cases:
            lines.append(f"  {case.get('citation', '')} (from {case.get('source_statute', '')[:40]})")
    else:
        lines.extend([
            "",
            "-" * 40,
            "SAMPLE (first 20):",
            "-" * 40,
        ])
        for case in missing_cases[:20]:
            lines.append(f"  {case.get('citation', '')} (from {case.get('source_statute', '')[:40]})")
        lines.append(f"  ... and {len(missing_cases) - 20} more")
    
    lines.append("=" * 60)
    
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Fetch missing cases referenced in statutes"
    )
    parser.add_argument("command", choices=["scan", "fetch", "update", "status"],
                        help="Command to run")
    parser.add_argument("--limit", "-n", type=int, help="Limit number of cases to fetch")
    parser.add_argument("--respect-hours", action="store_true",
                        help="Respect PLS operating hours (default: 24/7)")
    parser.add_argument("--save-report", help="Save scan report to file")
    
    args = parser.parse_args()
    
    if args.command == "scan":
        logger.info("Scanning for missing cases...")
        missing = scan_all_missing_cases()
        
        report = generate_missing_cases_report(missing)
        print(report)
        
        if args.save_report:
            Path(args.save_report).write_text(report, encoding='utf-8')
            logger.info(f"Report saved to {args.save_report}")
    
    elif args.command == "fetch":
        logger.info("Scanning for missing cases...")
        missing = scan_all_missing_cases()
        
        if not missing:
            logger.info("No missing cases found!")
            return
        
        logger.info(f"Found {len(missing)} missing cases")
        
        fetched = fetch_cases(missing, limit=args.limit, ignore_hours=(not args.respect_hours))
        
        if fetched > 0:
            logger.info(f"\nUpdating statute case links...")
            updated = update_all_statute_case_links()
            logger.info(f"Updated {updated} case links in statutes")
    
    elif args.command == "update":
        logger.info("Updating statute case links with fetched case metadata...")
        updated = update_all_statute_case_links()
        logger.info(f"Updated {updated} case links")
    
    elif args.command == "status":
        progress = load_progress()
        print(f"Fetched cases: {len(progress.get('fetched_cases', []))}")
        print(f"Failed cases: {len(progress.get('failed_cases', []))}")
        print(f"Total fetched: {progress.get('total_fetched', 0)}")
        print(f"Last updated: {progress.get('last_updated', 'Never')}")
        
        # Scan for current missing count
        missing = scan_all_missing_cases()
        print(f"Currently missing: {len(missing)}")


if __name__ == "__main__":
    main()
