#!/usr/bin/env python3
"""
Fetch Citation Cases - Fetches cases from the citation pipeline queue
======================================================================
Reads cases_to_fetch.json and fetches them using the PLS scraper.

Usage:
    python fetch_citation_cases.py              # Fetch all pending
    python fetch_citation_cases.py --limit 50   # Fetch max 50 cases
    python fetch_citation_cases.py --year 2020  # Only 2020 cases
"""

import json
import os
import re
import sys
import time
import random
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Set

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from pls_scraper_v2 import PLSScraperV2

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2"
PIPELINE_DIR = DATA_DIR / "pipeline"
QUEUE_FILE = PIPELINE_DIR / "cases_to_fetch.json"
PROGRESS_FILE = PIPELINE_DIR / "fetch_progress.json"

# JSONL dedup cache (module-level, populated lazily)
_jsonl_sets: Dict[str, Set[str]] = {}


def load_queue():
    """Load the fetch queue."""
    if not QUEUE_FILE.exists():
        logger.error(f"Queue file not found: {QUEUE_FILE}")
        logger.info("Run: python citation_pipeline.py run")
        return []
    return json.loads(QUEUE_FILE.read_text(encoding='utf-8'))


def load_progress():
    """Load fetch progress."""
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text(encoding='utf-8'))
    return {'fetched': [], 'failed': [], 'started': None}


def save_progress(progress):
    """Save fetch progress."""
    progress['updated'] = datetime.now().isoformat()
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding='utf-8')


def case_exists(year: int, reporter: str, page: int) -> bool:
    """Check if case already exists."""
    citation = f"{year}_{reporter}_{page}"
    case_path = DATA_DIR / reporter / str(year) / f"{citation}.json"
    return case_path.exists()


def fetch_single_case(scraper: PLSScraperV2, year: int, reporter: str, page: int) -> bool:
    """Fetch a single case and save in all 4 formats."""
    citation = f"{year} {reporter} {page}"
    safe_citation = f"{year}_{reporter}_{page}"
    
    try:
        # Search for the case
        url = f"https://www.pakistanlawsite.com/Login/SearchCaseLaw?Year={year}&Series={reporter}&PageNo={page}"
        
        # Use scraper's session
        if not scraper.session:
            scraper._create_session()
            scraper._login()
        
        response = scraper.session.get(url, timeout=30)
        
        if response.status_code != 200:
            logger.warning(f"Failed to fetch {citation}: HTTP {response.status_code}")
            return False
        
        # Parse response (should be judgment content or redirect)
        content = response.text
        
        if len(content) < 100 or 'error' in content.lower():
            logger.warning(f"Empty or error response for {citation}")
            return False
        
        # Create JSON structure
        case_data = {
            "citation": citation,
            "year": year,
            "reporter": reporter,
            "page": page,
            "judgment_raw": content,
            "fetched_at": datetime.now().isoformat(),
            "source": "citation_pipeline"
        }

        # 1. JSON
        case_dir = DATA_DIR / reporter / str(year)
        case_dir.mkdir(parents=True, exist_ok=True)
        case_file = case_dir / f"{safe_citation}.json"
        case_file.write_text(json.dumps(case_data, indent=2, ensure_ascii=False), encoding='utf-8')

        # 2. Original HTML
        original_dir = case_dir / "original"
        original_dir.mkdir(exist_ok=True)
        (original_dir / f"{safe_citation}.html").write_text(content, encoding='utf-8')

        # 3. Readable HTML
        html_dir = DATA_DIR / "html" / reporter / str(year)
        html_dir.mkdir(parents=True, exist_ok=True)
        readable = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>{citation}</title>
<style>body{{font-family:Georgia,serif;max-width:800px;margin:40px auto;padding:20px;line-height:1.6;color:#333}}
h1{{font-size:1.4rem;border-bottom:2px solid #333;padding-bottom:10px}}
.meta{{background:#f5f5f5;padding:15px;border-radius:5px;margin:15px 0}}
.headnotes{{border-left:3px solid #666;padding-left:15px;margin:20px 0;color:#555}}</style></head>
<body><h1>{citation}</h1>
<div class="meta"><b>Reporter:</b> {reporter}<br><b>Year:</b> {year}<br>
<b>Page:</b> {page}</div>
<div class="judgment">{content}</div></body></html>"""
        (html_dir / f"{safe_citation}.html").write_text(readable, encoding='utf-8')

        # 4. JSONL (reporter + master) with dedup
        jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
        jsonl_key = str(jsonl_path)
        if jsonl_key not in _jsonl_sets:
            _jsonl_sets[jsonl_key] = set()
            if jsonl_path.exists():
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        m = re.search(r'"citation":\s*"([^"]+)"', line)
                        if m:
                            _jsonl_sets[jsonl_key].add(m.group(1))

        if citation not in _jsonl_sets[jsonl_key]:
            with open(jsonl_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(case_data, ensure_ascii=False) + '\n')
            _jsonl_sets[jsonl_key].add(citation)

        # Master JSONL (append without scanning — too large)
        master = DATA_DIR / "all_cases.jsonl"
        with open(master, 'a', encoding='utf-8') as f:
            f.write(json.dumps(case_data, ensure_ascii=False) + '\n')

        logger.info(f"Saved (4 formats): {citation}")
        return True
        
    except Exception as e:
        logger.error(f"Error fetching {citation}: {e}")
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Fetch cases from citation queue')
    parser.add_argument('--limit', type=int, default=0, help='Max cases to fetch (0=all)')
    parser.add_argument('--year', type=int, help='Only fetch cases from this year')
    args = parser.parse_args()
    
    # Load queue
    queue = load_queue()
    if not queue:
        return
    
    logger.info(f"Loaded {len(queue)} cases from queue")
    
    # Filter if needed
    if args.year:
        queue = [c for c in queue if c['year'] == args.year]
        logger.info(f"Filtered to {len(queue)} cases for year {args.year}")
    
    # Load progress
    progress = load_progress()
    if not progress['started']:
        progress['started'] = datetime.now().isoformat()
    
    # Filter out already fetched/failed
    already_done = set(progress['fetched'] + progress['failed'])
    queue = [c for c in queue if c['citation'] not in already_done]
    
    # Also filter out cases that already exist
    queue = [c for c in queue if not case_exists(c['year'], c['reporter'], c['page'])]
    
    logger.info(f"Need to fetch: {len(queue)} cases")
    
    if not queue:
        logger.info("All cases already fetched!")
        return
    
    # Apply limit
    if args.limit > 0:
        queue = queue[:args.limit]
        logger.info(f"Limited to {len(queue)} cases")
    
    # Initialize scraper
    scraper = PLSScraperV2(ignore_hours=True)
    
    fetched = 0
    failed = 0
    
    try:
        for i, case in enumerate(queue):
            year = case['year']
            reporter = case['reporter']
            page = case['page']
            citation = case['citation']
            
            logger.info(f"[{i+1}/{len(queue)}] Fetching: {citation}")
            
            # Check if already exists (double check)
            if case_exists(year, reporter, page):
                logger.info(f"Already have: {citation}")
                progress['fetched'].append(citation)
                continue
            
            # Fetch
            success = fetch_single_case(scraper, year, reporter, page)
            
            if success:
                fetched += 1
                progress['fetched'].append(citation)
            else:
                failed += 1
                progress['failed'].append(citation)
            
            # Save progress periodically
            if (i + 1) % 10 == 0:
                save_progress(progress)
            
            # Human-like delay
            delay = random.uniform(2, 5)
            time.sleep(delay)
    
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
    finally:
        save_progress(progress)
    
    # Summary
    print(f"\n{'='*50}")
    print(f"FETCH COMPLETE")
    print(f"{'='*50}")
    print(f"Fetched: {fetched}")
    print(f"Failed: {failed}")
    print(f"Total progress: {len(progress['fetched'])} / {len(load_queue())}")


if __name__ == '__main__':
    main()
