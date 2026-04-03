#!/usr/bin/env python3
"""
Generate Original HTML Files from JSONL
========================================
Creates original HTML files from the backup JSONL data.
These are the raw HTML files as received from PLS (untouched).
"""

import json
import html
import re
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "data_v2"
BACKUP_JSONL = DATA_DIR / "backup" / "pre-clean-2024-original" / "all_cases.jsonl"


def decode_html(raw_html: str) -> str:
    """Decode JSON-escaped HTML to original format."""
    if not raw_html:
        return ""
    
    try:
        # Handle JSON string escaping
        if raw_html.startswith('"'):
            raw_html = raw_html[1:-1] if raw_html.endswith('"') else raw_html[1:]
        
        # Decode unicode escapes (\u003c -> <)
        if '\\u' in raw_html:
            raw_html = raw_html.encode().decode('unicode_escape')
        
        # Unescape HTML entities
        raw_html = html.unescape(raw_html)
        
        return raw_html
    except Exception as e:
        logger.warning(f"Error decoding HTML: {e}")
        return raw_html


def generate_originals(jsonl_path: Path = BACKUP_JSONL):
    """Generate original HTML files from JSONL backup."""
    
    if not jsonl_path.exists():
        # Try the main JSONL
        jsonl_path = DATA_DIR / "all_cases.jsonl"
        if not jsonl_path.exists():
            logger.error(f"JSONL file not found: {jsonl_path}")
            return
    
    logger.info(f"Reading from {jsonl_path}...")
    
    created = 0
    skipped = 0
    errors = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            try:
                case = json.loads(line.strip())
                citation = case.get('citation', '')
                
                if not citation:
                    skipped += 1
                    continue
                
                # Parse citation for directory structure
                parts = citation.split()
                if len(parts) < 2:
                    skipped += 1
                    continue
                
                year = parts[0]
                reporter = parts[1]
                
                # Create original directory
                original_dir = DATA_DIR / reporter / year / "original"
                original_dir.mkdir(parents=True, exist_ok=True)
                
                # Generate filename
                safe_citation = re.sub(r'[^\w\-]', '_', citation)
                html_filepath = original_dir / f"{safe_citation}.html"
                
                # Skip if already exists
                if html_filepath.exists():
                    skipped += 1
                    continue
                
                # Get and decode the original HTML
                raw_html = case.get('judgment', '') or case.get('judgment_raw', '')
                if not raw_html:
                    skipped += 1
                    continue
                
                original_html = decode_html(raw_html)
                
                # Save original HTML
                html_filepath.write_text(original_html, encoding='utf-8')
                created += 1
                
                if created % 100 == 0:
                    logger.info(f"Progress: {created} created, {skipped} skipped")
                    
            except Exception as e:
                errors += 1
                if errors < 5:
                    logger.error(f"Error on line {line_num}: {e}")
    
    logger.info(f"\nComplete: {created} created, {skipped} skipped, {errors} errors")
    
    # Show directory structure
    logger.info(f"\nOriginal HTML files saved to:")
    for reporter_dir in sorted(DATA_DIR.iterdir()):
        if reporter_dir.is_dir() and reporter_dir.name not in ['backup', 'html', 'audit', 'chromadb', 'logs']:
            for year_dir in sorted(reporter_dir.iterdir()):
                if year_dir.is_dir():
                    original_dir = year_dir / "original"
                    if original_dir.exists():
                        count = len(list(original_dir.glob("*.html")))
                        if count > 0:
                            logger.info(f"  {reporter_dir.name}/{year_dir.name}/original/ - {count} files")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate original HTML files from JSONL")
    parser.add_argument("--source", "-s", help="Path to JSONL file", 
                        default=str(BACKUP_JSONL))
    
    args = parser.parse_args()
    
    generate_originals(Path(args.source))
