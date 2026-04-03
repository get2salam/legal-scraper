#!/usr/bin/env python3
"""
Migration Script: Add judgment_raw to existing JSON files
=========================================================
Reads original HTML files and adds them as judgment_raw field to JSON.
Does NOT modify original HTML files.

Usage:
    python migrate_judgment_raw.py --year 2024
    python migrate_judgment_raw.py --year 2025
    python migrate_judgment_raw.py --all
"""

import json
import argparse
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).parent / "data_v2"
REPORTERS = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD"]

def migrate_year(year: int) -> dict:
    """Add judgment_raw to all cases for a year."""
    stats = {"updated": 0, "skipped": 0, "missing_html": 0, "errors": 0}
    
    for reporter in REPORTERS:
        json_dir = DATA_DIR / reporter / str(year)
        html_dir = json_dir / "original"
        
        if not json_dir.exists():
            continue
            
        for json_file in json_dir.glob("*.json"):
            try:
                # Read JSON
                data = json.loads(json_file.read_text(encoding='utf-8'))
                
                # Skip if already has judgment_raw
                if data.get("judgment_raw") and len(data["judgment_raw"]) > 100:
                    stats["skipped"] += 1
                    continue
                
                # Find corresponding HTML file
                html_file = html_dir / f"{json_file.stem}.html"
                
                if not html_file.exists():
                    stats["missing_html"] += 1
                    continue
                
                # Read HTML and add to JSON
                html_content = html_file.read_text(encoding='utf-8', errors='ignore')
                data["judgment_raw"] = html_content
                
                # Save updated JSON
                json_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
                stats["updated"] += 1
                
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
                stats["errors"] += 1
    
    return stats

def main():
    parser = argparse.ArgumentParser(description="Add judgment_raw from HTML files")
    parser.add_argument("--year", type=int, help="Year to migrate")
    parser.add_argument("--all", action="store_true", help="Migrate all years")
    args = parser.parse_args()
    
    years = []
    if args.all:
        # Find all years
        for reporter_dir in DATA_DIR.iterdir():
            if reporter_dir.is_dir() and reporter_dir.name in REPORTERS:
                for year_dir in reporter_dir.iterdir():
                    if year_dir.is_dir() and year_dir.name.isdigit():
                        years.append(int(year_dir.name))
        years = sorted(set(years))
    elif args.year:
        years = [args.year]
    else:
        print("Specify --year or --all")
        return
    
    print(f"Migrating years: {years}")
    print("=" * 50)
    
    total_stats = {"updated": 0, "skipped": 0, "missing_html": 0, "errors": 0}
    
    for year in years:
        print(f"\n{year}:")
        stats = migrate_year(year)
        print(f"  Updated: {stats['updated']}")
        print(f"  Skipped (already has): {stats['skipped']}")
        print(f"  Missing HTML: {stats['missing_html']}")
        print(f"  Errors: {stats['errors']}")
        
        for k in total_stats:
            total_stats[k] += stats[k]
    
    print("\n" + "=" * 50)
    print("TOTAL:")
    print(f"  Updated: {total_stats['updated']}")
    print(f"  Skipped: {total_stats['skipped']}")
    print(f"  Missing HTML: {total_stats['missing_html']}")
    print(f"  Errors: {total_stats['errors']}")

if __name__ == "__main__":
    main()
