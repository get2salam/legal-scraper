"""
Rebuild all JSONL files from authoritative JSON case files on disk.
Fixes: duplicates, missing entries, stale data.

For each reporter/year combo, reads all .json files from data_v2/{reporter}/{year}/
and writes a clean JSONL with one line per case. Also rebuilds all_cases.jsonl.
"""
import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'CLD', 'YLR', 'PLC', 'GBLR']

def rebuild_all():
    start = time.time()
    total_cases = 0
    total_jsonl_files = 0
    fixed_files = 0
    all_cases_lines = []
    
    print(f"JSONL Rebuild — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Source: {DATA_DIR}")
    print("=" * 60)
    
    for reporter in REPORTERS:
        rep_dir = DATA_DIR / reporter
        if not rep_dir.is_dir():
            continue
        
        for year_dir in sorted(rep_dir.iterdir()):
            if not year_dir.is_dir() or not year_dir.name.isdigit():
                continue
            
            year = year_dir.name
            json_files = sorted(year_dir.glob('*.json'))
            
            if not json_files:
                continue
            
            # Read all JSON files
            cases = []
            errors = 0
            for jf in json_files:
                try:
                    with open(jf, 'r', encoding='utf-8', errors='replace') as f:
                        raw = f.read()
                    case_data = json.loads(raw)
                    cases.append(case_data)
                except Exception as e:
                    errors += 1
            sys.stdout.flush()
            
            # Write reporter JSONL
            jsonl_path = DATA_DIR / f"{reporter}_{year}.jsonl"
            
            # Check if rebuild needed
            old_count = 0
            if jsonl_path.exists():
                with open(jsonl_path, 'r', encoding='utf-8') as f:
                    old_count = sum(1 for line in f if line.strip())
            
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for case in cases:
                    f.write(json.dumps(case, ensure_ascii=False) + '\n')
            
            status = ""
            if old_count != len(cases):
                status = f" [FIXED: was {old_count} -> {len(cases)}]"
                fixed_files += 1
            
            if status or not jsonl_path.exists():
                print(f"  {reporter}_{year}: {len(cases)} cases{status}")
            
            all_cases_lines.extend(cases)
            total_cases += len(cases)
            total_jsonl_files += 1
            
            if errors:
                print(f"    [WARN] {errors} JSON read errors")
            
            sys.stdout.flush()
    
    # Rebuild all_cases.jsonl
    print(f"\nRebuilding all_cases.jsonl...")
    all_jsonl = DATA_DIR / "all_cases.jsonl"
    old_master = 0
    if all_jsonl.exists():
        with open(all_jsonl, 'r', encoding='utf-8') as f:
            old_master = sum(1 for line in f if line.strip())
    
    with open(all_jsonl, 'w', encoding='utf-8') as f:
        for case in all_cases_lines:
            f.write(json.dumps(case, ensure_ascii=False) + '\n')
    
    elapsed = time.time() - start
    
    print(f"\n{'=' * 60}")
    print(f"DONE in {elapsed:.1f}s")
    print(f"  JSONL files written: {total_jsonl_files}")
    print(f"  Files fixed: {fixed_files}")
    print(f"  Total cases: {total_cases}")
    print(f"  all_cases.jsonl: {old_master} -> {len(all_cases_lines)}")

if __name__ == '__main__':
    rebuild_all()
