"""
Fast JSONL rebuilder — reads JSON files as raw text (no parsing), writes directly.
Much faster than loading/serializing 24K+ JSON objects.
"""
import os, sys, time, json
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'CLD', 'YLR', 'PLC', 'GBLR']

start = time.time()
total_cases = 0
fixed = 0
all_lines = []

print(f"Fast JSONL Rebuild - {datetime.now().strftime('%H:%M:%S')}")
print("=" * 50)

for rep in REPORTERS:
    rep_dir = DATA_DIR / rep
    if not rep_dir.is_dir():
        continue
    
    rep_count = 0
    for year_dir in sorted(rep_dir.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        
        year = year_dir.name
        json_files = sorted(year_dir.glob('*.json'))
        if not json_files:
            continue
        
        # Read JSON files as raw text — compact each to single line
        lines = []
        for jf in json_files:
            try:
                raw = jf.read_text(encoding='utf-8', errors='replace').strip()
                # Ensure it's valid JSON by parsing, then compact to one line
                obj = json.loads(raw)
                lines.append(json.dumps(obj, ensure_ascii=False, separators=(',', ':')))
            except:
                pass
        
        jsonl_path = DATA_DIR / f"{rep}_{year}.jsonl"
        
        # Check if needs fix
        old_count = 0
        if jsonl_path.exists():
            with open(jsonl_path, 'r', encoding='utf-8') as f:
                old_count = sum(1 for l in f if l.strip())
        
        needs_fix = old_count != len(lines)
        
        if needs_fix:
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for line in lines:
                    f.write(line + '\n')
            fixed += 1
            print(f"  FIXED {rep}_{year}: {old_count} -> {len(lines)}")
            sys.stdout.flush()
        
        all_lines.extend(lines)
        total_cases += len(lines)
        rep_count += len(lines)
    
    print(f"{rep}: {rep_count} cases")
    sys.stdout.flush()

# Rebuild master
print(f"\nRebuilding all_cases.jsonl ({total_cases} cases)...")
sys.stdout.flush()
master = DATA_DIR / "all_cases.jsonl"
with open(master, 'w', encoding='utf-8') as f:
    for line in all_lines:
        f.write(line + '\n')

elapsed = time.time() - start
print(f"\nDONE in {elapsed:.0f}s — {total_cases} cases, {fixed} files fixed")
