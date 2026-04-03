"""
Deep check on suspicious identical-size files and judgment truncation
"""
import os
import json
import hashlib

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"

# Check if SCMR 2020/2021 identical-size files have same content
print("=" * 80)
print("CHECKING IDENTICAL-SIZE FILES FOR DUPLICATE CONTENT")
print("=" * 80)

for year in ['2020', '2021']:
    year_path = os.path.join(DATA_DIR, 'SCMR', year)
    target_sizes = [64188, 64189, 64190, 64192]
    
    hashes = {}
    for f in sorted(os.listdir(year_path)):
        if not f.endswith('.json'):
            continue
        fpath = os.path.join(year_path, f)
        fsize = os.path.getsize(fpath)
        if fsize in target_sizes:
            with open(fpath, 'rb') as fh:
                content = fh.read()
            
            # Hash the judgment_raw content specifically
            data = json.loads(content)
            judgment = data.get('judgment_raw', data.get('judgment', ''))
            jhash = hashlib.md5(judgment.encode('utf-8') if isinstance(judgment, str) else judgment).hexdigest()
            
            if jhash not in hashes:
                hashes[jhash] = []
            hashes[jhash].append(f)
    
    print(f"\nSCMR/{year}:")
    for jhash, files in sorted(hashes.items(), key=lambda x: -len(x[1])):
        if len(files) > 1:
            print(f"  Hash {jhash}: {len(files)} files share IDENTICAL judgment content")
            print(f"    First 5: {files[:5]}")
            # Show a snippet from the first file
            fpath = os.path.join(year_path, files[0])
            with open(fpath, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            jr = data.get('judgment_raw', '')
            print(f"    Judgment starts with: {jr[:200]}...")
            print(f"    Judgment ends with: ...{jr[-200:]}")

# Check for truncated judgments across all reporters
print("\n" + "=" * 80)
print("CHECKING FOR TRUNCATED/STUB JUDGMENTS")
print("=" * 80)

REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]

truncated_counts = {}
empty_counts = {}

for reporter in REPORTERS:
    reporter_dir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(reporter_dir):
        continue
    
    truncated_counts[reporter] = 0
    empty_counts[reporter] = 0
    
    for year_name in sorted(os.listdir(reporter_dir)):
        year_path = os.path.join(reporter_dir, year_name)
        if not os.path.isdir(year_path) or not year_name.isdigit():
            continue
        
        for fname in os.listdir(year_path):
            if not fname.endswith('.json'):
                continue
            fpath = os.path.join(year_path, fname)
            fsize = os.path.getsize(fpath)
            
            if fsize < 500:
                empty_counts[reporter] += 1

print("\nFiles < 500 bytes per reporter:")
for r in REPORTERS:
    if r in empty_counts:
        print(f"  {r}: {empty_counts[r]}")

# Check PLD 1987 specifically (known missing year)
print("\n" + "=" * 80)
print("KNOWN PROBLEM YEARS - DETAILED CHECK")
print("=" * 80)

problem_years = [
    ('PLD', '1987'), ('PCrLJ', '1972'), ('PCrLJ', '1978'),
    ('CLC', '1987'), ('MLD', '1987'),
    ('PTD', '1961'), ('PTD', '1962'), ('PTD', '1967'),
]

for reporter, year in problem_years:
    year_path = os.path.join(DATA_DIR, reporter, year)
    if os.path.isdir(year_path):
        files = [f for f in os.listdir(year_path) if f.endswith('.json')]
        print(f"  {reporter}/{year}: {len(files)} files")
        if files:
            for f in sorted(files)[:5]:
                print(f"    {f} ({os.path.getsize(os.path.join(year_path, f))} bytes)")
    else:
        print(f"  {reporter}/{year}: DIRECTORY DOES NOT EXIST")

# Check years with 0 files for each reporter
print("\n" + "=" * 80)
print("MISSING YEARS (gaps in continuous ranges)")
print("=" * 80)

expected_ranges = {
    'PLD': (1947, 2026),
    'SCMR': (1968, 2026),
    'PCrLJ': (1968, 2025),
    'MLD': (1984, 2025),
    'CLC': (1979, 2025),
    'YLR': (1999, 2025),
    'PTD': (1960, 2025),
    'PLC': (1970, 2025),
    'CLD': (2002, 2025),
    'GBLR': (2014, 2016),
}

for reporter, (start, end) in expected_ranges.items():
    reporter_dir = os.path.join(DATA_DIR, reporter)
    existing_years = set()
    if os.path.isdir(reporter_dir):
        for item in os.listdir(reporter_dir):
            if item.isdigit() and os.path.isdir(os.path.join(reporter_dir, item)):
                existing_years.add(int(item))
    
    missing = []
    for y in range(start, end + 1):
        if y not in existing_years:
            missing.append(y)
    
    if missing:
        print(f"  {reporter}: Missing years: {missing}")
    else:
        print(f"  {reporter}: All years {start}-{end} present")
