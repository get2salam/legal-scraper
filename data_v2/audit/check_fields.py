"""
Check what fields actually exist in JSON files across different eras
"""
import os
import json
import random
from collections import Counter

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]

random.seed(123)

# Sample files from different eras
eras = {
    'early': ['1950', '1955', '1960', '1965'],
    'mid': ['1975', '1980', '1985', '1990'],
    'late': ['2000', '2005', '2010', '2015'],
    'recent': ['2020', '2022', '2024', '2025']
}

all_fields = Counter()
era_fields = {}

for era_name, years in eras.items():
    era_fields[era_name] = Counter()
    for reporter in REPORTERS:
        for year in years:
            year_path = os.path.join(DATA_DIR, reporter, year)
            if not os.path.isdir(year_path):
                continue
            json_files = [f for f in os.listdir(year_path) if f.endswith('.json')]
            if not json_files:
                continue
            sample = random.sample(json_files, min(2, len(json_files)))
            for fname in sample:
                fpath = os.path.join(year_path, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    for key in data.keys():
                        all_fields[key] += 1
                        era_fields[era_name][key] += 1
                except:
                    pass

print("ALL FIELDS FOUND (across all samples):")
print("-" * 50)
for field, count in all_fields.most_common():
    print(f"  {field:<30} {count:>5} occurrences")

print("\nFIELDS BY ERA:")
for era_name in ['early', 'mid', 'late', 'recent']:
    print(f"\n  {era_name.upper()} ({eras[era_name]}):")
    for field, count in era_fields[era_name].most_common():
        print(f"    {field:<30} {count:>5}")

# Show a sample file from each era
print("\n\nSAMPLE FILE STRUCTURES:")
print("=" * 80)
for reporter in ['SCMR', 'PLD']:
    for year in ['1970', '1990', '2010', '2025']:
        year_path = os.path.join(DATA_DIR, reporter, year)
        if not os.path.isdir(year_path):
            continue
        json_files = [f for f in os.listdir(year_path) if f.endswith('.json')]
        if not json_files:
            continue
        fpath = os.path.join(year_path, json_files[0])
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"\n{reporter}/{year}/{json_files[0]}:")
            for key in data:
                val = data[key]
                if isinstance(val, str):
                    preview = val[:100] + "..." if len(val) > 100 else val
                else:
                    preview = str(val)[:100]
                print(f"  {key}: {preview}")
        except:
            pass

# Check the suspicious identical-size files
print("\n\nSUSPICIOUS IDENTICAL SIZE FILES (SCMR 2020-2021, ~64189 bytes):")
print("=" * 80)
for year in ['2020', '2021']:
    year_path = os.path.join(DATA_DIR, 'SCMR', year)
    target_sizes = [64188, 64189, 64190, 64192]
    matches = []
    for f in os.listdir(year_path):
        if f.endswith('.json'):
            fsize = os.path.getsize(os.path.join(year_path, f))
            if fsize in target_sizes:
                matches.append((f, fsize))
    
    print(f"\nSCMR/{year}: {len(matches)} files with suspicious sizes")
    if matches:
        # Check content of first few
        for fname, fsize in matches[:3]:
            fpath = os.path.join(year_path, fname)
            with open(fpath, 'r', encoding='utf-8') as fh:
                data = json.load(fh)
            citation = data.get('citation', 'N/A')
            judgment_len = len(data.get('judgment', data.get('judgment_raw', '')))
            print(f"  {fname} ({fsize}b): citation={citation}, judgment_len={judgment_len}")
