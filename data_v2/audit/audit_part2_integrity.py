"""
PART 2: Random sample integrity check + duplicate detection
"""
import os
import json
import random
import re
import sys
from collections import Counter

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
OUTPUT_DIR = os.path.join(DATA_DIR, "audit")

random.seed(42)  # Reproducible

# ===== TECHNIQUE 3: INTEGRITY CHECK =====
print("=" * 100)
print("TECHNIQUE 3: RANDOM SAMPLE INTEGRITY CHECK")
print("=" * 100)

integrity_issues = []
total_checked = 0
total_ok = 0
total_corrupt = 0
total_missing_fields = 0
total_empty_judgment = 0
total_stub = 0

for reporter in REPORTERS:
    reporter_dir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(reporter_dir):
        continue
    
    for year_name in sorted(os.listdir(reporter_dir)):
        year_path = os.path.join(reporter_dir, year_name)
        if not os.path.isdir(year_path) or not re.match(r'^\d{4}$', year_name):
            continue
        
        json_files = [f for f in os.listdir(year_path) if f.endswith('.json')]
        if not json_files:
            continue
        
        # Pick up to 3 random files
        sample = random.sample(json_files, min(3, len(json_files)))
        
        for fname in sample:
            total_checked += 1
            fpath = os.path.join(year_path, fname)
            fsize = os.path.getsize(fpath)
            
            issues = []
            
            # Check file size
            if fsize < 500:
                issues.append(f"STUB: only {fsize} bytes")
                total_stub += 1
            
            # Try to parse JSON
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except json.JSONDecodeError as e:
                issues.append(f"CORRUPT JSON: {e}")
                total_corrupt += 1
                integrity_issues.append({
                    'reporter': reporter,
                    'year': year_name,
                    'file': fname,
                    'size': fsize,
                    'issues': issues
                })
                continue
            except Exception as e:
                issues.append(f"READ ERROR: {e}")
                total_corrupt += 1
                integrity_issues.append({
                    'reporter': reporter,
                    'year': year_name,
                    'file': fname,
                    'size': fsize,
                    'issues': issues
                })
                continue
            
            # Check required fields
            has_citation = 'citation' in data and data['citation']
            has_title = ('title' in data and data['title']) or ('case_title' in data and data['case_title'])
            has_judgment = False
            
            for jfield in ['judgment', 'judgment_raw', 'content', 'text', 'body']:
                if jfield in data and data[jfield] and len(str(data[jfield])) > 50:
                    has_judgment = True
                    break
            
            if not has_citation:
                issues.append("MISSING: citation")
            if not has_title:
                issues.append("MISSING: title/case_title")
            if not has_judgment:
                issues.append("MISSING/EMPTY: judgment content")
                total_empty_judgment += 1
            
            if not has_citation or not has_title:
                total_missing_fields += 1
            
            if issues:
                integrity_issues.append({
                    'reporter': reporter,
                    'year': year_name,
                    'file': fname,
                    'size': fsize,
                    'issues': issues
                })
            else:
                total_ok += 1

print(f"\nTotal files checked: {total_checked}")
print(f"  OK: {total_ok}")
print(f"  Corrupt JSON: {total_corrupt}")
print(f"  Missing fields: {total_missing_fields}")
print(f"  Empty judgment: {total_empty_judgment}")
print(f"  Stub files (<500 bytes): {total_stub}")

if integrity_issues:
    print(f"\nISSUES FOUND ({len(integrity_issues)} files):")
    print("-" * 80)
    for item in integrity_issues[:50]:
        print(f"  {item['reporter']}/{item['year']}/{item['file']} ({item['size']} bytes): {'; '.join(item['issues'])}")
    if len(integrity_issues) > 50:
        print(f"  ... and {len(integrity_issues) - 50} more")

# Save results
with open(os.path.join(OUTPUT_DIR, "part2_integrity.json"), 'w') as f:
    json.dump({
        'total_checked': total_checked,
        'total_ok': total_ok,
        'total_corrupt': total_corrupt,
        'total_missing_fields': total_missing_fields,
        'total_empty_judgment': total_empty_judgment,
        'total_stub': total_stub,
        'issues': integrity_issues
    }, f, indent=2)

# ===== TECHNIQUE 6: DUPLICATE DETECTION =====
print("\n" + "=" * 100)
print("TECHNIQUE 6: DUPLICATE DETECTION")
print("=" * 100)

dup_citations = []
dup_sizes = []

for reporter in REPORTERS:
    reporter_dir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(reporter_dir):
        continue
    
    for year_name in sorted(os.listdir(reporter_dir)):
        year_path = os.path.join(reporter_dir, year_name)
        if not os.path.isdir(year_path) or not re.match(r'^\d{4}$', year_name):
            continue
        
        json_files = [f for f in os.listdir(year_path) if f.endswith('.json')]
        if not json_files:
            continue
        
        # Check duplicate citation numbers
        citation_nums = []
        file_sizes = {}
        for f in json_files:
            match = re.search(r'(\d+)\.json$', f)
            if match:
                citation_nums.append(int(match.group(1)))
            
            fpath = os.path.join(year_path, f)
            fsize = os.path.getsize(fpath)
            if fsize not in file_sizes:
                file_sizes[fsize] = []
            file_sizes[fsize].append(f)
        
        # Duplicate citations
        cit_counts = Counter(citation_nums)
        dups = {k: v for k, v in cit_counts.items() if v > 1}
        if dups:
            dup_citations.append({
                'reporter': reporter,
                'year': year_name,
                'duplicates': {str(k): v for k, v in dups.items()}
            })
        
        # Duplicate sizes (only flag if many files have same size, suggesting copies)
        for size, files in file_sizes.items():
            if len(files) >= 3 and size > 100:  # 3+ files with same size
                dup_sizes.append({
                    'reporter': reporter,
                    'year': year_name,
                    'size': size,
                    'count': len(files),
                    'sample_files': files[:5]
                })

print(f"\nDuplicate citation numbers found: {len(dup_citations)} reporter/year combos")
if dup_citations:
    for item in dup_citations[:30]:
        print(f"  {item['reporter']}/{item['year']}: {item['duplicates']}")
    if len(dup_citations) > 30:
        print(f"  ... and {len(dup_citations) - 30} more")

print(f"\nSuspicious identical file sizes (3+ files same size): {len(dup_sizes)} groups")
if dup_sizes:
    for item in dup_sizes[:20]:
        print(f"  {item['reporter']}/{item['year']}: {item['count']} files of {item['size']} bytes")
    if len(dup_sizes) > 20:
        print(f"  ... and {len(dup_sizes) - 20} more")

# Save
with open(os.path.join(OUTPUT_DIR, "part2_duplicates.json"), 'w') as f:
    json.dump({
        'dup_citations': dup_citations,
        'dup_sizes': dup_sizes
    }, f, indent=2)

print("\nPart 2 complete.")
