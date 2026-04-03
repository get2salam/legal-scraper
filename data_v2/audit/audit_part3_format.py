"""
PART 3: Format completeness check (original HTML + readable HTML)
"""
import os
import json
import re

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
OUTPUT_DIR = os.path.join(DATA_DIR, "audit")

print("=" * 100)
print("TECHNIQUE 4: FORMAT COMPLETENESS CHECK")
print("=" * 100)

# Check original HTML folders inside reporter/year/original/
print("\n--- Original HTML folders (data_v2/REPORTER/YEAR/original/) ---")
original_results = []

for reporter in REPORTERS:
    reporter_dir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(reporter_dir):
        continue
    
    for year_name in sorted(os.listdir(reporter_dir)):
        year_path = os.path.join(reporter_dir, year_name)
        if not os.path.isdir(year_path) or not re.match(r'^\d{4}$', year_name):
            continue
        
        json_count = len([f for f in os.listdir(year_path) if f.endswith('.json')])
        
        original_path = os.path.join(year_path, "original")
        has_original = os.path.isdir(original_path)
        html_count = 0
        if has_original:
            html_count = len([f for f in os.listdir(original_path) if f.endswith('.html') or f.endswith('.htm')])
        
        original_results.append({
            'reporter': reporter,
            'year': year_name,
            'json_count': json_count,
            'has_original_dir': has_original,
            'html_count': html_count,
            'match': html_count == json_count if has_original else None
        })

# Summary
total_with_original = sum(1 for r in original_results if r['has_original_dir'])
total_without = sum(1 for r in original_results if not r['has_original_dir'])
total_matched = sum(1 for r in original_results if r['match'] == True)
total_mismatched = sum(1 for r in original_results if r['match'] == False)

print(f"\nTotal reporter/year combos: {len(original_results)}")
print(f"  With original/ dir: {total_with_original}")
print(f"  Without original/ dir: {total_without}")
print(f"  HTML count matches JSON count: {total_matched}")
print(f"  HTML count mismatches: {total_mismatched}")

if total_mismatched > 0:
    print(f"\nMismatches:")
    mismatches = [r for r in original_results if r['match'] == False]
    for r in mismatches[:30]:
        diff = r['json_count'] - r['html_count']
        print(f"  {r['reporter']}/{r['year']}: {r['json_count']} JSON, {r['html_count']} HTML (diff: {diff})")
    if len(mismatches) > 30:
        print(f"  ... and {len(mismatches) - 30} more")

# Check readable HTML (data_v2/html/REPORTER/YEAR/)
print("\n--- Readable HTML folders (data_v2/html/REPORTER/YEAR/) ---")
html_base = os.path.join(DATA_DIR, "html")
readable_results = []

if os.path.isdir(html_base):
    for reporter in REPORTERS:
        html_reporter = os.path.join(html_base, reporter)
        if not os.path.isdir(html_reporter):
            print(f"  {reporter}: NO readable HTML directory")
            continue
        
        year_dirs = [d for d in os.listdir(html_reporter) if os.path.isdir(os.path.join(html_reporter, d)) and re.match(r'^\d{4}$', d)]
        year_dirs.sort()
        
        if year_dirs:
            # Count total files
            total_html = 0
            for yd in year_dirs:
                ypath = os.path.join(html_reporter, yd)
                total_html += len([f for f in os.listdir(ypath) if f.endswith('.html') or f.endswith('.htm')])
            
            print(f"  {reporter}: {len(year_dirs)} years, {total_html} HTML files (years: {year_dirs[0]}-{year_dirs[-1]})")
            readable_results.append({
                'reporter': reporter,
                'year_count': len(year_dirs),
                'total_html': total_html,
                'year_range': f"{year_dirs[0]}-{year_dirs[-1]}"
            })
        else:
            print(f"  {reporter}: directory exists but no year folders")
else:
    print("  html/ base directory does NOT exist")

# Save results
with open(os.path.join(OUTPUT_DIR, "part3_format.json"), 'w') as f:
    json.dump({
        'original_html': {
            'total_combos': len(original_results),
            'with_original': total_with_original,
            'without_original': total_without,
            'matched': total_matched,
            'mismatched': total_mismatched,
            'details': original_results
        },
        'readable_html': readable_results
    }, f, indent=2)

print("\nPart 3 complete.")
