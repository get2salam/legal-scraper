"""
PART 1: File counts per reporter per year + citation sequence analysis
"""
import os
import json
import re
import sys

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]
OUTPUT_DIR = os.path.join(DATA_DIR, "audit")

results = {}  # {reporter: {year: {count, max_citation, min_citation, citations}}}

for reporter in REPORTERS:
    reporter_dir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(reporter_dir):
        print(f"WARNING: Reporter directory not found: {reporter}")
        continue
    
    results[reporter] = {}
    year_dirs = []
    for item in os.listdir(reporter_dir):
        item_path = os.path.join(reporter_dir, item)
        if os.path.isdir(item_path) and re.match(r'^\d{4}$', item):
            year_dirs.append(item)
    
    year_dirs.sort()
    
    for year in year_dirs:
        year_path = os.path.join(reporter_dir, year)
        json_files = [f for f in os.listdir(year_path) if f.endswith('.json')]
        
        citations = []
        for f in json_files:
            # Parse citation number from filename
            # Format: "YYYY REPORTER NNNN.json" or variations
            match = re.search(r'(\d+)\.json$', f)
            if match:
                citations.append(int(match.group(1)))
        
        results[reporter][year] = {
            'count': len(json_files),
            'max_citation': max(citations) if citations else 0,
            'min_citation': min(citations) if citations else 0,
            'citation_count': len(citations),
            'citations': sorted(citations)
        }

# Save raw results
output_file = os.path.join(OUTPUT_DIR, "part1_counts.json")
# Convert for JSON serialization (remove full citation lists for size)
summary = {}
for reporter in results:
    summary[reporter] = {}
    for year in results[reporter]:
        d = results[reporter][year]
        summary[reporter][year] = {
            'count': d['count'],
            'max_citation': d['max_citation'],
            'min_citation': d['min_citation'],
            'gap': d['max_citation'] - d['count'] if d['max_citation'] > 0 else 0,
            'gap_pct': round((d['max_citation'] - d['count']) / d['max_citation'] * 100, 1) if d['max_citation'] > 0 else 0
        }

with open(output_file, 'w') as f:
    json.dump(summary, f, indent=2)

# Print summary table
print("=" * 120)
print("PART 1: FILE COUNTS & CITATION GAPS PER REPORTER PER YEAR")
print("=" * 120)

# Grand totals
grand_total = 0
reporter_totals = {}
year_totals = {}

for reporter in REPORTERS:
    if reporter not in results:
        continue
    reporter_totals[reporter] = 0
    for year in sorted(results[reporter].keys()):
        count = results[reporter][year]['count']
        reporter_totals[reporter] += count
        grand_total += count
        y = int(year)
        year_totals[y] = year_totals.get(y, 0) + count

print(f"\nGRAND TOTAL: {grand_total:,} cases across all reporters")
print()

# Reporter totals
print("REPORTER TOTALS:")
print("-" * 40)
for reporter in REPORTERS:
    if reporter in reporter_totals:
        years = sorted(results[reporter].keys())
        print(f"  {reporter:8s}: {reporter_totals[reporter]:>7,} cases  (years: {years[0]}-{years[-1]}, {len(years)} years)")

# Citation gap analysis
print("\n" + "=" * 120)
print("CITATION GAP ANALYSIS (years where max_citation - file_count > 10% of max_citation)")
print("=" * 120)

gap_issues = []
for reporter in REPORTERS:
    if reporter not in results:
        continue
    for year in sorted(results[reporter].keys()):
        d = results[reporter][year]
        max_cit = d['max_citation']
        count = d['count']
        if max_cit > 0 and (max_cit - count) > 0.1 * max_cit:
            gap_pct = round((max_cit - count) / max_cit * 100, 1)
            gap_issues.append((reporter, year, count, max_cit, max_cit - count, gap_pct))

gap_issues.sort(key=lambda x: -x[5])  # Sort by gap percentage descending

print(f"\n{'Reporter':<10} {'Year':<6} {'Files':<8} {'MaxCit':<8} {'Gap':<8} {'Gap%':<8}")
print("-" * 50)
for reporter, year, count, max_cit, gap, gap_pct in gap_issues[:100]:
    print(f"{reporter:<10} {year:<6} {count:<8} {max_cit:<8} {gap:<8} {gap_pct:<8.1f}%")

if len(gap_issues) > 100:
    print(f"  ... and {len(gap_issues) - 100} more entries")

print(f"\nTotal reporter/year combos with >10% citation gaps: {len(gap_issues)}")

# Year-over-year totals for consistency check
print("\n" + "=" * 120)
print("YEAR-OVER-YEAR TOTALS (1947-2026)")
print("=" * 120)

all_years = sorted(year_totals.keys())
prev_total = None
anomalies = []

print(f"\n{'Year':<6} {'Total':<8} {'Change':<10} {'Flag':<20} ", end="")
for r in REPORTERS:
    print(f"{r:<7}", end="")
print()
print("-" * 130)

for y in range(min(all_years), max(all_years) + 1):
    total = year_totals.get(y, 0)
    if total == 0 and y < 1947:
        continue
    
    change = ""
    flag = ""
    if prev_total is not None and prev_total > 0:
        pct_change = (total - prev_total) / prev_total * 100
        change = f"{pct_change:+.0f}%"
        if total < prev_total * 0.5 and prev_total > 50:
            flag = "!! >50% DROP"
            anomalies.append((y, total, prev_total, pct_change))
        elif total < prev_total * 0.7 and prev_total > 50:
            flag = "!! >30% DROP"
            anomalies.append((y, total, prev_total, pct_change))
    
    print(f"{y:<6} {total:<8} {change:<10} {flag:<20} ", end="")
    for r in REPORTERS:
        if r in results and str(y) in results[r]:
            print(f"{results[r][str(y)]['count']:<7}", end="")
        else:
            print(f"{'--':<7}", end="")
    print()
    
    prev_total = total

# Save year totals
with open(os.path.join(OUTPUT_DIR, "part1_year_totals.json"), 'w') as f:
    json.dump(year_totals, f, indent=2)

# Save anomalies
with open(os.path.join(OUTPUT_DIR, "part1_anomalies.json"), 'w') as f:
    json.dump(anomalies, f, indent=2)

# Save gap issues  
with open(os.path.join(OUTPUT_DIR, "part1_gap_issues.json"), 'w') as f:
    json.dump(gap_issues, f, indent=2)

print(f"\nYear-over-year anomalies (>30% drops): {len(anomalies)}")
print("\nPart 1 complete. Results saved to audit/part1_*.json")
