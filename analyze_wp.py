"""
Analyze the W&P data more carefully.
The words_phrases.json has 2592 entries but 'phrase' field is a number (1-2592).
The actual phrase names are in 'snippet' field.
words_phrases_all.json has 96 entries with proper casetypeids and names.

Let's understand the full picture.
"""
import json, sys
from pathlib import Path
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

base = Path('data_v2/pls_extras')

# The 2592 entry file - analyze structure
with open(base/'words_phrases'/'words_phrases.json', encoding='utf-8') as f:
    wp_data = json.load(f)

print(f"words_phrases.json: {len(wp_data)} entries")
print("Sample entries:")
for item in wp_data[:5]:
    print(f"  {item}")

# Check if 'phrase' field is just a sequential number
phrases_are_numbers = all(item['phrase'].isdigit() for item in wp_data)
print(f"\nAll 'phrase' values are numbers: {phrases_are_numbers}")

# The 'snippet' field has the actual phrase name
snippets = [item['snippet'] for item in wp_data]
print(f"Unique non-empty snippets: {len([s for s in snippets if s])}")
print(f"Non-empty case_ids: {len([item for item in wp_data if item.get('case_id')])}")
print(f"Non-empty citations: {len([item for item in wp_data if item.get('citation')])}")

# Sample of actual phrase names (snippet field)
print("\nActual phrase names (snippet):")
for item in wp_data[:20]:
    print(f"  [{item['phrase']}] {item['snippet']}")

# Words_phrases_all.json - already confirmed 96 entries with casetypeids
with open(base/'words_phrases'/'words_phrases_all.json', encoding='utf-8') as f:
    wp_all = json.load(f)

print(f"\nwords_phrases_all.json: {len(wp_all)} entries with casetypeids")

# Check if snippet names match wp_all names
wp_all_names = {item['name'] for item in wp_all}
matching = [item for item in wp_data if item['snippet'] in wp_all_names]
print(f"Matching by name: {len(matching)}")

# Create a merged dataset: use snippet as phrase name, add casetypeid from wp_all
name_to_cid = {item['name']: item['casetypeid'] for item in wp_all}
print("\nBuilding merged dataset...")
merged = []
for item in wp_data:
    name = item['snippet']
    cid = name_to_cid.get(name, '')
    merged.append({
        'phrase': name,
        'casetypeid': cid,
        'citation': item.get('citation', ''),
    })

print(f"Merged: {len(merged)} total, {len([m for m in merged if m['casetypeid']])} with casetypeid")

# Save merged
out = base/'words_phrases'/'words_phrases_merged.json'
with open(out, 'w', encoding='utf-8') as f:
    json.dump(merged, f, ensure_ascii=False, indent=2)
print(f"Saved to {out}")

# Show distribution: how many phrase names appear multiple times?
from collections import Counter
name_counts = Counter(item['snippet'] for item in wp_data if item['snippet'])
repeated = [(name, count) for name, count in name_counts.most_common() if count > 1]
print(f"\nRepeated phrase names: {len(repeated)}")
for name, count in repeated[:10]:
    print(f"  {count}x: {name}")
