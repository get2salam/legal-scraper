"""Find and log all original HTML files that are PLS error pages or corrupt — for re-fetching."""
import glob, os, json

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)))
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

error_pages = []
corrupt = []

for r in REPORTERS:
    for f in glob.glob(os.path.join(DATA_DIR, r, '*', 'original', '*.html')):
        with open(f, 'r', encoding='utf-8', errors='replace') as fh:
            start = fh.read(200)
        
        stripped = start.strip()
        # Extract citation from path: data_v2/REPORTER/YEAR/original/CITATION.html
        parts = f.replace('\\', '/').split('/')
        idx = parts.index('original')
        reporter = parts[idx - 2]
        year = parts[idx - 1]
        citation_safe = os.path.splitext(parts[idx + 1])[0]
        
        if stripped.startswith('Pakistan Law Site'):
            error_pages.append({'reporter': reporter, 'year': year, 'citation': citation_safe, 'type': 'error_page', 'path': f})
        elif stripped.startswith('"') and '<' not in stripped[:200]:
            corrupt.append({'reporter': reporter, 'year': year, 'citation': citation_safe, 'type': 'corrupt', 'path': f})

all_failed = error_pages + corrupt
print(f"Error pages: {len(error_pages)}")
print(f"Corrupt: {len(corrupt)}")
print(f"Total to re-fetch: {len(all_failed)}")

# Save list
output = os.path.join(os.path.dirname(__file__), 'refetch_list.json')
with open(output, 'w', encoding='utf-8') as f:
    json.dump(all_failed, f, indent=2, ensure_ascii=False)
print(f"Saved to {output}")

# Summary by reporter/year
from collections import Counter
by_reporter = Counter(e['reporter'] for e in all_failed)
print("\nBy reporter:")
for r, c in by_reporter.most_common():
    print(f"  {r}: {c}")
