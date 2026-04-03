import json
from pathlib import Path

p = json.load(open(r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\legislation\progress.json', encoding='utf-8'))
b_names = [n for n in p.get('statutes_scraped', []) if n and n[0].upper() == 'B']
b_files = len(list(Path(r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\legislation\B').glob('*.json')))

completed = p.get("completed_alphabets", [])
print(f"B in completed: {'B' in completed}")
print(f"B names in statutes_scraped: {len(b_names)}")
print(f"B files on disk: {b_files}")
print(f"Difference: {len(b_names) - b_files} (statutes with no sections on PLS)")
print(f"Last updated: {p.get('last_updated', '?')}")

# Check latest B file
import os
b_dir = Path(r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\legislation\B')
latest = sorted(b_dir.glob('*.json'), key=lambda f: f.stat().st_mtime, reverse=True)[:3]
print("\nLatest B files:")
for f in latest:
    from datetime import datetime
    mtime = datetime.fromtimestamp(f.stat().st_mtime)
    print(f"  {f.stem[:60]} — {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
