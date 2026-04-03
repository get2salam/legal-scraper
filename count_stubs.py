import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

leg_dir = Path('data_v2/legislation')
stubs = []
total = 0

for f in leg_dir.rglob('*.json'):
    try:
        d = json.load(open(f, encoding='utf-8'))
        total += 1
        full_text = d.get('full_text','') or d.get('body','') or d.get('text','') or d.get('content','')
        if not full_text or len(str(full_text).strip()) < 100:
            src_url = d.get('source_url','') or d.get('url','')
            stubs.append({
                'file': str(f.relative_to(leg_dir)),
                'title': d.get('title',''),
                'url': src_url,
                'year': d.get('year','') or d.get('enactment_date','')
            })
    except Exception as e:
        pass

print(f'Total files: {total}')
print(f'Stubs (no full_text): {len(stubs)}')
print()
print('Sample stubs:')
for s in stubs[:10]:
    print(f'  File: {s["file"]}')
    print(f'  Title: {s["title"][:60]}')
    print(f'  URL: {s["url"][:80]}')
    print(f'  Year: {s["year"]}')
    print()
