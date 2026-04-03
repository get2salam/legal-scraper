import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Look at a small stub file
small_stubs = []
leg_dir = Path('data_v2/legislation')

for letter_dir in sorted(leg_dir.iterdir()):
    if not letter_dir.is_dir() or letter_dir.name in ['audit', 'html', 'original']:
        continue
    for f in sorted(letter_dir.glob('*.json')):
        try:
            d = json.load(open(f, encoding='utf-8'))
            if not isinstance(d, dict):
                continue
            body = d.get('body','') or ''
            if '[Content not available' in body or len(body.strip()) < 100:
                small_stubs.append({'f': str(f), 'size': f.stat().st_size, 
                                    'title': d.get('title',''),
                                    'year': d.get('year',''),
                                    'url': str(d.get('source_url','') or d.get('url','')),
                                    'body_preview': body[:100]})
        except Exception as e:
            pass
        if len(small_stubs) >= 200:
            break
    if len(small_stubs) >= 200:
        break

print(f'Found {len(small_stubs)} stubs')
print()
# Show 10 smallest (most likely pure stubs)
sorted_stubs = sorted(small_stubs, key=lambda x: x['size'])
for s in sorted_stubs[:10]:
    print(f'  Size:{s["size"]:6d} | {s["title"][:60]} ({s["year"]})')
    print(f'         url: {s["url"][:80]}')
    print(f'         body: {s["body_preview"][:80]}')
    print()
