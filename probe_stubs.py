import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
leg_dir = Path('data_v2/legislation')
stubs = []
all_files = list(leg_dir.glob('*.json'))
print(f'Total files: {len(all_files)}')
for f in all_files[:2000]:
    try:
        d = json.load(open(f, encoding='utf-8'))
        body = d.get('body','') or d.get('text','') or d.get('content','')
        if not body or len(body.strip()) < 100:
            stubs.append({'file': f.name, 'title': d.get('title',''), 'url': d.get('source_url','') or d.get('url',''), 'year': d.get('year','')})
    except:
        pass
print(f'Stubs found (in first 2000): {len(stubs)}')
for s in stubs[:10]:
    title = s['title'][:60]
    url = s['url'][:80]
    year = s['year']
    print(f'  {title} | URL: {url} | Year: {year}')
