import json, sys, urllib.request, time
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

leg_dir = Path('data_v2/legislation')
stubs = []; pls_urls = 0; other_urls = 0; no_url = 0
for f in leg_dir.glob('*.json'):
    try:
        raw = json.load(open(f, encoding='utf-8'))
        d = raw if isinstance(raw, dict) else (raw[0] if isinstance(raw, list) and raw else {})
        body = d.get('body','') or d.get('text','') or d.get('content','') or ''
        if '[Content not available' in body or len(body.strip()) < 100:
            url = d.get('source_url','') or d.get('url','') or ''
            stubs.append(url)
            if 'pakistanlawsite' in url: pls_urls += 1
            elif url: other_urls += 1
            else: no_url += 1
    except: pass

print(f'Total stubs: {len(stubs)}')
print(f'  PLS URLs only: {pls_urls}')
print(f'  Other URLs: {other_urls}')
print(f'  No URL at all: {no_url}')

# Quick Wayback check
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/*&output=json&limit=3&fl=timestamp,original&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=12)
    data = json.loads(resp.read())
    print(f'\nPakistanCode Wayback: {len(data)-1} snapshots')
    for row in data[1:]: print(f'  {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'PakistanCode Wayback: {e}')
