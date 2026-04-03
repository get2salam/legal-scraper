import json, sys, urllib.request, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Try fetching a known pakistancode.gov.pk URL from Wayback
# First, get a list of URLs from Wayback CDX
api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/*&output=json&limit=20&fl=timestamp,original,statuscode&filter=statuscode:200'
resp = urllib.request.urlopen(api, timeout=30)
data = json.loads(resp.read())
print(f'PakistanCode English snapshots: {len(data)-1}')
for row in data[1:]:
    print(f'  {row[0]}: {row[1][:100]}')

time.sleep(2)

# Try one specific archived page
if len(data) > 1:
    snap = data[1]
    ts = snap[0]
    orig = snap[1]
    wayback_url = f'http://web.archive.org/web/{ts}/{orig}'
    print(f'\nFetching: {wayback_url}')
    try:
        req = urllib.request.Request(wayback_url, headers={'User-Agent': 'Mozilla/5.0'})
        resp2 = urllib.request.urlopen(req, timeout=30)
        content = resp2.read().decode('utf-8', errors='replace')
        print(f'Content length: {len(content)} chars')
        # Show first 500 chars of body
        import re
        text = re.sub(r'<[^>]+>', ' ', content)
        text = re.sub(r'\s+', ' ', text).strip()
        print(f'Text preview: {text[:500]}')
    except Exception as e:
        print(f'Error fetching: {e}')
