import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Probe Wayback Machine more deeply for legislation content
# Focus: find any archived page with actual legislation text

print("=" * 60)
print("PROBE 1: pakistancode.gov.pk - any subpages with 'english' path?")
print("=" * 60)
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/*&output=json&limit=20&fl=timestamp,original,statuscode,length&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:10]:
        print(f'  [{row[2]}] len={row[3]} {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')
time.sleep(2)

print()
print("=" * 60)
print("PROBE 2: na.gov.pk - uploads/PDFs?")
print("=" * 60)
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=na.gov.pk/uploads/*&output=json&limit=10&fl=timestamp,original,statuscode,mimetype&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=15)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:10]:
        print(f'  {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')
time.sleep(2)

print()
print("=" * 60)
print("PROBE 3: Broader *.gov.pk legislation search on WB")
print("=" * 60)
for domain in ['nalaw.gov.pk/*', 'legislation.gov.pk/*', 'federallegislation.gov.pk/*', 'molaw.gov.pk/legislation*']:
    try:
        api = f'http://web.archive.org/cdx/search/cdx?url={domain}&output=json&limit=5&fl=timestamp,original,statuscode&filter=statuscode:200'
        resp = urllib.request.urlopen(api, timeout=12)
        data = json.loads(resp.read())
        count = len(data) - 1
        print(f'{domain}: {count} results')
        for row in data[1:3]:
            print(f'  {row[0]}: {row[1][:80]}')
    except Exception as e:
        print(f'{domain}: {e}')
    time.sleep(1)

print()
print("=" * 60)
print("PROBE 4: Try fetching a live Pakistani gov site (via requests with longer timeout)")
print("=" * 60)
import socket
socket.setdefaulttimeout(25)
for url in [
    'https://na.gov.pk/en/legislation.php',
    'http://www.molaw.gov.pk',
    'https://senate.gov.pk/en/bills.php',
]:
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        resp = urllib.request.urlopen(req, timeout=25)
        content = resp.read()
        print(f'{url}: HTTP {resp.status} ({len(content):,} bytes)')
        text = re.sub(r'<[^>]+>', ' ', content.decode('utf-8', errors='replace'))
        text = re.sub(r'\s+', ' ', text).strip()
        print(f'  Text preview: {text[:200]}')
    except Exception as e:
        print(f'{url}: {type(e).__name__} - {str(e)[:80]}')
    time.sleep(2)

print()
print("=" * 60)
print("PROBE 5: Check if na.gov.pk has Wayback snapshots at all (any URL)")
print("=" * 60)
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=na.gov.pk/*&output=json&limit=10&fl=timestamp,original,statuscode&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=15)
    data = json.loads(resp.read())
    print(f'na.gov.pk: {len(data)-1} archived pages found')
    for row in data[1:5]:
        print(f'  {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')
