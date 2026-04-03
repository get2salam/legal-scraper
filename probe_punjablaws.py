import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Last attempt: Check punjablaws.gov.pk and other provincial archives on Wayback
# Also try fetching a WB page we know exists

print("Testing punjablaws.gov.pk CDX directly...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=punjablaws.gov.pk/*&output=json&limit=10&fl=timestamp,original,statuscode,length&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=15)
    data = json.loads(resp.read())
    print(f'punjablaws.gov.pk: {len(data)-1} archived pages')
    for row in data[1:8]:
        print(f'  len={row[3]} {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(1)

print()
print("Testing sindhlegislature CDX...")
for domain in [
    'sindhlegislature.gov.pk/*',
    'kpklegislature.gov.pk/*',
    'balochlegislature.pk/*',
]:
    try:
        api = f'http://web.archive.org/cdx/search/cdx?url={domain}&output=json&limit=5&fl=timestamp,original,statuscode&filter=statuscode:200'
        resp = urllib.request.urlopen(api, timeout=10)
        data = json.loads(resp.read())
        count = len(data) - 1
        print(f'{domain}: {count} results')
    except Exception as e:
        print(f'{domain}: {type(e).__name__}')
    time.sleep(0.5)

print()
print("Trying to fetch the WB archived pakistancode page (the one we confirmed exists)...")
# We know this URL is archived: http://pakistancode.gov.pk:80/english/
# Try with a fresh request
for ts in ['20191123022612', '20191223083325', '20191023204142']:
    url = f'http://web.archive.org/web/{ts}/http://pakistancode.gov.pk:80/english/'
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; ResearchBot/1.0)',
        })
        resp = urllib.request.urlopen(req, timeout=20)
        content = resp.read()
        print(f'  {ts}: OK ({len(content):,} bytes)')
        # Look for actual legislation links in the page
        html = content.decode('utf-8', errors='replace')
        # Find any link with a numeric/ID pattern
        ids = re.findall(r'UY\w+|/english/\w{5,}|/acts/\w+|/ord/\w+', html)
        print(f'  Found potential legislation IDs: {ids[:10]}')
        break
    except Exception as e:
        print(f'  {ts}: {type(e).__name__} - {str(e)[:60]}')
    time.sleep(1)

print()
print("Checking if punjablaws actually has searchable content...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=punjablaws.gov.pk/laws*&output=json&limit=10&fl=timestamp,original,statuscode,length&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=15)
    data = json.loads(resp.read())
    print(f'punjablaws /laws*: {len(data)-1} results')
    for row in data[1:]:
        print(f'  len={row[3]} {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')
