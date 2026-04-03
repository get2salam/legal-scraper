import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# We found IDs like UY2FqaJw1, UY2Fvbpw, UY2FwbZw in the pakistancode HTML
# These are likely internal identifiers for legislation
# Let's try to find archived pages for these IDs

known_ids = ['UY2FqaJw1', 'UY2Fqa', 'UY2Fvbpw', 'UY2FwbZw']

print("Checking CDX for specific UY* legislation IDs...")
for uid in known_ids[:4]:
    for base in ['http://pakistancode.gov.pk/english/', 'http://www.pakistancode.gov.pk/english/']:
        url = f'{base}{uid}'
        try:
            api = f'http://web.archive.org/cdx/search/cdx?url={urllib.parse.quote(url)}&output=json&limit=3&fl=timestamp,original,statuscode,length&filter=statuscode:200'
            resp = urllib.request.urlopen(api, timeout=10)
            data = json.loads(resp.read())
            count = len(data) - 1
            if count > 0:
                print(f'{url}: {count} archived snapshots')
                for row in data[1:3]:
                    print(f'  len={row[3]} {row[0]}: {row[1]}')
        except Exception as e:
            pass
    time.sleep(0.5)

print()
print("CDX for all pakistancode UY* paths...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/UY*&output=json&limit=20&fl=timestamp,original,statuscode,length&filter=statuscode:200&collapse=original'
    resp = urllib.request.urlopen(api, timeout=15)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:10]:
        print(f'  len={row[3]} {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(1)

# Also try to fetch the actual page with the UY ID
print()
print("Trying to fetch an archived UY* legislation page...")
try:
    # Try fetching a specific ID page via wayback
    test_urls = [
        'http://web.archive.org/web/20191123022612/http://pakistancode.gov.pk/english/UY2FqaJw1',
        'http://web.archive.org/web/20191123022612/http://pakistancode.gov.pk/english/public/details/UY2FqaJw1',
    ]
    for url in test_urls:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            resp = urllib.request.urlopen(req, timeout=15)
            content = resp.read()
            html = content.decode('utf-8', errors='replace')
            text = re.sub(r'<[^>]+>', ' ', html)
            text = re.sub(r'\s+', ' ', text).strip()
            print(f'  URL: {url}')
            print(f'  Size: {len(content)} bytes')
            print(f'  Text: {text[:300]}')
        except Exception as e:
            print(f'  {url}: {type(e).__name__}')
        time.sleep(1)
except Exception as e:
    print(f'Error: {e}')

# Also try punjablaws specific laws
print()
print("Trying to fetch archived punjablaws laws listing...")
try:
    url = 'http://web.archive.org/web/20081021151954/http://punjablaws.gov.pk:80/laws/'
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urllib.request.urlopen(req, timeout=20)
    content = resp.read()
    html = content.decode('utf-8', errors='replace')
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    print(f'Size: {len(content)} bytes')
    print(f'Text: {text[:600]}')
    # Look for legislation links
    links = re.findall(r'href=["\']([^"\']+)["\']', html)
    print(f'Links ({len(links)}):')
    for link in links[:30]:
        if 'archive.org' not in link.lower() and len(link) > 3:
            print(f'  {link}')
except Exception as e:
    print(f'Error: {e}')
