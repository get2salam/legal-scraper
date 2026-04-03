import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# The pakistancode.gov.pk/english/ page uses JavaScript links like 'index', 'Xki72H' etc.
# These are likely Flash/JS-based navigation. Let's check if there are deeper archived pages.

print("Checking CDX for deep pakistancode.gov.pk subpages (with content length > 10KB)...")
try:
    # Collapse by URL to get unique pages
    api = ('http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/UY*'
           '&output=json&limit=30&fl=timestamp,original,statuscode,length'
           '&filter=statuscode:200&collapse=original')
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'UY* paths: {len(data)-1} results')
    for row in data[1:15]:
        print(f'  len={row[3]:>8} {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

print()
print("Checking broader pakistancode subpages (any path, len > 10KB)...")
try:
    api = ('http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/*'
           '&output=json&limit=50&fl=timestamp,original,statuscode,length'
           '&filter=statuscode:200&filter=length:10000'
           '&collapse=original')
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:20]:
        print(f'  len={row[3]:>8} {row[0]}: {row[1][:90]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

print()
print("Checking for specific legislation on WB - 'Abandoned Property' search...")
# Try specific legislation URL patterns from pakistancode
test_patterns = [
    'http://pakistancode.gov.pk/english/UY2FW6FM3',  # typical pattern
    'http://www.pakistancode.gov.pk/UY2FW6FM3',
    'http://pakistancode.gov.pk/details/UY',
    'http://www.pakistancode.gov.pk/acts/',
    'http://www.pakistancode.gov.pk/ordinances/',
]

for pattern in test_patterns:
    try:
        encoded = urllib.parse.quote(pattern, safe=':/?=&*')
        api = f'http://web.archive.org/cdx/search/cdx?url={pattern}*&output=json&limit=5&fl=timestamp,original,statuscode&filter=statuscode:200'
        resp = urllib.request.urlopen(api, timeout=10)
        data = json.loads(resp.read())
        count = len(data) - 1
        print(f'{pattern}: {count} results')
        for row in data[1:3]:
            print(f'  {row[0]}: {row[1]}')
    except Exception as e:
        print(f'{pattern}: {e}')
    time.sleep(1)
