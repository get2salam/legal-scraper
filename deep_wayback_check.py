import json, sys, urllib.request, time, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Get more URLs from pakistancode Wayback - look for actual act pages (not just index)
api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/*&output=json&limit=100&fl=timestamp,original,statuscode&filter=statuscode:200'
try:
    resp = urllib.request.urlopen(api, timeout=30)
    data = json.loads(resp.read())
    print(f'Total snapshots: {len(data)-1}')
    for row in data[1:]:
        print(f'  {row[0]}: {row[1][:100]}')
except Exception as e:
    print(f'CDX error: {e}')

time.sleep(2)

# Also try with path filter to get actual act pages
print('\n--- Looking for specific act pages ---')
api2 = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/UY*&output=json&limit=20&fl=timestamp,original,statuscode'
try:
    resp2 = urllib.request.urlopen(api2, timeout=30)
    data2 = json.loads(resp2.read())
    print(f'UY* snapshots: {len(data2)-1}')
    for row in data2[1:10]:
        print(f'  {row[0]}: {row[1][:100]}')
except Exception as e:
    print(f'CDX2 error: {e}')

time.sleep(2)

# Try with a specific act URL pattern - pakistancode uses URL encoded strings
print('\n--- Checking any act-specific pages ---')
api3 = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/*act*&output=json&limit=20&fl=timestamp,original,statuscode&filter=statuscode:200'
try:
    resp3 = urllib.request.urlopen(api3, timeout=30)
    data3 = json.loads(resp3.read())
    print(f'Act-specific snapshots: {len(data3)-1}')
    for row in data3[1:10]:
        print(f'  {row[0]}: {row[1][:100]}')
except Exception as e:
    print(f'CDX3 error: {e}')
