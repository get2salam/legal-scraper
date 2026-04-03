import json, sys, time, urllib.request, urllib.parse, re
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Strategy: Check if Wayback Machine has archived:
# 1. pakistancode.gov.pk individual legislation pages
# 2. nalaw.gov.pk pages (National Assembly Law portal)

# First check what archived pages exist for pakistancode
print("Checking Wayback for pakistancode.gov.pk deep pages...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/*&output=json&limit=50&fl=timestamp,original,statuscode,length&filter=statuscode:200&collapse=original'
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:]:
        print(f'  [{row[2]}] len={row[3]} {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

print()
print("Checking Wayback for molaw.gov.pk deep pages...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=molaw.gov.pk/*&output=json&limit=20&fl=timestamp,original,statuscode&filter=statuscode:200&collapse=original'
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:10]:
        print(f'  {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

print()
print("Checking Wayback for nalaw.gov.pk deep pages...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=nalaw.gov.pk/*&output=json&limit=20&fl=timestamp,original,statuscode&filter=statuscode:200&collapse=original'
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:10]:
        print(f'  {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

# Try a known specific legislation from na.gov.pk
print()
print("Checking na.gov.pk legislation CDX...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=na.gov.pk/*legislation*&output=json&limit=10&fl=timestamp,original,statuscode&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:]:
        print(f'  {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

# Try senate.gov.pk
print()
print("Checking senate.gov.pk CDX...")
try:
    api = 'http://web.archive.org/cdx/search/cdx?url=senate.gov.pk/*bills*&output=json&limit=10&fl=timestamp,original,statuscode&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=20)
    data = json.loads(resp.read())
    print(f'Results: {len(data)-1}')
    for row in data[1:]:
        print(f'  {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')
