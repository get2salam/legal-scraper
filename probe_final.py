import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Last probes: Check what's available via Wayback Machine for Pakistani legislation
# 1. Try fetching actual archived legislation page with smaller timeout
# 2. Check web.archive.org search API for legislation content
# 3. Try alternate known URLs

print("=" * 60)
print("Testing Wayback API availability (basic connectivity)")
print("=" * 60)
try:
    resp = urllib.request.urlopen('http://web.archive.org/cdx/search/cdx?url=example.com&output=json&limit=1', timeout=15)
    print(f'WB CDX API: OK ({resp.status})')
except Exception as e:
    print(f'WB CDX API: {e}')

time.sleep(1)

print()
print("=" * 60)
print("Trying to fetch archived pakistancode page (smaller request)")
print("=" * 60)
# Use a snapshot we know exists from CDX (from earlier results)
# 20191123022612 for pakistancode.gov.pk:80/english/
# Let's try a specific legislation URL that might be in the archive
try:
    # Check the CDX for any /english/ path with substantial content
    api = 'http://web.archive.org/cdx/search/cdx?url=pakistancode.gov.pk/english/*&output=json&limit=5&fl=timestamp,original,statuscode,mimetype&filter=statuscode:200'
    resp = urllib.request.urlopen(api, timeout=15)
    data = json.loads(resp.read())
    print(f'pakistancode /english/* snapshots: {len(data)-1}')
    for row in data[1:]:
        print(f'  [{row[3]}] {row[0]}: {row[1][:80]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

print()
print("=" * 60)
print("Checking laws.com.pk / pakistanlaw / other sources on WB")
print("=" * 60)
for domain in [
    'laws.com.pk/*',
    'pakistanlaw.pk/*',
    'pakistanlaw.net/*',
    'pakistanlegal.org/*',
    'sindh.gov.pk/legislation*',
    'punjablaws.gov.pk/*',
    'khyberpakhtunkhwa.gov.pk/*legislation*',
]:
    try:
        api = f'http://web.archive.org/cdx/search/cdx?url={domain}&output=json&limit=3&fl=timestamp,original,statuscode&filter=statuscode:200'
        resp = urllib.request.urlopen(api, timeout=10)
        data = json.loads(resp.read())
        count = len(data) - 1
        if count > 0:
            print(f'{domain}: {count} results')
            for row in data[1:3]:
                print(f'  {row[0]}: {row[1][:80]}')
        else:
            print(f'{domain}: no results')
    except Exception as e:
        print(f'{domain}: {type(e).__name__}')
    time.sleep(0.5)

print()
print("=" * 60)
print("Summary: What strategy can fill stubs?")
print("=" * 60)
print("""
KEY FINDINGS:
1. All 4,817 stubs have source_url pointing to pakistanlawsite.com (login-walled)
2. Stubs have full_text like '[Section RULE]\\n\"-1\"' (= content not scraped from source)
3. The legislation_*.jsonl JSONL files have the SAME data (also \"-1\" for stubs)
4. Wayback Machine has:
   - pakistancode.gov.pk: only homepage/english/ (Flash-based, no deep pages)
   - molaw.gov.pk: homepage only from 2007-2012 (no deep pages)
   - na.gov.pk: very few snapshots (homepage only, 1998)
   - nalaw.gov.pk, legislation.gov.pk: NOT archived at all
5. Live sources: ALL gov.pk sites are timing out (firewall/geoblocking from UK?)
   - na.gov.pk, senate.gov.pk, molaw.gov.pk all refuse connection

CONCLUSION:
- Wayback Machine cannot fill these stubs (no archived legislation pages)
- Live gov.pk sites are unreachable from this machine
- The data gap is systemic: content was behind login at pakistanlawsite.com
  and was never publicly indexed
""")
