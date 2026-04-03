import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
import json

s = requests.Session(impersonate='chrome')

# IHC - Test srchDecision (case law search), need to check parameter format
# The JS sends strings like: "{'PCASENO':'0','PJUG':'0',...}"
# That's literally a string of JS object notation, not actual JSON.
# Let's check what format it expects

# Test 1: Standard JSON
print('=== Test 1: Standard JSON ===')
r = s.post('https://mis.ihc.gov.pk/ihc.asmx/srchDecision',
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({
        'PCASENO': '0', 'PJUG': '0', 'PADV': '0',
        'PYEAR': '2025', 'pPrty': '', 'PDDATE': '01-01-1900',
        'PLANDMARK': '1', 'PAFR': '9999'
    }),
    timeout=60)
print(f"Status: {r.status_code}")
print(f"Response: {r.text[:500]}")

# Test 2: Try srchDecisionClms with year 2025 and judge 0 (all judges)
print('\n=== Test 2: srchDecisionClms all judges 2025 ===')
r2 = s.post('https://mis.ihc.gov.pk/ihc.asmx/srchDecisionClms',
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({
        'PCASENO': '0', 'PJUG': '0', 'PADV': '0',
        'PYEAR': '2025', 'pPrty': '', 'PDDATE': '01/01/1900',
        'PLANDMARK': 0, 'PAFR': '0'
    }),
    timeout=60)
print(f"Status: {r2.status_code}")
data2 = r2.json()
if 'd' in data2:
    parsed = json.loads(data2['d'])
    if isinstance(parsed, list):
        print(f"Records: {len(parsed)}")
        if parsed:
            print(f"Keys: {list(parsed[0].keys())}")
    elif parsed == 'empty':
        print("Empty result")
    else:
        print(f"Other: {str(parsed)[:200]}")
else:
    print(f"Response keys: {list(data2.keys())}")
    print(f"Response: {r2.text[:500]}")

# Test 3: srchDecisionClms with PJUG=0, no landmark filter, year 2024
print('\n=== Test 3: srchDecisionClms all judges 2024 ===')
r3 = s.post('https://mis.ihc.gov.pk/ihc.asmx/srchDecisionClms',
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({
        'PCASENO': '0', 'PJUG': '0', 'PADV': '0',
        'PYEAR': '2024', 'pPrty': '', 'PDDATE': '01/01/1900',
        'PLANDMARK': 0, 'PAFR': '0'
    }),
    timeout=60)
data3 = r3.json()
if 'd' in data3:
    parsed3 = json.loads(data3['d'])
    if isinstance(parsed3, list):
        print(f"Records: {len(parsed3)}")
    elif parsed3 == 'empty':
        print("Empty")

# Test 4: IHC PDF download
print('\n=== IHC PDF Test ===')
test_path = "/attachments/judgements/200273/1/W.P_No.2795_of_2025._Approved_for_reporting_639046919007357626.pdf"
r4 = s.get(f"https://mis.ihc.gov.pk{test_path}", timeout=30)
print(f"Status: {r4.status_code}, Size: {len(r4.content)} bytes, Is PDF: {r4.content[:4] == b'%PDF'}")
