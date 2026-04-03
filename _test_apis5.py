import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
import json

s = requests.Session(impersonate='chrome')

# IHC - Get available years
print('=== IHC Years ===')
r = s.post('https://mis.ihc.gov.pk/ihc.asmx/FillYear',
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data='{}', timeout=15)
data = r.json()
years = json.loads(data['d'])
print(f"Years count: {len(years)}")
if years:
    print(f"First: {years[0]}")
    print(f"Last: {years[-1]}")
    # Show year codes  
    for y in years[:5]:
        print(f"  Code={y['Code']} Name={y['Name']}")
    print("...")
    for y in years[-5:]:
        print(f"  Code={y['Code']} Name={y['Name']}")

# IHC - Test srchDecision (case law search, search by year)
print('\n=== IHC: srchDecision by year 2025 ===')
r2 = s.post('https://mis.ihc.gov.pk/ihc.asmx/srchDecision',
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({
        'PCASENO': '0', 'PJUG': '0', 'PADV': '0',
        'PYEAR': '2025', 'pPrty': '', 'PDDATE': '01-01-1900',
        'PLANDMARK': '1', 'PAFR': '9999'
    }),
    timeout=60)
data2 = r2.json()
parsed2 = json.loads(data2['d'])
if isinstance(parsed2, list):
    print(f"Judgments for 2025: {len(parsed2)}")
    if parsed2:
        print(f"Keys: {list(parsed2[0].keys())}")
        # Just show first
        rec = parsed2[0]
        print(json.dumps({
            'CASENO': rec.get('CASENO'),
            'PARTIES': rec.get('PARTIES'),
            'DDATE': rec.get('DDATE'),
            'ATTACHMENTS': rec.get('ATTACHMENTS'),
            'O_CITATION': rec.get('O_CITATION'),
            'AUTHOR_JUDGES': rec.get('AUTHOR_JUDGES'),
        }, indent=2, ensure_ascii=False))
elif parsed2 == 'empty':
    print("Empty")
else:
    print(f"Result: {str(parsed2)[:500]}")

# IHC - test PDF download
print('\n=== IHC PDF Test ===')
test_path = "/attachments/judgements/200273/1/W.P_No.2795_of_2025._Approved_for_reporting_639046919007357626.pdf"
r3 = s.get(f"https://mis.ihc.gov.pk{test_path}", timeout=30)
print(f"Status: {r3.status_code}, Size: {len(r3.content)} bytes")
print(f"Is PDF: {r3.content[:4] == b'%PDF'}")

# SST - Get ALL records to see the range  
print('\n=== SST Full Range ===')
r4 = s.get('https://sstsindh.gov.pk/admin/api/judgements.php', params={
    'action': 'getJ', 'start': 260, 'noOfRecords': 10, 'keyword': ''
}, timeout=15)
data4 = r4.json()
print(f"Last page records: {len(data4['data'])}")
for rec in data4['data']:
    print(f"  ID={rec['id']} Appeal={rec['appeal']} Date={rec['created_at']}")
