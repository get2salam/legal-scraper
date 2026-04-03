import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
import json

s = requests.Session(impersonate='chrome')

# IHC - Search ALL judgments for judge 20 (no landmark filter)
r = s.post('https://mis.ihc.gov.pk/ihc.asmx/srchDecisionClms', 
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({
        'PCASENO': '0', 'PJUG': '20', 'PADV': '0', 'PYEAR': '0',
        'pPrty': '', 'PDDATE': '01/01/1900', 'PLANDMARK': 0, 'PAFR': '0'
    }),
    timeout=30)
data = r.json()
parsed = json.loads(data['d'])
print('All judgments for judge 20:', len(parsed) if isinstance(parsed, list) else parsed)

# Try judge 9 (Kayani - senior, many judgments) 
r2 = s.post('https://mis.ihc.gov.pk/ihc.asmx/srchDecisionClms', 
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({
        'PCASENO': '0', 'PJUG': '9', 'PADV': '0', 'PYEAR': '0',
        'pPrty': '', 'PDDATE': '01/01/1900', 'PLANDMARK': 0, 'PAFR': '0'
    }),
    timeout=30)
data2 = r2.json()
parsed2 = json.loads(data2['d'])
print('All judgments for judge 9:', len(parsed2) if isinstance(parsed2, list) else parsed2)

# Get retired judges
r3 = s.post('https://mis.ihc.gov.pk/ihc.asmx/Juges_GA', 
    headers={'Content-Type': 'application/json; charset=utf-8'},
    data=json.dumps({'_params': {'PISRETIRED': 1, 'CUR_REC': None}}),
    timeout=30)
data3 = r3.json()
parsed3 = json.loads(data3['d'])
print('Former judges count:', len(parsed3))
for j in parsed3[:5]:
    jid = j["JUDGE_ID"]
    name = j["JUG_REALNAME"]
    print(f"  ID={jid}  Name={name}")

# SST - Get the config.js and functions.js
print("\n=== SST CONFIG ===")
r4 = s.get('https://sstsindh.gov.pk/admin/js/config.js', timeout=15)
print('config.js:', r4.text[:1000])

print("\n=== SST FUNCTIONS ===")
r5 = s.get('https://sstsindh.gov.pk/admin/js/functions.js', timeout=15)
# Find getJudgements function
text = r5.text
idx = text.find('getJudgements')
if idx >= 0:
    print('getJudgements found at:', idx)
    print(text[max(0, idx-50):idx+2000])
else:
    print('getJudgements not found')
    # Look for function definitions
    import re
    funcs = re.findall(r'function\s+(\w+)', text)
    print('Functions:', funcs[:30])
    print('First 2000 chars:', text[:2000])
