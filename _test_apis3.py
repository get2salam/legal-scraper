import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
import json

s = requests.Session(impersonate='chrome')

# Get full getJudgements function from functions.js
r = s.get('https://sstsindh.gov.pk/admin/js/functions.js', timeout=15)
text = r.text

# Find the getJudgements implementation
idx = text.find('$.getJudgements = function')
if idx >= 0:
    # Get about 3000 chars to capture full function
    print("=== getJudgements implementation ===")
    print(text[idx:idx+3000])

# Also test the SST API directly
print("\n\n=== Testing SST API ===")
API_URL = "https://sstsindh.gov.pk/api/"

# Try fetching judgments
r2 = s.get(f"{API_URL}?action=getJ&search=&limit=10&start_limit=0&sort=&orderby=&limitVal=1", timeout=15)
print('GET Status:', r2.status_code)
print('Response:', r2.text[:2000])

# Try POST
r3 = s.post(API_URL, data={
    'action': 'getJ',
    'search': '',
    'limit': 10,
    'start_limit': 0,
    'sort': '',
    'orderby': '',
    'limitVal': 1
}, timeout=15)
print('\nPOST Status:', r3.status_code)
print('Response:', r3.text[:2000])
