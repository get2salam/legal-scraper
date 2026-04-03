import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
import json

s = requests.Session(impersonate='chrome')

# SST - Get the full functions.js to find getJudgements implementation
r = s.get('https://sstsindh.gov.pk/admin/js/functions.js', timeout=15)
text = r.text

# Find the $.getJudgements definition
idx = text.find('getJudgements')
while idx >= 0:
    # Print context around each occurrence
    start = max(0, idx - 100)
    end = min(len(text), idx + 500)
    print(f"=== getJudgements at {idx} ===")
    print(text[start:end])
    print("---")
    idx = text.find('getJudgements', idx + 1)

# Also look for API_URL usage near judgements
idx = text.find('Judgement')
while idx >= 0:
    start = max(0, idx - 200)
    end = min(len(text), idx + 500)
    if 'api' in text[start:end].lower() or 'ajax' in text[start:end].lower():
        print(f"\n=== Judgement API at {idx} ===")
        print(text[start:end])
    idx = text.find('Judgement', idx + 1)

# Try searching for 'api/' 
idx = text.find("api/")
while idx >= 0:
    start = max(0, idx - 100)
    end = min(len(text), idx + 200)
    snippet = text[start:end]
    if 'judg' in snippet.lower() or 'appeal' in snippet.lower():
        print(f"\n=== API call at {idx} ===")
        print(snippet)
    idx = text.find("api/", idx + 1)
