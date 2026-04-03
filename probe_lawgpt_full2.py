import sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'
SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'

# The API requires a 'mode' field - try different modes
for mode in ['keyword', 'citation', 'semantic', 'fulltext', 'hybrid', 'exact']:
    resp = s.post(SEARCH,
        json={'query': '2024 PLJ 1', 'mode': mode, 'page': 1, 'pageSize': 1},
        headers={'Content-Type': 'application/json'},
        timeout=15)
    print(f"mode={mode}: {resp.status_code} ({len(resp.text)} chars)")
    if resp.status_code == 200:
        data = resp.json()
        print(f"  Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        print(json.dumps(data, indent=2)[:2000])
        break
    elif resp.status_code != 400:
        print(f"  Response: {resp.text[:200]}")
    time.sleep(1)
