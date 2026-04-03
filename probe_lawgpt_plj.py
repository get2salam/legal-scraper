import sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'

# Case Search API - public, no auth
SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search'
FACETS = 'https://facets-lawcases-2.azurewebsites.net/api'

# First check PLJ facets to understand structure
print("=== PLJ via Facets API ===")
resp = s.get(f'{FACETS}/facets/lawcases', 
    params={'filter': 'PLJ', 'size': 2},
    timeout=15)
print(f"Status: {resp.status_code}")
print(resp.text[:1000])
time.sleep(2)

# Try direct search for PLJ
print("\n=== PLJ via Search API ===")
for query_style in [
    {'q': 'PLJ', 'reporter': 'PLJ', 'size': 1},
    {'query': '2024 PLJ 1', 'size': 1},
    {'search': 'PLJ', 'citation': 'PLJ', 'size': 1},
]:
    resp2 = s.get(SEARCH, params=query_style, timeout=15)
    print(f"Params {list(query_style.keys())}: {resp2.status_code} ({len(resp2.text)} chars)")
    if resp2.status_code == 200 and len(resp2.text) > 100:
        try:
            data = resp2.json()
            print(f"  Keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
            print(f"  Sample: {str(data)[:300]}")
        except:
            print(f"  Raw: {resp2.text[:300]}")
        break
    time.sleep(1)

# Try the lawcases endpoint
print("\n=== /api/lawcases ===")
for base in ['https://prod-search-engine.azurewebsites.net', 'https://facets-lawcases-2.azurewebsites.net']:
    resp3 = s.get(f'{base}/api/lawcases',
        params={'reporter': 'PLJ', 'page': 1, 'size': 1},
        timeout=15)
    print(f"{base}: {resp3.status_code} ({len(resp3.text)} chars)")
    if resp3.status_code == 200:
        print(resp3.text[:400])
    time.sleep(1)
