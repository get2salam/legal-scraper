import sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'

# Real endpoint from agent findings
SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'

# Search for PLJ cases
print("=== PLJ search ===")
resp = s.post(SEARCH,
    json={'query': 'PLJ', 'page': 1, 'pageSize': 2, 'filters': {'reporter': ['PLJ']}},
    headers={'Content-Type': 'application/json'},
    timeout=15)
print(f"Status: {resp.status_code} ({len(resp.text)} chars)")
if resp.status_code == 200:
    data = resp.json()
    print(f"Keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")
    print(json.dumps(data, indent=2)[:1500])
else:
    print(resp.text[:300])

time.sleep(2)

# Try without filter
print("\n=== Generic PLJ ===")
resp2 = s.post(SEARCH,
    json={'query': '2024 PLJ', 'page': 1, 'pageSize': 1},
    headers={'Content-Type': 'application/json'},
    timeout=15)
print(f"Status: {resp2.status_code} ({len(resp2.text)} chars)")
if resp2.status_code == 200:
    data2 = resp2.json()
    # Look for full text field
    print(json.dumps(data2, indent=2)[:2000])
