import sys, json, time
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
s = r.Session(); s.impersonate = 'chrome'
FACETS = 'https://facets-lawcases-2.azurewebsites.net/api'
SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'

print("Fetching full facets index...")
resp = s.get(f'{FACETS}/facets/lawcases', timeout=30)
data = resp.json()
print(f"Keys: {list(data.keys())}")

# Find PLJ cases in facets
facets = data.get('facets', {})
print(f"Facet categories: {list(facets.keys())[:10]}")

# Check Section facet
sections = facets.get('Section', [])
print(f"\nSection facets ({len(sections)} total):")
for s_item in sections[:30]:
    print(f"  {s_item.get('value','')}: {s_item.get('count',0)}")

# Find PLJ specifically
plj = [s for s in sections if 'PLJ' in str(s.get('value',''))]
print(f"\nPLJ entries: {plj}")

# Check if there are actual document IDs or pagination info
print(f"\nFull data structure keys: {list(data.keys())}")
if 'value' in data:
    print(f"Total documents: {len(data['value'])}")
    # Check first PLJ doc
    plj_docs = [d for d in data['value'] if 'PLJ' in d.get('Section', [])]
    print(f"PLJ documents in response: {len(plj_docs)}")
    if plj_docs:
        print(f"Sample PLJ doc keys: {list(plj_docs[0].keys())}")
        print(f"Sample: {str(plj_docs[0])[:400]}")
