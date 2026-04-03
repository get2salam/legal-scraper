"""Test direct doc_id fetch and facets full dump."""
import sys, json, time, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'
SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'
FACETS = 'https://facets-lawcases-2.azurewebsites.net/api/facets/lawcases'

# 1. See how many unique cases the search API can ever surface
print("=== Testing query diversity ===")
seen_ids = set()
test_queries = [
    'murder', 'divorce', 'contract', 'property', 'bail', 
    'tax', 'customs', 'banking', 'rent', 'land acquisition',
    'constitutional petition', 'habeas corpus', 'contempt',
    'service matters', 'insurance', 'copyright', 'trademark',
    '2020', '2019', '2018', '2017', '1990', '1980', '1970'
]
for q in test_queries:
    resp = s.post(SEARCH, json={'query': q, 'mode': 'keyword', 'page': 1, 'pageSize': 10},
                  headers={'Content-Type': 'application/json'}, timeout=15)
    if resp.status_code == 200:
        vals = resp.json().get('value', [])
        new_ids = set()
        for v in vals:
            doc_id = v.get('doc_id', v.get('id', ''))
            if doc_id and doc_id not in seen_ids:
                seen_ids.add(doc_id)
                new_ids.add(doc_id)
        print(f"'{q}': {len(new_ids)} NEW cases (total unique: {len(seen_ids)})")
    time.sleep(1.5)

print(f"\nTotal unique cases found across {len(test_queries)} queries: {len(seen_ids)}")

# 2. Check facets for doc_id list
print("\n=== Facets structure check ===")
resp = s.get(FACETS, timeout=30)
data = resp.json()
facets = data.get('facets', {})
# Check if there are doc IDs anywhere
all_keys = list(facets.keys())
print(f"Facet keys: {all_keys}")
for key in all_keys:
    items = facets[key][:3]
    print(f"  {key} sample: {items}")

# 3. Try semantic/vector search mode
print("\n=== Semantic search test ===")
resp = s.post(SEARCH, json={'query': 'property dispute Punjab', 'mode': 'semantic', 'page': 1, 'pageSize': 10},
              headers={'Content-Type': 'application/json'}, timeout=15)
print(f"Semantic mode: {resp.status_code}")
if resp.status_code == 200:
    vals = resp.json().get('value', [])
    sem_ids = {v.get('doc_id','') for v in vals}
    overlap = sem_ids & seen_ids
    print(f"  {len(vals)} results, {len(overlap)} overlap with keyword results")
    for v in vals[:3]:
        print(f"  doc_id={v.get('doc_id','')} Section={v.get('Section',[])} Title={v.get('Title','')[:40]}")
