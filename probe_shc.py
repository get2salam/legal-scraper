"""Probe SHC caselaw portal structure."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'
BASE = 'https://caselaw.shc.gov.pk'
IP_BASE = 'http://43.245.130.98:8056'

# 1. Check home page
print("=== SHC Home ===")
resp = s.get(f'{BASE}/caselaw/public/home', timeout=15)
print(f"Status: {resp.status_code}, Size: {len(resp.text)}")
if resp.status_code == 200:
    # Find forms, APIs, search endpoints
    forms = re.findall(r'action=["\']([^"\']+)["\']', resp.text, re.I)
    apis = re.findall(r'(?:url|href|action)\s*[:=]\s*["\']([^"\']*(?:search|api|json|fetch|ajax)[^"\']*)["\']', resp.text, re.I)
    print(f"Form actions: {forms[:5]}")
    print(f"API hints: {list(set(apis))[:5]}")
    print(f"Sample: {resp.text[1000:1500]}")

time.sleep(1)

# 2. Try the IP-based search API
print("\n=== IP Search API ===")
search_url = f'{IP_BASE}/caselaw/search-all/search'
# Try GET first
resp2 = s.get(search_url, timeout=10)
print(f"GET: {resp2.status_code} ({len(resp2.text)} chars) -> {resp2.text[:200]}")

# Try POST with various payloads
for payload in [
    {'q': 'murder', 'page': 1},
    {'search': 'murder', 'page': 1},
    {'query': 'murder'},
    {},
]:
    resp3 = s.post(search_url, json=payload, headers={'Content-Type': 'application/json'}, timeout=10)
    print(f"POST {payload}: {resp3.status_code} -> {resp3.text[:150]}")
    time.sleep(0.5)

# 3. Check search endpoint patterns
print("\n=== Search endpoint patterns ===")
for ep in [
    f'{BASE}/caselaw/public/search',
    f'{BASE}/caselaw/search',
    f'{BASE}/caselaw/api/search',
    f'{IP_BASE}/caselaw/public/search',
    f'{IP_BASE}/caselaw/search',
]:
    try:
        r2 = s.get(ep, timeout=8)
        print(f"{ep}: {r2.status_code} ({len(r2.text)} chars)")
    except Exception as e:
        print(f"{ep}: ERROR {str(e)[:40]}")
    time.sleep(0.3)
