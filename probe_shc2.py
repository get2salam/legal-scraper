"""Probe SHC IP-based search API directly (faster)."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'
IP_BASE = 'http://43.245.130.98:8056'

print("=== SHC IP Search API ===")
# Try GET on search endpoint
for ep in [
    '/caselaw/search-all/search',
    '/caselaw/public/search-all/search',
    '/caselaw/search',
    '/caselaw/api/search',
    '/caselaw/',
    '/',
]:
    try:
        resp = s.get(f'{IP_BASE}{ep}', timeout=10)
        print(f"GET {ep}: {resp.status_code} ({len(resp.text)} chars) -> {resp.text[:100]}")
    except Exception as e:
        print(f"GET {ep}: TIMEOUT/ERR {str(e)[:50]}")
    time.sleep(0.5)

# Also try the shc.gov.pk main
print("\n=== SHC main site quick ===")
try:
    resp2 = s.get('https://www.shc.gov.pk', timeout=10, stream=False)
    # Find all hrefs with 'caselaw' or 'judgment'
    links = re.findall(r'href=["\']([^"\']*(?:caselaw|judgment|case|decision)[^"\']*)["\']', resp2.text, re.I)
    print(f"Status: {resp2.status_code}")
    print(f"Caselaw links: {list(set(links))[:8]}")
except Exception as e:
    print(f"SHC main: ERR {str(e)[:60]}")
