"""Probe SHC auto-login and case search API."""
import sys, re, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'
IP_BASE = 'http://43.245.130.98:8056'

# 1. Try auto-login
print("=== Auto-login ===")
login_url = f'{IP_BASE}/caselaw/faces/pub/Login.jsp?u=shc&p=shc&a=caselaw-%09shc&rp=/caselaw/faces/pub/SearchCitationWise.jsp'
resp = s.get(login_url, timeout=15)
print(f"Login: {resp.status_code} ({len(resp.text)} chars)")
print(f"Cookies: {dict(s.cookies)}")
print(f"Sample: {resp.text[:500]}")
time.sleep(1)

# 2. Try cases.shc.gov.pk search
print("\n=== cases.shc.gov.pk search ===")
search_url = 'https://cases.shc.gov.pk/lar/web/index.php?r=cases%2Fsearch'
resp2 = s.get(search_url, timeout=12)
print(f"Status: {resp2.status_code} ({len(resp2.text)} chars)")
# Find API hints
apis = re.findall(r'(?:url|action|href)\s*[=:]\s*["\']([^"\']*(?:search|api|case|fetch|ajax|json)[^"\']*)["\']', resp2.text, re.I)
print(f"API hints: {list(set(apis))[:8]}")
print(f"Sample: {resp2.text[500:1000]}")
time.sleep(1)

# 3. Try direct case list / search POST
print("\n=== Search POST test ===")
for ep, payload in [
    (f'https://cases.shc.gov.pk/lar/web/index.php?r=cases%2Fsearch', {'q': 'murder', 'year': '2024'}),
    (f'{IP_BASE}/caselaw/search-all/search', {'q': 'murder'}),
]:
    try:
        r2 = s.post(ep, data=payload, timeout=10)
        print(f"POST {ep[:60]}: {r2.status_code} ({len(r2.text)} chars)")
        if r2.status_code == 200:
            # Look for case records
            cases = re.findall(r'(?:case[_-]?no|citation|title|petitioner)["\']?\s*[:=]\s*["\']([^"\']+)', r2.text, re.I)
            print(f"  Case hints: {cases[:5]}")
    except Exception as e:
        print(f"POST {ep[:60]}: ERR {str(e)[:50]}")
    time.sleep(0.5)
