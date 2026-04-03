"""Probe SC website structure."""
import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'

urls = [
    'https://www.supremecourt.gov.pk/judgements/',
    'https://www.supremecourt.gov.pk/judgments/',
    'https://www.supremecourt.gov.pk/cases/',
    'https://www.supremecourt.gov.pk/',
]
for url in urls:
    try:
        resp = s.get(url, timeout=15)
        print(f"{url} -> {resp.status_code} ({len(resp.text)} chars)")
        if resp.status_code == 200 and len(resp.text) > 500:
            # Find pagination, case links
            links = re.findall(r'href=["\']([^"\']+(?:judgment|case|decision)[^"\']*)["\']', resp.text, re.I)
            print(f"  Judgment links: {links[:5]}")
            pages = re.findall(r'page[=\s]*(\d+)', resp.text)
            print(f"  Page numbers: {list(set(pages))[:10]}")
            print(f"  Sample HTML: {resp.text[500:900]}")
            break
    except Exception as e:
        print(f"  Error: {e}")
