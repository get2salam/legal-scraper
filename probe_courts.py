"""Probe all court websites for judgment availability."""
import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
import time

s = r.Session()
s.impersonate = 'chrome'
s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})

courts = [
    ('SC', 'https://www.supremecourt.gov.pk'),
    ('LHC', 'https://www.lhc.gov.pk'),
    ('LHC judgments', 'https://www.lhc.gov.pk/judgments'),
    ('SHC', 'https://www.shc.gov.pk'),
    ('SHC judgments', 'https://www.shc.gov.pk/judgments'),
    ('IHC', 'https://www.ihc.gov.pk'),
    ('IHC judgments', 'https://ihc.gov.pk/judgment'),
    ('PHC', 'https://www.peshawarhighcourt.gov.pk'),
    ('BHC', 'https://www.bhc.gov.pk'),
]

for name, url in courts:
    try:
        resp = s.get(url, timeout=12)
        size = len(resp.text)
        print(f"{name}: {resp.status_code} ({size} chars)")
        if resp.status_code == 200 and size > 1000:
            # Look for judgment links
            jlinks = re.findall(r'href=["\']([^"\']*(?:judgment|decision|case|order)[^"\']*)["\']', resp.text, re.I)
            if jlinks:
                print(f"  Judgment links: {jlinks[:3]}")
            # Look for API endpoints
            apis = re.findall(r'(?:api|search|fetch)[^"\'<>\s]*\.(?:php|asp|json|do)', resp.text, re.I)
            if apis:
                print(f"  API hints: {list(set(apis))[:3]}")
    except Exception as e:
        print(f"{name}: ERROR - {str(e)[:60]}")
    time.sleep(1)
