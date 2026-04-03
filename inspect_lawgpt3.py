import sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session(); s.impersonate = 'chrome'
B = 'https://platform.lawgpt.pk'

# The platform is a React SPA - let's try the API endpoints
# First get the page source to find API base
resp = s.get(B, timeout=15)
soup = BeautifulSoup(resp.text, 'html.parser')

# Find script tags with API URLs
for script in soup.find_all('script'):
    src = script.get('src', '')
    if src:
        print(f'Script: {src}')
    text = script.string or ''
    if 'api' in text.lower() and len(text) < 2000:
        print(f'Inline: {text[:300]}')

# Try common API endpoints
print('\n=== API probe ===')
for ep in ['/api/auth/login', '/api/login', '/auth/login', '/api/v1/login', '/sanctum/csrf-cookie']:
    time.sleep(0.5)
    try:
        resp = s.post(f'{B}{ep}',
            json={'email': 'ab.salaam@hotmail.com', 'password': 'openhouse'},
            headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
            timeout=8)
        print(f'{ep}: {resp.status_code} -> {resp.text[:200]}')
    except Exception as e:
        print(f'{ep}: {e}')
