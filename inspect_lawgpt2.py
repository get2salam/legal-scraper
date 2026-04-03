import sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session(); s.impersonate = 'chrome'
B = 'https://platform.lawgpt.pk'

# Check platform
resp = s.get(B, timeout=15)
print(f'Platform: {resp.status_code} ({len(resp.text)} chars) -> {resp.url}')
soup = BeautifulSoup(resp.text, 'html.parser')
title = soup.find('title')
print(f'Title: {title.get_text() if title else "?"}')

# Login form
for form in soup.find_all('form'):
    print(f'Form: action={form.get("action","")} method={form.get("method","")}')
    for inp in form.find_all(['input','button']):
        n = inp.get('name',''); t = inp.get('type',''); v = inp.get('value','')[:20]
        print(f'  {inp.name}: name={n} type={t} val={v}')

# Try login
time.sleep(2)
print('\n=== Trying login ===')
login_data = {'email': 'ab.salaam@hotmail.com', 'password': 'openhouse'}
resp2 = s.post(f'{B}/login', data=login_data, timeout=15, allow_redirects=True)
print(f'POST /login: {resp2.status_code} -> {resp2.url}')
print(resp2.text[:800])
