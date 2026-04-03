import sys, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session(); s.impersonate = 'chrome'
B = 'http://lawgpt.pk'

# Homepage
resp = s.get(B, timeout=15)
print(f'Homepage: {resp.status_code} ({len(resp.text)} chars)')
soup = BeautifulSoup(resp.text, 'html.parser')
title = soup.find('title')
print(f'Title: {title.get_text() if title else "?"}')

# All links
links = set()
for a in soup.find_all('a', href=True):
    href = a['href']
    if href.startswith('http') or href.startswith('/'):
        links.add(href)
print(f'Links found: {len(links)}')
for l in sorted(links)[:20]:
    print(f'  {l}')

# Try login
time.sleep(2)
# Check login form action
forms = soup.find_all('form')
for f in forms:
    print(f'Form: action={f.get("action","")} method={f.get("method","")}')
    for inp in f.find_all(['input','button']):
        print(f'  {inp.name}: name={inp.get("name","")} type={inp.get("type","")} value={inp.get("value","")[:30]}')
