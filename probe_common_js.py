"""
Fetch and analyze common.js which likely contains the search logic
"""
import os, sys, re, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from dotenv import load_dotenv
load_dotenv('.env')
from curl_cffi.requests import Session, BrowserType

BASE_URL = 'https://www.pakistanlawsite.com'
PLS_USER = os.getenv('PLS_USER', '')
PLS_PASS = os.getenv('PLS_PASS', '')

s = Session(impersonate=BrowserType.chrome120)
s.headers.update({
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Upgrade-Insecure-Requests': '1',
})

# Login first
resp0 = s.get(f'{BASE_URL}/', timeout=30)
csrf = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp0.text)
s.post(f'{BASE_URL}/Login/ClearLoginHistory', data={
    'Login.UserName': PLS_USER, 'Login.Password': PLS_PASS,
    '__RequestVerificationToken': csrf.group(1)
}, timeout=30)
time.sleep(3)
check = s.get(f'{BASE_URL}/Login/Check', timeout=30)
print(f'Login: {"OK" if "Logout" in check.text else "FAILED"}')

# Fetch common.js
time.sleep(2)
resp_js = s.get(f'{BASE_URL}/Scripts/common.js', timeout=30)
print(f'common.js: {resp_js.status_code}, {len(resp_js.text)} chars')

if resp_js.status_code == 200:
    with open('common.js', 'w', encoding='utf-8') as f:
        f.write(resp_js.text)
    print('Saved common.js')
    
    # Find all AJAX calls
    ajax_calls = re.findall(r"""url:\s*['"](\/[^'"]+)['"]""", resp_js.text)
    print(f'\nAll API endpoints in common.js:')
    for url in sorted(set(ajax_calls)):
        print(f'  {url}')
    
    # Look for search endpoints
    search_patterns = re.findall(r'[/]Login[/][A-Za-z]+[^\n]{0,200}', resp_js.text)
    print(f'\nLogin endpoints usage:')
    for p in search_patterns[:20]:
        print(f'  {p.strip()[:150]}')

    # Find function that handles keyword search
    kw_func = re.search(r'(?:Keyword|keyword)[^\n]{0,300}', resp_js.text)
    if kw_func:
        print(f'\nKeyword usage: {kw_func.group(0)[:300]}')
    
    # Find all function definitions
    funcs = re.findall(r'function\s+(\w+)\s*\([^)]*\)', resp_js.text)
    print(f'\nFunctions: {funcs[:30]}')

time.sleep(2)

# Also fetch layoutScript.js
resp_layout = s.get(f'{BASE_URL}/Scripts/layoutScript.js', timeout=30)
print(f'\nlayoutScript.js: {resp_layout.status_code}, {len(resp_layout.text)} chars')
if resp_layout.status_code == 200:
    with open('layoutScript.js', 'w', encoding='utf-8') as f:
        f.write(resp_layout.text)
    print('Saved layoutScript.js')
    
    ajax_calls2 = re.findall(r"""url:\s*['"](\/[^'"]+)['"]""", resp_layout.text)
    print(f'API endpoints in layoutScript.js:')
    for url in sorted(set(ajax_calls2)):
        print(f'  {url}')
    
    # Look for the main search function
    search_func = re.search(r'(?:citationSearch|Citation_Search|caseLaw|CaseLaw)[^\n]{0,500}', resp_layout.text)
    if search_func:
        print(f'\nSearch function: {search_func.group(0)[:300]}')
