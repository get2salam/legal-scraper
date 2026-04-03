import os, sys, time, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from dotenv import load_dotenv
load_dotenv('.env')
from curl_cffi import requests as r

s = r.Session(); s.impersonate='chrome'
B = 'https://www.pakistanlawsite.com'
u = os.getenv('PLS_USER',''); p = os.getenv('PLS_PASS','')

print(f'User: {u[:3]}***')

# First get the login page
resp0 = s.get(f'{B}/Login', timeout=30)
print(f'GET Login page: {resp0.status_code}')
hidden = re.findall(r'<input[^>]+type=["\']hidden["\'][^>]*>', resp0.text)
for h in hidden:
    print(f'  Hidden: {h}')

time.sleep(3)

# Login
resp2 = s.post(f'{B}/Login/Login',
    data={'Login.UserName': u, 'Login.Password': p, 'Login.RememberMe': 'false'},
    headers={'Referer': f'{B}/Login', 'Content-Type': 'application/x-www-form-urlencoded'},
    timeout=30)
print(f'Login POST: {resp2.status_code}, {len(resp2.text)} chars')
print(f'URL after login: {resp2.url}')
print(f'Cookies: {dict(s.cookies)}')

# Check if we're logged in by hitting a protected page
time.sleep(3)
resp3 = s.get(f'{B}/Login/Home', timeout=30)
print(f'Home: {resp3.status_code}, URL: {resp3.url}')
print(resp3.text[:500])
