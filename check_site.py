import os
import re
from dotenv import load_dotenv
load_dotenv()
from curl_cffi.requests import Session, BrowserType

BASE_URL = 'https://www.pakistanlawsite.com'

session = Session(impersonate=BrowserType.chrome120)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.pakistanlawsite.com/',
})

# Check home page
home = session.get(f'{BASE_URL}/', timeout=30)
print(f'Home status: {home.status_code}')
print(f'Home URL: {home.url}')

# Look for login form on home page
links = re.findall(r'href=["\']([^"\']*login[^"\']*)["\']', home.text, re.IGNORECASE)
forms = re.findall(r'action=["\']([^"\']*)["\']', home.text, re.IGNORECASE)
print(f'\nLogin links on homepage: {links[:5]}')
print(f'Form actions: {forms[:5]}')

# Check common login URLs
for url in ['/Login', '/login', '/Account/Login', '/User/Login', '/Home/Login']:
    try:
        r = session.get(f'{BASE_URL}{url}', timeout=15, allow_redirects=True)
        print(f'{url}: status={r.status_code}, final_url={r.url}')
    except Exception as e:
        print(f'{url}: ERROR {e}')
