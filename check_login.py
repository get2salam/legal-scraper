import os
import re
from dotenv import load_dotenv
load_dotenv()
from curl_cffi.requests import Session, BrowserType

BASE_URL = 'https://www.pakistanlawsite.com'

session = Session(impersonate=BrowserType.chrome120)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
})

# Get login page to check for CSRF tokens
login_page = session.get(f'{BASE_URL}/Login/Login', timeout=30)
print(f'Login page status: {login_page.status_code}')

# Look for form fields
inputs = re.findall(r'<input[^>]+>', login_page.text)
print('Form inputs found:')
for inp in inputs:
    name_match = re.search(r'name=["\']([^"\']+)["\']', inp)
    val_match = re.search(r'value=["\']([^"\']*)["\']', inp)
    type_match = re.search(r'type=["\']([^"\']+)["\']', inp)
    if name_match:
        print(f"  name={name_match.group(1)}, type={type_match.group(1) if type_match else 'text'}, value={val_match.group(1)[:50] if val_match else '(none)'}")

# Try login with CSRF token if found
token_match = re.search(r'__RequestVerificationToken[^>]+value=["\']([^"\']+)["\']', login_page.text)
if token_match:
    print(f'\nFound CSRF token: {token_match.group(1)[:20]}...')
else:
    print('\nNo CSRF token found in login page')

print(f'\nCookies after GET: {dict(session.cookies)}')
