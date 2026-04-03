import os
import re
from dotenv import load_dotenv
load_dotenv()
from curl_cffi.requests import Session, BrowserType

BASE_URL = 'https://www.pakistanlawsite.com'
PLS_USER = os.getenv('PLS_USER')
PLS_PASS = os.getenv('PLS_PASS')

session = Session(impersonate=BrowserType.chrome120)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
})

# Get home page with CSRF token
home = session.get(f'{BASE_URL}/', timeout=30)
token_match = re.search(r'<input name="__RequestVerificationToken"[^>]+value=["\']([^"\']+)["\']', home.text)
token = token_match.group(1) if token_match else ''
print(f'CSRF token: {token[:20]}...')

# Login POST
resp = session.post(
    f'{BASE_URL}/Login/Login',
    data={
        'Login.UserName': PLS_USER,
        'Login.Password': PLS_PASS,
        '__RequestVerificationToken': token,
    },
    headers={
        'Referer': f'{BASE_URL}/',
        'Origin': BASE_URL,
        'Content-Type': 'application/x-www-form-urlencoded',
        'X-Requested-With': 'XMLHttpRequest',  # Login form uses AJAX
    },
    timeout=30
)
print(f'Login POST status: {resp.status_code}')
print(f'Full response ({len(resp.text)} chars):')
print(resp.text[:2000])

# Look for clearLoginHistory button/form
clear_match = re.search(r'clearLoginHistory|ClearLoginHistory|clear_login', resp.text, re.IGNORECASE)
print(f'\nHas clearLoginHistory: {bool(clear_match)}')

# Look for any action URLs in the response
actions = re.findall(r'(?:href|action|url)\s*[=:]\s*["\']([^"\']*)["\']', resp.text, re.IGNORECASE)
print(f'\nURLs/actions in response: {actions[:10]}')
