"""Check PLS login URL structure."""
import sys
import re
sys.stdout.reconfigure(encoding='utf-8')

from curl_cffi import requests as cf_requests
session = cf_requests.Session(impersonate='chrome')

r = session.get('https://www.pakistanlawsite.com', timeout=30)
print(f'Homepage: {r.status_code} ({len(r.text)} bytes)')

# Find all URLs
urls = set(re.findall(r'href=["\']([^"\']*)["\']', r.text))
login_urls = [u for u in urls if any(w in u.lower() for w in ['login', 'sign', 'account', 'auth'])]
print(f'\nLogin-related URLs ({len(login_urls)}):')
for u in sorted(login_urls):
    print(f'  {u}')

# Find forms
forms = re.findall(r'<form[^>]*action=["\']([^"\']*)["\']', r.text)
print(f'\nForm actions: {forms}')

# Check for token
if '__RequestVerificationToken' in r.text:
    idx = r.text.index('__RequestVerificationToken')
    print(f'\nCSRF token found on homepage!')
    # Extract it
    val_match = re.search(r'__RequestVerificationToken["\s]*value=["\']([^"\']+)', r.text[idx-10:idx+200])
    if val_match:
        print(f'  Token: {val_match.group(1)[:30]}...')
else:
    print('\nNo CSRF token on homepage')

# Check for login fields
if 'Username' in r.text:
    print('Username field found on homepage')
if 'Password' in r.text or 'password' in r.text:
    print('Password field found on homepage')

# Check what the scraper was using
print('\n--- Checking scraper login URL ---')
import os
scraper_path = 'legislation_scraper.py'
if os.path.exists(scraper_path):
    with open(scraper_path, 'r', encoding='utf-8') as f:
        content = f.read()
    login_matches = re.findall(r'["\']https?://.*?login.*?["\']', content, re.IGNORECASE)
    url_matches = re.findall(r'(https?://www\.pakistanlawsite\.com/[^\s"\']+)', content)
    print(f'  Login URLs in scraper: {login_matches[:5]}')
    print(f'  All PLS URLs in scraper: {url_matches[:10]}')
