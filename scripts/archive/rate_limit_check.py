#!/usr/bin/env python3
"""
Rate Limit Diagnostic for pakistanlawsite.com
Checks multiple indicators to detect rate limiting/blocking.
"""

import requests
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
from datetime import datetime

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
LOGIN_URL = f"{BASE_URL}/Login"
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

def check_rate_limits():
    print("=" * 60)
    print("PLS RATE LIMIT DIAGNOSTIC")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 60)
    
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    
    # 1. Check login page accessibility
    print("\n[1] CHECKING LOGIN PAGE ACCESS...")
    try:
        resp = session.get(LOGIN_URL, timeout=30)
        print(f"    Status: {resp.status_code}")
        print(f"    Response time: {resp.elapsed.total_seconds():.2f}s")
        
        # Check for rate limit headers
        print("\n[2] RATE LIMIT HEADERS...")
        rate_headers = ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset',
                       'Retry-After', 'X-Rate-Limit', 'RateLimit-Limit', 'RateLimit-Remaining']
        found_headers = False
        for header in rate_headers:
            if header.lower() in [h.lower() for h in resp.headers]:
                print(f"    {header}: {resp.headers.get(header)}")
                found_headers = True
        if not found_headers:
            print("    No standard rate limit headers found (common for legacy ASP.NET)")
        
        # Check all response headers
        print("\n[3] ALL RESPONSE HEADERS...")
        for k, v in resp.headers.items():
            print(f"    {k}: {v[:100]}..." if len(v) > 100 else f"    {k}: {v}")
        
        # Parse login page for messages
        print("\n[4] LOGIN PAGE ANALYSIS...")
        soup = BeautifulSoup(resp.text, 'lxml')
        
        # Check for error messages
        errors = soup.find_all(['div', 'span', 'p'], class_=lambda x: x and ('error' in x.lower() or 'alert' in x.lower() or 'warning' in x.lower()))
        if errors:
            print("    ⚠️ Error messages found:")
            for e in errors:
                print(f"       - {e.get_text(strip=True)[:200]}")
        else:
            print("    ✓ No error messages on login page")
        
        # Check for captcha
        captcha_indicators = ['captcha', 'recaptcha', 'hcaptcha', 'verify you are human', 'robot']
        page_text = resp.text.lower()
        captcha_found = [c for c in captcha_indicators if c in page_text]
        if captcha_found:
            print(f"    ⚠️ CAPTCHA indicators found: {captcha_found}")
        else:
            print("    ✓ No captcha detected")
        
        # Check for blocking messages
        block_indicators = ['blocked', 'too many requests', 'rate limit', 'try again later', 
                          'temporarily unavailable', 'access denied', 'forbidden']
        block_found = [b for b in block_indicators if b in page_text]
        if block_found:
            print(f"    ⚠️ BLOCKING indicators found: {block_found}")
        else:
            print("    ✓ No blocking messages detected")
        
        # Check verification token exists
        token_input = soup.find('input', {'name': '__RequestVerificationToken'})
        if token_input:
            print("    ✓ Verification token present (login form working)")
        else:
            print("    ⚠️ No verification token - login form may be blocked")
        
    except requests.exceptions.RequestException as e:
        print(f"    ❌ ERROR accessing login page: {e}")
        return
    
    # 5. Try actual login
    print("\n[5] ATTEMPTING LOGIN...")
    try:
        # Get fresh verification token
        resp = session.get(LOGIN_URL, timeout=30)
        soup = BeautifulSoup(resp.text, 'lxml')
        token_input = soup.find('input', {'name': '__RequestVerificationToken'})
        
        if not token_input:
            print("    ❌ Cannot get verification token")
            return
        
        verification_token = token_input.get('value')
        
        login_data = {
            "__RequestVerificationToken": verification_token,
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
        }
        
        resp = session.post(f"{BASE_URL}/Login/Login", data=login_data, timeout=30, allow_redirects=True)
        print(f"    Status: {resp.status_code}")
        print(f"    Final URL: {resp.url}")
        
        # Check cookies
        cookies = {c.name: c.value for c in session.cookies}
        print(f"    Cookies received: {list(cookies.keys())}")
        
        if 'ASP.NET_SessionId' in cookies:
            print("    ✓ Session cookie acquired - LOGIN SUCCESSFUL")
        else:
            print("    ⚠️ No session cookie - login may have failed")
        
        # Check response for username (indicates logged in)
        if PLS_USER and PLS_USER in resp.text:
            print("    ✓ Username appears in response - LOGGED IN")
        
        # Check for login error messages
        soup = BeautifulSoup(resp.text, 'lxml')
        validation_errors = soup.find_all('span', class_='field-validation-error')
        if validation_errors:
            print("    ⚠️ Login validation errors:")
            for e in validation_errors:
                print(f"       - {e.get_text(strip=True)}")
        
        # Check for account-specific messages
        account_messages = ['account locked', 'suspended', 'disabled', 'exceeded', 'limit reached']
        page_text = resp.text.lower()
        account_issues = [m for m in account_messages if m in page_text]
        if account_issues:
            print(f"    ⚠️ ACCOUNT ISSUES detected: {account_issues}")
        
    except requests.exceptions.RequestException as e:
        print(f"    ❌ Login request failed: {e}")
    
    # 6. Test an API endpoint (if logged in)
    print("\n[6] TESTING API ENDPOINT...")
    try:
        session.headers["X-Requested-With"] = "XMLHttpRequest"
        test_url = f"{BASE_URL}/Login/CitationSearch"
        test_data = {"book": "SCMR", "year": "2024", "page": "1"}
        
        resp = session.post(test_url, data=test_data, timeout=30)
        print(f"    Status: {resp.status_code}")
        print(f"    Response length: {len(resp.text)} chars")
        
        if resp.status_code == 429:
            print("    ❌ RATE LIMITED (429)")
            if 'Retry-After' in resp.headers:
                print(f"       Retry after: {resp.headers['Retry-After']}")
        elif resp.status_code == 403:
            print("    ❌ FORBIDDEN (403) - may be rate limited or session issue")
        elif resp.status_code == 200:
            print("    ✓ API responding normally")
            # Quick check if response has data
            if 'caseLawTable' in resp.text or 'caseType' in resp.text:
                print("    ✓ Response contains case data")
            else:
                print("    ⚠️ Response may be empty or error page")
                print(f"    Preview: {resp.text[:300]}...")
                
    except requests.exceptions.RequestException as e:
        print(f"    ❌ API request failed: {e}")
    
    print("\n" + "=" * 60)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    check_rate_limits()
