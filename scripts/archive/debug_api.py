#!/usr/bin/env python3
"""Debug API responses."""

import os
import re
from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"

def main():
    session = Session(impersonate=BrowserType.chrome120)
    
    # Login
    print("1. Getting homepage...")
    resp = session.get(f'{BASE_URL}/')
    csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
    csrf_token = csrf_match.group(1) if csrf_match else ''
    print(f"   CSRF: {csrf_token[:40]}...")
    
    print("\n2. Logging in...")
    login_resp = session.post(f'{BASE_URL}/Login/Login', data={
        'Login.UserName': os.getenv('PLS_USER'),
        'Login.Password': os.getenv('PLS_PASS'),
        '__RequestVerificationToken': csrf_token
    })
    print(f"   Login status: {login_resp.status_code}")
    
    # Verify login
    check_resp = session.get(f'{BASE_URL}/Login/Check')
    logged_in = 'pakistanlaws' in check_resp.text.lower()
    print(f"   Logged in: {logged_in}")
    
    # Get statute list
    print("\n3. Getting statute list...")
    resp = session.get(f'{BASE_URL}/Login/StatuecharSearch', params={'character': 'A'})
    print(f"   Status: {resp.status_code}")
    print(f"   Length: {len(resp.text)}")
    
    # Save response for debugging
    with open('debug_statuechar.html', 'w', encoding='utf-8') as f:
        f.write(resp.text)
    print("   Saved to debug_statuechar.html")
    
    # Parse and count
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Find all tr elements
    all_tr = soup.find_all('tr')
    print(f"\n   All TR elements: {len(all_tr)}")
    
    # Find tr with caseType class
    casetype_tr = soup.find_all('tr', class_='caseType')
    print(f"   TR with class='caseType': {len(casetype_tr)}")
    
    # Find tr with casetypeid attribute
    caseid_tr = soup.find_all('tr', attrs={'casetypeid': True})
    print(f"   TR with casetypeid attr: {len(caseid_tr)}")
    
    # Print first few tr samples
    print("\n   Sample TR elements:")
    for tr in all_tr[:5]:
        print(f"      class={tr.get('class')}, casetypeid={tr.get('casetypeid', '')[:30]}")

if __name__ == "__main__":
    main()
