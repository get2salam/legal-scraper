#!/usr/bin/env python3
"""Debug API - try different endpoints."""

import os
import re
import time
from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"

def main():
    session = Session(impersonate=BrowserType.chrome120)
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",  # Important for AJAX
    })
    
    # Login
    print("1. Getting homepage...")
    resp = session.get(f'{BASE_URL}/')
    csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
    csrf_token = csrf_match.group(1) if csrf_match else ''
    
    print("\n2. Logging in...")
    time.sleep(1)
    login_resp = session.post(f'{BASE_URL}/Login/Login', data={
        'Login.UserName': os.getenv('PLS_USER'),
        'Login.Password': os.getenv('PLS_PASS'),
        '__RequestVerificationToken': csrf_token
    })
    
    # Navigate to Check page first (like browser does)
    time.sleep(1)
    check = session.get(f'{BASE_URL}/Login/Check')
    print(f"   Check page: {check.status_code}")
    
    # Now try the statute search with AJAX header
    time.sleep(1)
    print("\n3. Trying StatuecharSearch as AJAX...")
    resp = session.get(f'{BASE_URL}/Login/StatuecharSearch', 
                       params={'character': 'A'},
                       headers={'X-Requested-With': 'XMLHttpRequest'})
    print(f"   Status: {resp.status_code}")
    print(f"   Length: {len(resp.text)}")
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    print(f"   TR with caseType: {len(rows)}")
    
    rows2 = soup.find_all('tr', attrs={'casetypeid': True})
    print(f"   TR with casetypeid: {len(rows2)}")
    
    if len(rows2) > 0:
        print("   First 5 statutes:")
        for row in rows2[:5]:
            print(f"      - {row.get('casetypeid', '')[:60]}")
    
    # Also try POST method
    time.sleep(1)
    print("\n4. Trying StatuecharSearch as POST...")
    resp2 = session.post(f'{BASE_URL}/Login/StatuecharSearch', 
                        data={'char': 'A'},
                        headers={'X-Requested-With': 'XMLHttpRequest'})
    print(f"   Status: {resp2.status_code}")
    print(f"   Length: {len(resp2.text)}")
    
    soup2 = BeautifulSoup(resp2.text, 'html.parser')
    rows3 = soup2.find_all('tr', attrs={'casetypeid': True})
    print(f"   TR with casetypeid: {len(rows3)}")
    
    if len(rows3) > 0:
        print("   First 5 statutes:")
        for row in rows3[:5]:
            print(f"      - {row.get('casetypeid', '')[:60]}")

if __name__ == "__main__":
    main()
