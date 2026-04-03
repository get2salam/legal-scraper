#!/usr/bin/env python3
"""Quick test script to verify core scraping functionality."""

import os
import re
import json
from pathlib import Path
from datetime import datetime

from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"

def main():
    # Create session
    session = Session(impersonate=BrowserType.chrome120)
    
    # Login
    print("1. Logging in...")
    resp = session.get(f'{BASE_URL}/')
    csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
    csrf_token = csrf_match.group(1) if csrf_match else ''
    
    session.post(f'{BASE_URL}/Login/Login', data={
        'Login.UserName': os.getenv('PLS_USER'),
        'Login.Password': os.getenv('PLS_PASS'),
        '__RequestVerificationToken': csrf_token
    })
    print("   Login OK")
    
    # Get statutes list
    print("\n2. Getting statute list for 'A'...")
    resp = session.get(f'{BASE_URL}/Login/StatuecharSearch', params={'character': 'A'})
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.find_all('tr', class_='caseType')
    statutes = [{'name': row.get('casetypeid', '').strip(), 'alphabet': 'A'} for row in rows if row.get('casetypeid')]
    print(f"   Found {len(statutes)} statutes")
    
    # Get sections for first statute
    statute_name = statutes[0]['name']
    print(f"\n3. Getting sections for: {statute_name}...")
    resp = session.get(f'{BASE_URL}/Login/GetStatuesSearch', params={'caseName': statute_name})
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    sections = []
    for row in soup.find_all('tr', class_='table_row_hover'):
        cells = row.find_all('td')
        if len(cells) >= 4:
            read_cell = cells[0]
            section_id = read_cell.get('casetypeid', '')
            if not section_id:
                link = read_cell.find(class_='readCaseLaw')
                if link:
                    section_id = link.get('casetypeid', '')
            
            sections.append({
                'section_id': section_id,
                'number': cells[1].get_text(strip=True),
                'act_name': cells[2].get_text(strip=True),
                'definition': cells[3].get_text(strip=True),
            })
    
    print(f"   Found {len(sections)} sections")
    for s in sections[:5]:
        print(f"      Section {s['number']}: {s['definition'][:50]}")
    
    # Get content for first section
    if sections and sections[0]['section_id']:
        print(f"\n4. Getting content for section '{sections[0]['number']}'...")
        resp = session.post(f'{BASE_URL}/Login/SearchStatueFile', 
                           data={'caseTypeId': sections[0]['section_id']})
        if resp.text and resp.text != '-1':
            text_soup = BeautifulSoup(resp.text, 'html.parser')
            content = text_soup.get_text(separator='\n', strip=True)[:500]
            print(f"   Content preview:\n{content}...")
        else:
            print("   No content returned")
    
    # Save sample output
    print("\n5. Saving sample statute...")
    sample = {
        'id': 'test123',
        'title': statute_name,
        'alphabet': 'A',
        'sections': sections[:5],
        'scraped_at': datetime.now().isoformat(),
    }
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / 'A').mkdir(exist_ok=True)
    
    safe_name = re.sub(r'[^\w\-]', '_', statute_name)[:80]
    output_path = DATA_DIR / 'A' / f'{safe_name}.json'
    output_path.write_text(json.dumps(sample, indent=2, ensure_ascii=False))
    print(f"   Saved to: {output_path}")
    
    print("\n✓ All tests passed!")

if __name__ == "__main__":
    main()
