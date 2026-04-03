#!/usr/bin/env python3
"""Fix a single corrupted case by re-scraping it."""

import os
import sys
import json
import time
import random
import re
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi.requests import Session, BrowserType
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path("data_v2")


def create_session():
    """Create a session with Chrome TLS fingerprint (same as main scraper)."""
    session = Session(impersonate=BrowserType.chrome120)
    session.headers.update({
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })
    return session


def login(session):
    """Login to PLS using CSRF token (same as main scraper)."""
    username = os.getenv("PLS_USER")
    password = os.getenv("PLS_PASS")
    
    if not username or not password:
        print("Error: PLS_USER and PLS_PASS must be set in .env")
        return False
    
    print("Getting homepage for CSRF token...")
    
    # Get homepage for CSRF token
    try:
        resp = session.get(f"{BASE_URL}/", timeout=30)
        if resp.status_code != 200:
            print(f"[FAIL] Homepage returned status {resp.status_code}")
            return False
    except Exception as e:
        print(f"[FAIL] Failed to load homepage: {e}")
        return False
    
    time.sleep(random.uniform(2, 4))
    
    # Extract CSRF token
    csrf_match = re.search(
        r'name="__RequestVerificationToken"[^>]*value="([^"]+)"',
        resp.text
    )
    if not csrf_match:
        print("[FAIL] CSRF token not found")
        return False
    
    csrf_token = csrf_match.group(1)
    print(f"CSRF token: {csrf_token[:40]}...")
    
    time.sleep(random.uniform(2, 4))
    
    # Submit login form (field names from main scraper)
    print("Submitting login form...")
    try:
        login_resp = session.post(f"{BASE_URL}/Login/Login", data={
            "Login.UserName": username,
            "Login.Password": password,
            "__RequestVerificationToken": csrf_token
        }, timeout=30)
    except Exception as e:
        print(f"[FAIL] Login request failed: {e}")
        return False
    
    time.sleep(random.uniform(2, 3))
    
    # Verify login by checking session
    try:
        check_resp = session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if "Logout" not in check_resp.text:
            print("[FAIL] Login verification failed")
            return False
    except Exception as e:
        print(f"[FAIL] Login check failed: {e}")
        return False
    
    print("[OK] Login successful!")
    return True


def parse_case_content(html, citation, case_name):
    """Parse case content from HTML (same as main scraper)."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Extract title
    title = ""
    title_elem = soup.find(["h1", "h2", "h3"], class_=re.compile(r"title|heading", re.I))
    if title_elem:
        title = title_elem.get_text(strip=True)
    
    # Extract court
    court = ""
    court_match = re.search(r"(Supreme Court|High Court|Federal Shariat|Tribunal)[^<]*", html, re.I)
    if court_match:
        court = court_match.group(0).strip()
    
    # Extract date
    date = ""
    date_match = re.search(r"(\d{1,2}(?:st|nd|rd|th)?\s+\w+,?\s+\d{4})", html)
    if date_match:
        date = date_match.group(1)
    
    # Extract judges
    judges = []
    judge_section = soup.find(string=re.compile(r"Before|Coram|JUDGE", re.I))
    if judge_section:
        parent = judge_section.find_parent()
        if parent:
            judges_text = parent.get_text(separator=" ", strip=True)
            judge_names = re.findall(r"(?:Mr\.|Mrs\.|Justice|J\.)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)", judges_text)
            judges = list(set(judge_names))
    
    # Extract headnotes
    headnotes = ""
    headnote_div = soup.find("div", class_=re.compile(r"headnote", re.I))
    if headnote_div:
        headnotes = headnote_div.get_text(separator="\n", strip=True)
    
    # Extract judgment (main content)
    judgment = ""
    judgment_div = soup.find("div", class_=re.compile(r"judgment|content", re.I))
    if judgment_div:
        judgment = judgment_div.get_text(separator="\n", strip=True)
    else:
        # Fallback: get all text
        for script in soup(["script", "style", "nav", "header", "footer"]):
            script.decompose()
        judgment = soup.get_text(separator="\n", strip=True)
    
    # Extract cited statutes
    statutes_cited = []
    statute_patterns = [
        r"([A-Z][a-z]+(?:\s+[A-Z]?[a-z]+)*\s+(?:Act|Ordinance|Rules?|Regulation|Order|Code)(?:\s*,\s*|\s+)(?:of\s+)?\d{4})",
        r"((?:Act|Ordinance|Rules?|Regulation|Order|Code)(?:\s*,\s*|\s+)(?:of\s+)?\d{4})",
    ]
    for pattern in statute_patterns:
        matches = re.findall(pattern, judgment, re.IGNORECASE)
        statutes_cited.extend(matches)
    statutes_cited = list(set(statutes_cited))[:50]
    
    # Extract cited cases
    cases_cited = []
    case_pattern = r"\d{4}\s+(?:PLD|SCMR|CLC|MLD|YLR|PCrLJ|PTD|PLC|CLD|GBLR)\s+\d+"
    cases_cited = list(set(re.findall(case_pattern, judgment)))
    
    return {
        "citation": citation,
        "case_name": case_name,
        "title": title,
        "court": court,
        "date": date,
        "judges": judges,
        "headnotes": headnotes,
        "judgment": judgment,
        "judgment_raw": html,
        "statutes_cited": statutes_cited,
        "cases_cited": cases_cited,
        "fetched_at": datetime.now().isoformat(),
    }


def get_case_id_from_existing(reporter, year, page):
    """Get the casetypeid from the existing corrupted JSON file."""
    filepath = DATA_DIR / reporter / str(year) / f"{year}_{reporter}_{page}.json"
    if filepath.exists():
        # Try different encodings
        for encoding in ["utf-8", "latin-1", "cp1252"]:
            try:
                data = json.loads(filepath.read_bytes().decode(encoding))
                case_name = data.get("case_name", "")
                if case_name:
                    return case_name
            except:
                pass
    return None


def fix_case(reporter, year, page, case_id=None):
    """Fix a single case by re-fetching with the correct endpoint."""
    citation = f"{year} {reporter} {page}"
    print(f"\nFixing: {citation}")
    
    # Get case_id from existing file if not provided
    if not case_id:
        case_id = get_case_id_from_existing(reporter, year, page)
        if case_id:
            print(f"Found casetypeid from existing file: {case_id}")
        else:
            print("[FAIL] No casetypeid found in existing file")
            return False
    
    session = create_session()
    
    if not login(session):
        print("Login failed, aborting")
        return False
    
    # Random delay to be human-like
    time.sleep(random.uniform(3, 5))
    
    # Fetch using GetCaseFile endpoint (same as main scraper)
    print(f"Fetching case content using GetCaseFile endpoint...")
    print(f"caseName: {case_id}")
    
    try:
        resp = session.post(f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": case_id,
            "headNotes": 0,
        }, timeout=30)
    except Exception as e:
        print(f"[FAIL] Request failed: {e}")
        return False
    
    print(f"Response status: {resp.status_code}")
    print(f"Response length: {len(resp.text)} chars")
    
    # Check for empty or error response
    if resp.text.strip() in ["1", '"1"', ""] or len(resp.text) < 500:
        print(f"[FAIL] Empty or error response")
        print(f"Response: {resp.text[:200]}")
        return False
    
    # Parse the content
    case_data = parse_case_content(resp.text, citation, case_id)
    
    print(f"Title: {case_data['title'][:80]}..." if case_data['title'] else "Title: (empty)")
    print(f"Court: {case_data['court']}")
    print(f"Judgment length: {len(case_data['judgment'])} chars")
    
    if len(case_data["judgment"]) < 500:
        print(f"[WARN] Short judgment, but saving anyway")
    
    # Save to correct location
    save_dir = DATA_DIR / reporter / str(year)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"{year}_{reporter}_{page}.json"
    filepath = save_dir / filename
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(case_data, f, ensure_ascii=False, indent=2)
    
    print(f"[OK] Saved: {filepath}")
    
    # Also save original HTML
    original_dir = save_dir / "original"
    original_dir.mkdir(parents=True, exist_ok=True)
    html_file = original_dir / f"{year}_{reporter}_{page}.html"
    html_file.write_text(resp.text, encoding="utf-8")
    print(f"[OK] Original HTML saved: {html_file}")
    
    return True


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python fix_case.py <reporter> <year> <page> [casetypeid]")
        print("Example: python fix_case.py PTD 2024 889")
        print("Example with ID: python fix_case.py PTD 2024 889 2024L6028")
        sys.exit(1)
    
    reporter = sys.argv[1].upper()
    year = int(sys.argv[2])
    page = int(sys.argv[3])
    case_id = sys.argv[4] if len(sys.argv) > 4 else None
    
    success = fix_case(reporter, year, page, case_id)
    sys.exit(0 if success else 1)
