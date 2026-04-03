"""
Data Quality Re-fetch Script
1. Re-fetch corrupt/empty case law files
2. Re-fetch PLS sentinel (-1) legislation files
"""
import os
import sys
import json
import time
import random
import re
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from curl_cffi import requests as cf_requests

PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")
BASE_URL = "https://www.pakistanlawsite.com"


def login_pls():
    """Login using the same flow as legislation_scraper.py."""
    session = cf_requests.Session(impersonate="chrome")
    try:
        session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
        }, timeout=30)
    except Exception as e:
        print(f"  ClearLoginHistory error: {e}")

    try:
        check = session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if check.status_code == 200 and len(check.text) > 5000:
            print("  [+] PLS login successful")
            return session
    except:
        pass

    # Fallback
    try:
        session.post(f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
        }, timeout=30, allow_redirects=True)
        check = session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if check.status_code == 200 and len(check.text) > 5000:
            print("  [+] PLS login successful (fallback)")
            return session
    except:
        pass

    print("  [!] Login failed")
    return None


def refetch_case_law(session, citation):
    """Re-fetch a single case law file from PLS."""
    try:
        resp = session.post(f"{BASE_URL}/Login/GetCaseFile", data={
            "caseName": citation,
        }, timeout=60)
        if resp.status_code == 200 and len(resp.text) > 100:
            return resp.text
        return None
    except Exception as e:
        print(f"    Error fetching {citation}: {e}")
        return None


def refetch_statute(session, title):
    """Re-fetch a statute from PLS by searching and fetching."""
    try:
        # Search for the statute
        resp = session.get(f"{BASE_URL}/Login/StatuecharSearch", params={
            "caseName": title[:80],
            "PageNo": 1,
        }, timeout=30)
        if resp.status_code != 200:
            return None
        
        # Try to find the statute ID in the search results
        html = resp.text
        # Look for GetStatuesSearch links
        matches = re.findall(r'GetStatuesSearch\?caseName=([^"&\']+)', html)
        if not matches:
            matches = re.findall(r'caseName=([^"&\']+)', html)
        
        if not matches:
            return None
        
        # Fetch the statute content
        statute_name = matches[0]
        resp2 = session.get(f"{BASE_URL}/Login/GetStatuesSearch", params={
            "caseName": statute_name,
        }, timeout=60)
        
        if resp2.status_code == 200 and len(resp2.text) > 200:
            return resp2.text
        return None
    except Exception as e:
        print(f"    Error: {e}")
        return None


def find_empty_case_files():
    """Find empty or corrupt case law JSON files."""
    empty = []
    base = "data_v2"
    reporters = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "CLD", "YLR", "GBLR"]
    for reporter in reporters:
        rdir = os.path.join(base, reporter)
        if not os.path.isdir(rdir):
            continue
        for year in os.listdir(rdir):
            ydir = os.path.join(rdir, year)
            if not os.path.isdir(ydir) or not year.isdigit():
                continue
            for fname in os.listdir(ydir):
                if not fname.endswith(".json"):
                    continue
                fpath = os.path.join(ydir, fname)
                size = os.path.getsize(fpath)
                if size == 0:
                    citation = fname.replace(".json", "").replace("_", " ")
                    empty.append({"path": fpath, "citation": citation, "size": size})
                elif size < 50:
                    # Might be corrupt
                    try:
                        with open(fpath, "r", encoding="utf-8") as f:
                            data = json.load(f)
                    except:
                        citation = fname.replace(".json", "").replace("_", " ")
                        empty.append({"path": fpath, "citation": citation, "size": size})
    return empty


def find_sentinel_legislation():
    """Find legislation files with PLS sentinel (-1) values."""
    sentinel_files = []
    base = "data_v2/legislation"
    for letter in sorted(os.listdir(base)):
        lp = os.path.join(base, letter)
        if not os.path.isdir(lp) or len(letter) != 1 or not letter.isalpha():
            continue
        for fname in os.listdir(lp):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(lp, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                sections = data.get("sections", [])
                if isinstance(sections, list):
                    for s in sections:
                        if isinstance(s, dict):
                            for v in s.values():
                                if str(v).strip() in ("-1", '"-1"'):
                                    sentinel_files.append({
                                        "path": fpath,
                                        "letter": letter,
                                        "title": data.get("title", ""),
                                    })
                                    break
                            else:
                                continue
                            break
            except:
                pass
    return sentinel_files


def main():
    print("=" * 70)
    print("  DATA QUALITY RE-FETCH")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Phase 1: Find empty/corrupt case law files
    print("\n[Phase 1] Scanning for empty/corrupt case law files...")
    empty_cases = find_empty_case_files()
    print(f"  Found {len(empty_cases)} empty/corrupt case law files")
    for e in empty_cases[:10]:
        print(f"    {e['path']} ({e['size']} bytes)")

    # Phase 2: Find sentinel legislation files
    print("\n[Phase 2] Scanning for PLS sentinel (-1) legislation files...")
    sentinel_leg = find_sentinel_legislation()
    print(f"  Found {len(sentinel_leg)} sentinel legislation files")

    # Phase 3: Login and re-fetch
    print("\n[Phase 3] Logging into PLS...")
    session = login_pls()
    if not session:
        print("  Cannot proceed without PLS session")
        return

    # Re-fetch empty case law files
    if empty_cases:
        print(f"\n[Phase 4] Re-fetching {len(empty_cases)} empty case law files...")
        fixed = 0
        for i, case in enumerate(empty_cases):
            citation = case["citation"]
            print(f"  [{i+1}/{len(empty_cases)}] {citation}...", end=" ")
            
            content = refetch_case_law(session, citation)
            if content and len(content) > 200:
                # Parse and save
                try:
                    data = json.loads(content)
                    with open(case["path"], "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    print(f"FIXED ({len(content)} bytes)")
                    fixed += 1
                except json.JSONDecodeError:
                    # Save as raw
                    with open(case["path"], "w", encoding="utf-8") as f:
                        f.write(content)
                    print(f"SAVED RAW ({len(content)} bytes)")
                    fixed += 1
            else:
                print("SKIP (no data from PLS)")
            
            time.sleep(random.uniform(2, 5))
        
        print(f"  Fixed: {fixed}/{len(empty_cases)} case law files")

    # Re-fetch sentinel legislation (sample — these are usually unfixable)
    if sentinel_leg:
        print(f"\n[Phase 5] Testing sentinel legislation re-fetch (sample of 5)...")
        sample = random.sample(sentinel_leg, min(5, len(sentinel_leg)))
        refetched = 0
        for i, sl in enumerate(sample):
            title = sl["title"]
            print(f"  [{i+1}/5] {title[:60]}...", end=" ")
            
            content = refetch_statute(session, title)
            if content and len(content) > 500:
                # Check if the re-fetched content still has -1
                if '"-1"' in content or "sentinel" in content.lower():
                    print("STILL SENTINEL (PLS limitation)")
                else:
                    print(f"NEW DATA ({len(content)} bytes)")
                    refetched += 1
            else:
                print("NO DATA")
            
            time.sleep(random.uniform(3, 6))
        
        print(f"  Re-fetchable: {refetched}/5 sentinel files")
        if refetched == 0:
            print("  → Sentinel files are a PLS limitation — cannot fix from our end")

    # Summary
    print("\n" + "=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print(f"  Empty case law files: {len(empty_cases)}")
    print(f"  Sentinel legislation: {len(sentinel_leg)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
