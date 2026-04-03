"""
Random Legislation Verification — 1 per alphabet letter
Compares local scraped data against LIVE PLS API.
"""
import os
import json
import random
import time
import sys
from datetime import datetime

# Fix Windows console encoding
sys.stdout.reconfigure(encoding='utf-8')

# Add project to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

import requests
from curl_cffi import requests as cf_requests

PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")
BASE_URL = "https://www.pakistanlawsite.com"
BASE_DIR = "data_v2/legislation"


def login_pls():
    """Login to PLS and return session."""
    session = cf_requests.Session(impersonate="chrome")
    
    # Use the EXACT same login flow as legislation_scraper.py
    # Step 1: ClearLoginHistory (clears old sessions + logs in)
    try:
        clear_resp = session.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
        }, timeout=30)
        print(f"  ClearLoginHistory: {clear_resp.status_code}")
    except Exception as e:
        print(f"  ClearLoginHistory error: {e}")
    
    # Step 2: Check if logged in
    try:
        check = session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if check.status_code == 200 and len(check.text) > 5000:
            print("  [+] PLS login successful (via ClearLoginHistory)")
            return session
    except Exception as e:
        print(f"  Check error: {e}")
    
    # Step 3: Fallback — Login/Login
    try:
        resp = session.post(f"{BASE_URL}/Login/Login", data={
            "Login.UserName": PLS_USER,
            "Login.Password": PLS_PASS,
        }, timeout=30, allow_redirects=True)
        print(f"  Login/Login: {resp.status_code}")
    except Exception as e:
        print(f"  Login/Login error: {e}")
        return None
    
    # Verify
    try:
        check = session.get(f"{BASE_URL}/Login/Check", timeout=30)
        if check.status_code == 200 and len(check.text) > 5000:
            print("  [+] PLS login successful (via Login/Login)")
            return session
    except Exception as e:
        print(f"  Verify error: {e}")
    
    print("  [!] Login failed after both methods")
    return None


def fetch_statute_from_pls(session, title):
    """Search PLS for a statute by title and return basic info."""
    try:
        # Use the correct legislation search endpoint (post-Feb 2026 redesign)
        search_url = f"{BASE_URL}/Login/StatuecharSearch"
        params = {"caseName": title[:80], "PageNo": 1}
        resp = session.get(search_url, params=params, timeout=30)
        
        if resp.status_code != 200:
            return {"error": f"HTTP {resp.status_code}"}
        
        data = resp.text
        # Check if title appears in results
        title_lower = title.lower()[:50]
        found = title_lower in data.lower()
        
        return {
            "found_in_search": found,
            "response_length": len(data),
            "status": resp.status_code,
        }
    except Exception as e:
        return {"error": str(e)}


def fetch_statute_content(session, statute_id):
    """Fetch actual statute content from PLS."""
    try:
        url = f"{BASE_URL}/Login/GetLegislationFile"
        params = {"caseid": statute_id}
        resp = session.get(url, params=params, timeout=30)
        
        if resp.status_code == 200:
            try:
                content = json.loads(resp.text)
                return {"content": content, "length": len(resp.text)}
            except json.JSONDecodeError:
                return {"raw_length": len(resp.text), "is_html": "<html" in resp.text.lower()}
        return {"error": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def verify_local_file(fpath):
    """Read and validate a local legislation JSON file."""
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        title = data.get("title", "")
        sections = data.get("sections", [])
        full_text = data.get("full_text", "")
        statute_id = data.get("id", "") or data.get("case_id", "") or data.get("statute_id", "")
        judgment_raw = data.get("judgment_raw", "")
        
        has_content = bool(full_text) or (isinstance(sections, list) and len(sections) > 0)
        section_count = len(sections) if isinstance(sections, list) else 0
        
        # Check for sentinel values
        sentinel_count = 0
        if isinstance(sections, list):
            for s in sections:
                if isinstance(s, dict):
                    for v in s.values():
                        if str(v).strip() in ("-1", '"-1"'):
                            sentinel_count += 1
        
        return {
            "title": title,
            "statute_id": statute_id,
            "has_content": has_content,
            "section_count": section_count,
            "sentinel_count": sentinel_count,
            "full_text_len": len(full_text) if full_text else 0,
            "file_size": os.path.getsize(fpath),
            "judgment_raw_len": len(judgment_raw) if judgment_raw else 0,
            "fields": list(data.keys()),
        }
    except Exception as e:
        return {"error": str(e)}


def main():
    print("=" * 70)
    print("  LEGISLATION VERIFICATION — Random Sample (1 per Letter)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # Collect all letters with files
    letters = {}
    for d in sorted(os.listdir(BASE_DIR)):
        dp = os.path.join(BASE_DIR, d)
        if os.path.isdir(dp) and len(d) == 1 and d.isalpha():
            files = [f for f in os.listdir(dp) if f.endswith(".json")]
            if files:
                letters[d] = files
    
    print(f"  Letters with data: {len(letters)}")
    print(f"  Total files: {sum(len(v) for v in letters.values())}")
    print()
    
    # Login to PLS
    print("  Logging into PLS...")
    session = login_pls()
    if not session:
        print("  [!] FAILED to login. Running local-only verification.")
        pls_available = False
    else:
        pls_available = True
    
    print()
    print("-" * 70)
    
    results = []
    passed = 0
    warned = 0
    failed = 0
    
    for letter, files in sorted(letters.items()):
        # Pick random file
        chosen = random.choice(files)
        fpath = os.path.join(BASE_DIR, letter, chosen)
        
        print(f"\n  [{letter}] {chosen[:60]}")
        
        # Verify local
        local = verify_local_file(fpath)
        if "error" in local:
            print(f"      ❌ LOCAL ERROR: {local['error']}")
            failed += 1
            results.append({"letter": letter, "file": chosen, "status": "FAIL", "error": local["error"]})
            continue
        
        print(f"      Title: {local['title'][:55]}")
        print(f"      Sections: {local['section_count']} | Text: {local['full_text_len']} chars | Size: {local['file_size']} bytes")
        
        # Check content quality
        if local["has_content"] and local["sentinel_count"] == 0 and local["full_text_len"] > 100:
            status = "PASS"
            icon = "✅"
            passed += 1
        elif local["has_content"] and local["sentinel_count"] > 0:
            status = "WARN"
            icon = "⚠️"
            warned += 1
            print(f"      ⚠️  {local['sentinel_count']} sentinel (-1) sections")
        elif not local["has_content"] or local["full_text_len"] == 0:
            status = "WARN"
            icon = "⚠️"
            warned += 1
            print(f"      ⚠️  Metadata-only (no text content)")
        else:
            status = "PASS"
            icon = "✅"
            passed += 1
        
        # Verify against PLS if available
        pls_result = None
        if pls_available and local.get("title"):
            time.sleep(random.uniform(2, 5))  # Human-like delay
            pls_result = fetch_statute_from_pls(session, local["title"])
            
            if pls_result.get("error"):
                print(f"      🔍 PLS: Error — {pls_result['error']}")
            elif pls_result.get("found_in_search"):
                print(f"      🔍 PLS: Found ✅")
            else:
                print(f"      🔍 PLS: Not found in search (may use different title)")
        
        # Check readable HTML exists
        html_path = os.path.join("data_v2/html/legislation", letter, chosen.replace(".json", ".html"))
        html_exists = os.path.exists(html_path)
        if not html_exists:
            # Try alternate path
            html_path2 = os.path.join(BASE_DIR, "html", letter, chosen.replace(".json", ".html"))
            html_exists = os.path.exists(html_path2)
        
        print(f"      HTML: {'✅' if html_exists else '❌ Missing'} | {icon} {status}")
        
        results.append({
            "letter": letter,
            "file": chosen,
            "title": local["title"],
            "status": status,
            "section_count": local["section_count"],
            "full_text_len": local["full_text_len"],
            "file_size": local["file_size"],
            "sentinel_count": local["sentinel_count"],
            "html_exists": html_exists,
            "pls_check": pls_result,
        })
    
    # Summary
    print()
    print("=" * 70)
    print(f"  VERIFICATION SUMMARY")
    print("=" * 70)
    print(f"  Letters checked: {len(results)}")
    print(f"  ✅ Passed: {passed}")
    print(f"  ⚠️  Warnings: {warned}")
    print(f"  ❌ Failed: {failed}")
    print(f"  Score: {passed}/{len(results)} ({100*passed/max(len(results),1):.0f}%)")
    print()
    
    if warned > 0:
        print("  Warnings are expected — PLS has metadata-only statutes (no text digitized)")
    
    # Save report
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_checked": len(results),
        "passed": passed,
        "warned": warned,
        "failed": failed,
        "results": results,
    }
    report_path = "data_v2/legislation/verification_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    print(f"  Report saved: {report_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
