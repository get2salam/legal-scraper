"""Debug: check what corrupt sections look like and test PLS directly."""
import json, os, re, time
from pathlib import Path
from curl_cffi.requests import Session, BrowserType
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://www.pakistanlawsite.com"
DATA_DIR = Path(__file__).parent / "data_v2" / "legislation" / "A"
PLS_USER = os.getenv("PLS_USER")
PLS_PASS = os.getenv("PLS_PASS")

# Find 5 corrupt section IDs  
samples = []
for f in sorted(DATA_DIR.glob("*.json"))[:50]:
    data = json.load(open(f, encoding='utf-8'))
    for s in data.get("sections", []):
        text = s.get("text", "").strip()
        if len(text) < 10 or '"-1"' in text or text == '-1':
            sid = s.get("section_id", "")
            if sid:
                samples.append({
                    "file": f.stem[:50],
                    "section_id": sid,
                    "number": s.get("number", "?"),
                    "stored_text": repr(text[:30]),
                })
                break
    if len(samples) >= 5:
        break

print("Corrupt sections to test:")
for s in samples:
    print(f"  {s['file'][:40]} | id={s['section_id']} | num={s['number']} | stored={s['stored_text']}")

# Also find 3 VALID sections for comparison
valid_samples = []
for f in sorted(DATA_DIR.glob("*.json"))[:50]:
    data = json.load(open(f, encoding='utf-8'))
    for s in data.get("sections", []):
        text = s.get("text", "").strip()
        if len(text) > 50:
            sid = s.get("section_id", "")
            if sid:
                valid_samples.append({
                    "file": f.stem[:50],
                    "section_id": sid,
                    "number": s.get("number", "?"),
                })
                break
    if len(valid_samples) >= 3:
        break

print(f"\nValid sections for comparison:")
for s in valid_samples:
    print(f"  {s['file'][:40]} | id={s['section_id']} | num={s['number']}")

# Login to PLS
print("\nLogging in...")
session = Session(impersonate=BrowserType.chrome120)
session.headers.update({
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
})

resp = session.get(f"{BASE_URL}/", timeout=30)
csrf = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
time.sleep(2)

login_resp = session.post(f"{BASE_URL}/Login/Login", data={
    "Login.UserName": PLS_USER,
    "Login.Password": PLS_PASS,
    "__RequestVerificationToken": csrf.group(1)
}, timeout=30)

time.sleep(2)
check = session.get(f"{BASE_URL}/Login/Check", timeout=30)
if "pakistanlaws" in check.text.lower():
    print("[OK] Logged in")
else:
    print("FAIL: Login failed")
    exit(1)

# Test valid sections first
print("\n--- Testing VALID sections ---")
for s in valid_samples:
    time.sleep(2)
    resp = session.post(f"{BASE_URL}/Login/SearchStatueFile",
                       data={"caseTypeId": s["section_id"]}, timeout=30)
    raw = resp.text.strip()
    is_neg1 = raw in ["-1", '"-1"', '"-1', '-1"', ""] or len(raw) < 10
    print(f"  id={s['section_id']} | status={resp.status_code} | len={len(raw)} | is_neg1={is_neg1} | preview={repr(raw[:80])}")

# Test corrupt sections
print("\n--- Testing CORRUPT sections ---")
for s in samples:
    time.sleep(2)
    resp = session.post(f"{BASE_URL}/Login/SearchStatueFile",
                       data={"caseTypeId": s["section_id"]}, timeout=30)
    raw = resp.text.strip()
    is_neg1 = raw in ["-1", '"-1"', '"-1', '-1"', ""] or len(raw) < 10
    print(f"  id={s['section_id']} | status={resp.status_code} | len={len(raw)} | is_neg1={is_neg1} | preview={repr(raw[:80])}")

# Test with different request format
print("\n--- Testing corrupt with GET instead of POST ---")
for s in samples[:2]:
    time.sleep(2)
    resp = session.get(f"{BASE_URL}/Login/SearchStatueFile?caseTypeId={s['section_id']}", timeout=30)
    raw = resp.text.strip()
    print(f"  GET id={s['section_id']} | status={resp.status_code} | len={len(raw)} | preview={repr(raw[:80])}")
