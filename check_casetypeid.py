"""
Step 2+3: Check what casetypeid points to and explore W&P detail endpoints.
Uses ONE PLS session only.
"""
import os, sys, time, json
from pathlib import Path
from dotenv import load_dotenv
load_dotenv('.env')
from curl_cffi import requests as r
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

s = r.Session()
s.impersonate = "chrome"
B = "https://www.pakistanlawsite.com"
u = os.getenv("PLS_USER", "")
p = os.getenv("PLS_PASS", "")

print(f"Logging in as {u}...")
resp = s.post(f"{B}/Login/ClearLoginHistory",
              data={"Login.UserName": u, "Login.Password": p}, timeout=30)
print(f"Login: {resp.status_code}")
time.sleep(3)

# Load the known W&P list with casetypeids
with open('data_v2/pls_extras/words_phrases/words_phrases_all.json', encoding='utf-8') as f:
    wp_all = json.load(f)

print(f"\nTotal W&P entries with casetypeid: {len(wp_all)}")
print("First 5:")
for item in wp_all[:5]:
    print(f"  {item}")

# Fetch first entry via GetCaseFile
cid = wp_all[0]['casetypeid']
name = wp_all[0]['name']
print(f"\nFetching GetCaseFile for casetypeid={cid} name={name}...")
time.sleep(2)
resp = s.post(f"{B}/Login/GetCaseFile",
              data={"caseName": cid, "headNotes": 0}, timeout=20)
print(f"Status: {resp.status_code}, Size: {len(resp.text)} chars")
try:
    parsed = json.loads(resp.text)
    if isinstance(parsed, list):
        print(f"Response is JSON list with {len(parsed)} items")
        for item in parsed[:2]:
            if isinstance(item, dict):
                print(f"  Keys: {list(item.keys())}")
                for k, v in item.items():
                    print(f"    {k}: {str(v)[:200]}")
    elif isinstance(parsed, dict):
        print(f"Response is JSON dict, keys: {list(parsed.keys())}")
except:
    print(f"Raw text (first 500): {resp.text[:500]}")

# Also try W&P detail endpoints
print("\n=== Testing W&P detail endpoints ===")
for ep in ["/Login/GetWordPhrase", "/Login/WordDetail", "/Login/GetWordsAndPhrases",
           "/Login/WordsAndPhrasesDetail", "/Login/GetPhrase", "/Login/GetLegalTerm"]:
    time.sleep(1)
    try:
        resp2 = s.post(f"{B}{ep}", data={"id": "1", "wordid": "1", "casetypeid": cid}, timeout=8)
        print(f"{ep}: {resp2.status_code} ({len(resp2.text)} chars) | {resp2.text[:100]}")
    except Exception as e:
        print(f"{ep}: ERROR {e}")

# Try GET endpoint for word details
time.sleep(1)
try:
    resp3 = s.get(f"{B}/Login/WordsAndPhrases",
                  params={"id": cid, "wordid": cid, "type": "words"}, timeout=8)
    print(f"GET /Login/WordsAndPhrases: {resp3.status_code} ({len(resp3.text)} chars)")
    if len(resp3.text) > 200:
        print(f"  First 300: {resp3.text[:300]}")
except Exception as e:
    print(f"GET /Login/WordsAndPhrases: ERROR {e}")

print("\nDone.")
