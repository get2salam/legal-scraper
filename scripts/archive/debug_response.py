#!/usr/bin/env python3
"""Debug: Check actual response format from CitationSearch"""

import os
import re
import time
from curl_cffi.requests import Session, BrowserType
from dotenv import load_dotenv

load_dotenv()

BASE = "https://www.pakistanlawsite.com"

# Create session
session = Session(impersonate=BrowserType.chrome120)

# Login
print("Logging in...")
resp = session.get(f"{BASE}/", timeout=30)
csrf = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', resp.text)
csrf_token = csrf.group(1) if csrf else ""

time.sleep(3)

session.post(f"{BASE}/Login/Login", data={
    "Login.UserName": os.getenv("PLS_USER"),
    "Login.Password": os.getenv("PLS_PASS"),
    "__RequestVerificationToken": csrf_token
}, timeout=30)

time.sleep(2)
session.get(f"{BASE}/Login/Check", timeout=30)
print("Logged in!")

time.sleep(3)

# Citation search
print("\nFetching CitationSearch response...")
resp = session.post(f"{BASE}/Login/CitationSearch", data={
    "year": 2024,
    "book": "SCMR",
    "code": "",
    "court": "",
    "judge": "",
    "lawyer": "",
    "party": "",
}, timeout=30)

print(f"Status: {resp.status_code}")
print(f"Length: {len(resp.text)}")

# Save full response for inspection
with open("debug_citation_response.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
print("Saved to debug_citation_response.html")

# Look for patterns
print("\n=== Pattern Analysis ===")
print(f"Contains 'caseLawTable': {'caseLawTable' in resp.text}")
print(f"Contains 'SCMR': {resp.text.count('SCMR')} occurrences")
print(f"Contains 'onclick': {resp.text.count('onclick')} occurrences")
print(f"Contains 'GetCaseFile': {'GetCaseFile' in resp.text}")

# Find citation patterns
citations = re.findall(r'\d{4}\s+SCMR\s+\d+', resp.text)
print(f"Regex matches (YYYY SCMR N): {len(citations)}")
if citations:
    print(f"  First 5: {citations[:5]}")

# Find onclick handlers
onclicks = re.findall(r"onclick=['\"]([^'\"]+)['\"]", resp.text)
print(f"\nonclick handlers: {len(onclicks)}")
if onclicks:
    print(f"  First: {onclicks[0][:100]}")

# Show sample HTML
print("\n=== First 3000 chars of response ===")
print(resp.text[:3000])
