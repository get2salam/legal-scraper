"""Extract the exact GetCaseFile AJAX call and parameters from PLS JS."""
from dotenv import load_dotenv
import os, re, time
load_dotenv()
from curl_cffi import requests as curl_requests

BASE_URL = "https://www.pakistanlawsite.com"
username = os.getenv('PLS_USER')
password = os.getenv('PLS_PASS')

s = curl_requests.Session(impersonate='chrome')

# Login
r = s.get(f"{BASE_URL}/", timeout=15)
csrf_match = re.search(r'name="__RequestVerificationToken"[^>]*value="([^"]+)"', r.text)
csrf = csrf_match.group(1) if csrf_match else None
s.post(f"{BASE_URL}/Login/ClearLoginHistory", data={
    "Login.UserName": username, "Login.Password": password,
    "__RequestVerificationToken": csrf,
}, timeout=15)
time.sleep(2)
dash = s.get(f"{BASE_URL}/Login/Check", timeout=15)

# Get layoutScript.js - that's where GetCaseFile is called
js_resp = s.get(f"{BASE_URL}/Scripts/layoutScript.js", timeout=15)
js = js_resp.text

# Find the GetCaseFile context (50 lines around it)
lines = js.split('\n')
for i, line in enumerate(lines):
    if 'GetCaseFile' in line:
        start = max(0, i-15)
        end = min(len(lines), i+25)
        print(f"=== GetCaseFile context (lines {start}-{end}) ===")
        for j in range(start, end):
            print(f"  {j}: {lines[j]}")
        print()

# Also find citation search / case browse function
for keyword in ['GetCaseFile', 'sbn', 'citationSearch', 'loadCase', 'searchCase', 'getCitation']:
    for i, line in enumerate(lines):
        if keyword.lower() in line.lower() and 'GetCaseFile' not in line:
            context_start = max(0, i-3)
            context_end = min(len(lines), i+3)
            relevant = '\n'.join(f"  {j}: {lines[j]}" for j in range(context_start, context_end))
            print(f"--- '{keyword}' at line {i} ---")
            print(relevant)
            print()
            break

# Find the dashboard inline JS for GetCaseFile
dash_lines = dash.text.split('\n')
for i, line in enumerate(dash_lines):
    if 'GetCaseFile' in line:
        start = max(0, i-20)
        end = min(len(dash_lines), i+30)
        print(f"=== Dashboard GetCaseFile context (lines {start}-{end}) ===")
        for j in range(start, end):
            print(f"  {j}: {dash_lines[j]}")
        print()
