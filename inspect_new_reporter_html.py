"""Inspect the HTML structure PLS returns for new reporters."""
import sys, os, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
try:
    from curl_cffi import requests as r
    s = r.Session()
    s.impersonate = "chrome"
except ImportError:
    import requests as r
    s = r.Session()

BASE = "https://www.pakistanlawsite.com"
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)

# Get PLC(CS) 2024 - small enough to inspect
resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": 2024, "book": "PLC(CS)", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)

from bs4 import BeautifulSoup
soup = BeautifulSoup(resp.text, "html.parser")

# Find all caseType rows
rows = soup.find_all("tr", class_="caseType")
print(f"Found {len(rows)} caseType rows\n")

# Inspect first 3 rows in detail
for i, row in enumerate(rows[:3]):
    print(f"=== Row {i+1} ===")
    print(f"Attrs: {row.attrs}")
    print(f"HTML: {str(row)[:500]}")
    # Check all attrs of all children
    for child in row.find_all(True):
        if child.attrs:
            print(f"  <{child.name}> attrs: {child.attrs} text: {child.get_text(strip=True)[:80]}")
    print()
