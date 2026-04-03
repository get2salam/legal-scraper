"""Verify ALL early PLC(CS) years using proper HTML parsing."""
import sys, os, time, json
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

from bs4 import BeautifulSoup

BASE = "https://www.pakistanlawsite.com"
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))

s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)
print("Logged in\n")

# Check ALL years from 1947 to 1975
years_to_check = list(range(1947, 1976))
found_years = {}

for y in years_to_check:
    resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": y, "book": "PLC(CS)", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
    
    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.find_all("tr", class_="caseType")
    
    if len(rows) > 0:
        found_years[y] = len(rows)
        citations = []
        for row in rows[:3]:
            tds = row.find_all("td")
            if len(tds) >= 2:
                citations.append(tds[1].get_text(strip=True))
        print(f"  {y}: {len(rows)} REAL cases - {citations}")
    else:
        print(f"  {y}: 0 cases")
    
    time.sleep(2)

print(f"\nYears with real PLC(CS) cases: {sorted(found_years.keys())}")
print(f"Total: {sum(found_years.values())} cases across {len(found_years)} years")
