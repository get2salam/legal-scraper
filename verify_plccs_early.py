"""Verify early PLC(CS) years - parse properly like the scraper does."""
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

# Check years 1960-1970 properly
years_to_check = list(range(1960, 1971))
total_real = 0
total_count_method = 0

for y in years_to_check:
    resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": y, "book": "PLC(CS)", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
    
    # Method 1: raw count (probe method - may be false positive)
    raw_count = resp.text.count("caseType")
    total_count_method += raw_count
    
    # Method 2: proper BeautifulSoup parsing (scraper method)
    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.find_all("tr", class_="caseType")
    real_count = len(rows)
    total_real += real_count
    
    # Show diff
    status = "✓ REAL CASES" if real_count > 0 else "✗ empty (false positives in probe)"
    print(f"  {y}: raw_count={raw_count}, real_cases={real_count}  [{status}]")
    if real_count > 0:
        # Show first case citation
        for row in rows[:2]:
            tds = row.find_all("td")
            if len(tds) >= 2:
                print(f"    -> {tds[1].get_text(strip=True)}")
    time.sleep(2)

print(f"\nSummary: raw count method found {total_count_method}, real cases found {total_real}")
