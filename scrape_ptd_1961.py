"""Scrape PTD 1961 - the last gap."""
import sys, os, re, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = "chrome"
BASE = "https://www.pakistanlawsite.com"
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)
print("Logged in")

resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": 1961, "book": "PTD", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
soup = BeautifulSoup(resp.text, "html.parser")
rows = soup.find_all("tr", class_="caseType")
print(f"Found {len(rows)} rows")

# Parse all cases
cases = []
for row in rows:
    tds = row.find_all("td")
    citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
    if not re.match(r"\d{4}\s+\w+\s+\d+", citation):
        continue
    btn = row.find("input", attrs={"casetypeid": True})
    case_id = btn.get("casetypeid", "") if btn else ""
    cases.append({"citation": citation, "case_id": case_id})

print(f"Parsed {len(cases)} valid cases")
if cases:
    print(f"First: {cases[0]}")
    print(f"Has case_id: {bool(cases[0]['case_id'])}")

# Scrape
out_dir = Path("data_v2/PTD/1961")
out_dir.mkdir(parents=True, exist_ok=True)
html_dir = out_dir / "original_html"
html_dir.mkdir(exist_ok=True)
readable_dir = out_dir / "readable_html"
readable_dir.mkdir(exist_ok=True)

scraped = 0
skipped = 0
errors = 0

for case in cases:
    fn = case["citation"].replace(" ", "_")
    if (out_dir / f"{fn}.json").exists():
        skipped += 1
        continue
    
    if not case["case_id"]:
        errors += 1
        continue
    
    time.sleep(3)
    try:
        resp2 = s.post(f"{BASE}/Login/GetCaseFile", data={"caseName": case["case_id"], "headNotes": 0}, timeout=60)
        if resp2.status_code == 200 and len(resp2.text) > 100:
            content = resp2.text
            try:
                content = json.loads(content)
            except:
                pass
            
            case_data = {
                "citation": case["citation"],
                "reporter": "PTD",
                "year": 1961,
                "case_name": case["case_id"],
                "judgment": content if isinstance(content, str) else json.dumps(content),
            }
            with open(out_dir / f"{fn}.json", "w", encoding="utf-8") as f:
                json.dump(case_data, f, ensure_ascii=False, indent=2)
            with open(html_dir / f"{fn}.html", "w", encoding="utf-8") as f:
                f.write(content if isinstance(content, str) else json.dumps(content))
            
            scraped += 1
            if scraped % 20 == 0:
                print(f"  Progress: {scraped} scraped")
        else:
            errors += 1
    except Exception as e:
        print(f"  Error: {e}")
        errors += 1

print(f"\nDONE: Scraped {scraped} | Skipped {skipped} | Errors {errors}")
print(f"PTD 1961 now: {len(list(out_dir.glob('*.json')))} files")
