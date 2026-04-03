"""Find which PLD 2025 cases we're missing."""
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

# Get PLD 2025
resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": 2025, "book": "PLD", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
soup = BeautifulSoup(resp.text, "html.parser")
rows = soup.find_all("tr", class_="caseType")

# Parse all citations from PLS
pls_cases = []
for row in rows:
    tds = row.find_all("td")
    citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
    btn = row.find("input", attrs={"casetypeid": True})
    case_id = btn.get("casetypeid", "") if btn else ""
    if citation:
        pls_cases.append({"citation": citation, "case_id": case_id})

print(f"PLS has {len(pls_cases)} cases for PLD 2025")

# Get our files
our_dir = Path("data_v2/PLD/2025")
our_files = set(f.stem for f in our_dir.glob("*.json")) if our_dir.exists() else set()
print(f"We have {len(our_files)} files")

# Find missing
missing = []
for case in pls_cases:
    fn = case["citation"].replace(" ", "_")
    if fn not in our_files:
        missing.append(case)

print(f"\nMissing: {len(missing)} cases")
for m in missing[:10]:
    print(f"  {m['citation']} (ID: {m['case_id']})")

# Now scrape the missing ones
if missing:
    print(f"\nScraping {len(missing)} missing cases...")
    scraped = 0
    for case in missing:
        if not case["case_id"]:
            print(f"  SKIP {case['citation']}: no case_id")
            continue
        time.sleep(3)
        resp2 = s.post(f"{BASE}/Login/GetCaseFile", data={"caseName": case["case_id"], "headNotes": 0}, timeout=60)
        if resp2.status_code == 200 and len(resp2.text) > 100:
            content = resp2.text
            try:
                content = json.loads(content)
            except:
                pass
            
            fn = case["citation"].replace(" ", "_")
            out_dir = our_dir
            out_dir.mkdir(parents=True, exist_ok=True)
            
            case_data = {
                "citation": case["citation"],
                "reporter": "PLD",
                "year": 2025,
                "case_name": case["case_id"],
                "judgment": content if isinstance(content, str) else json.dumps(content),
            }
            with open(out_dir / f"{fn}.json", "w", encoding="utf-8") as f:
                json.dump(case_data, f, ensure_ascii=False, indent=2)
            
            # HTML
            html_dir = out_dir / "original_html"
            html_dir.mkdir(exist_ok=True)
            html_content = content if isinstance(content, str) else json.dumps(content)
            with open(html_dir / f"{fn}.html", "w", encoding="utf-8") as f:
                f.write(html_content)
            
            scraped += 1
            if scraped % 10 == 0:
                print(f"  Scraped {scraped}/{len(missing)}")
        else:
            print(f"  FAIL {case['citation']}: {resp2.status_code}")
    
    print(f"\nDone: scraped {scraped} new cases")
    print(f"PLD 2025 now: {len(list(our_dir.glob('*.json')))} files")
