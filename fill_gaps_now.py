"""Fill all identified gaps — scrape missing cases from PLS."""
import sys, os, time, json, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / '.env')

try:
    from curl_cffi import requests as r
    s = r.Session()
    s.impersonate = "chrome"
except ImportError:
    import requests as r
    s = r.Session()

from bs4 import BeautifulSoup

BASE = "https://www.pakistanlawsite.com"
DATA = Path(__file__).parent / "data_v2"
DELAY = 3.0
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))

# Login
s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)
print("Logged in\n")

# Load targets
targets = json.load(open("gap_fill_targets.json", encoding="utf-8"))
print(f"Targets: {len(targets)} reporter/years, ~{sum(t['missing'] for t in targets)} cases\n")

total_scraped = 0
total_skipped = 0
total_errors = 0

for target in targets:
    reporter = target["reporter"]
    year = target["year"]
    rep_clean = reporter.replace("(", "").replace(")", "")
    
    print(f"=== {reporter} {year} (missing ~{target['missing']}) ===")
    
    # Search for cases
    time.sleep(DELAY)
    resp = s.post(f"{BASE}/Login/CitationSearch",
        data={"year": year, "book": reporter, "code": "", "court": "", "judge": "", "lawyer": "", "party": ""},
        timeout=30)
    
    soup = BeautifulSoup(resp.text, "html.parser")
    cases = []
    
    for row in soup.find_all("tr", class_="caseType"):
        tds = row.find_all("td")
        citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
        case_id = ""
        input_elem = row.find("input", attrs={"casetypeid": True})
        if input_elem:
            case_id = input_elem.get("casetypeid", "")
        
        court = tds[3].get_text(strip=True) if len(tds) >= 4 else ""
        judge_elem = row.find("span", style=lambda st: st and "darkred" in st)
        judge = judge_elem.get_text(strip=True) if judge_elem else ""
        
        if citation:
            cases.append({"citation": citation, "case_id": case_id, "court": court, "judge": judge})
    
    print(f"  Found {len(cases)} cases on PLS")
    
    out_dir = DATA / rep_clean / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    html_dir = out_dir / "original_html"
    html_dir.mkdir(exist_ok=True)
    readable_dir = out_dir / "readable_html"
    readable_dir.mkdir(exist_ok=True)
    
    for i, case in enumerate(cases):
        filename = case["citation"].replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_")
        json_path = out_dir / f"{filename}.json"
        
        if json_path.exists():
            total_skipped += 1
            continue
        
        # Download case content
        if not case["case_id"]:
            total_errors += 1
            continue
        
        time.sleep(DELAY)
        try:
            resp2 = s.post(f"{BASE}/Login/GetCaseFile",
                data={"caseName": case["case_id"], "headNotes": 0}, timeout=60)
            
            if resp2.status_code != 200 or len(resp2.text) < 100:
                total_errors += 1
                continue
            
            # Decode content
            content = resp2.text
            try:
                content = json.loads(content)
            except:
                pass
            
            # Save JSON
            case_data = {
                "citation": case["citation"],
                "reporter": reporter,
                "year": year,
                "case_name": case["case_id"],
                "court": case.get("court", ""),
                "judges": case.get("judge", ""),
                "judgment": content if isinstance(content, str) else json.dumps(content),
                "scraped_at": datetime.now().isoformat(),
            }
            
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(case_data, f, ensure_ascii=False, indent=2)
            
            # Save original HTML
            html_content = content if isinstance(content, str) else json.dumps(content)
            with open(html_dir / f"{filename}.html", "w", encoding="utf-8") as f:
                f.write(html_content)
            
            # Save readable HTML
            try:
                readable = html_content
                if readable.startswith('"') and readable.endswith('"'):
                    try:
                        readable = json.loads(readable)
                    except:
                        readable = readable[1:-1]
                def _decode(m):
                    try: return chr(int(m.group(1), 16))
                    except: return m.group(0)
                readable = re.sub(r'\\u([0-9a-fA-F]{4})', _decode, readable)
                readable = readable.replace('\\r\\n', '\n').replace('\\n', '\n')
                readable = readable.replace('\\"', '"').replace('\\\\', '\\')
                with open(readable_dir / f"{filename}.html", "w", encoding="utf-8") as f:
                    f.write(readable)
            except:
                pass
            
            # Append to JSONL
            try:
                with open(out_dir / f"{year}_{rep_clean}.jsonl", "a", encoding="utf-8") as f:
                    f.write(json.dumps(case_data, ensure_ascii=False) + "\n")
            except:
                pass
            
            total_scraped += 1
            
            if (i + 1) % 20 == 0:
                print(f"  Progress: {i+1}/{len(cases)} ({total_scraped} new)")
        
        except Exception as e:
            print(f"  Error: {e}")
            total_errors += 1

print(f"\n{'='*50}")
print(f"DONE: Scraped {total_scraped} | Skipped {total_skipped} (existing) | Errors {total_errors}")
print(f"{'='*50}")
