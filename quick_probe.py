"""Quick probe of new reporters across all years."""
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

BASE = "https://www.pakistanlawsite.com"
user = os.getenv("PLS_USER", os.getenv("PAKISTAN_LAW_USER", ""))
pw = os.getenv("PLS_PASS", os.getenv("PAKISTAN_LAW_PASS", ""))
s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)
print("Logged in\n")

reporters = ["PLC(CS)", "PCRLJN", "YLRN", "CLCN", "PLC(CS)N"]
years = [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2015, 2010, 2005, 2000, 1995, 1990, 1985, 1980]
grand_total = 0

for rep in reporters:
    total = 0
    year_data = []
    for y in years:
        resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": y, "book": rep, "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
        count = resp.text.count("caseType")
        if count > 0:
            total += count
            year_data.append(f"{y}:{count}")
        time.sleep(2.5)
    grand_total += total
    yd = "  ".join(year_data) if year_data else "no cases"
    print(f"{rep}: {total} total | {yd}")

print(f"\nGRAND TOTAL across sampled years: {grand_total}")
