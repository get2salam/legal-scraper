"""Probe early PLC(CS) years that might have been missed."""
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

# Check if legislation scraper is using the session
import subprocess
result = subprocess.run(
    ["powershell", "-Command", 
     "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'rescrape_empty' -and $_.Name -match 'python' } | Select-Object ProcessId"],
    capture_output=True, text=True, timeout=10
)
if "ProcessId" in result.stdout and len(result.stdout.strip().split("\n")) > 2:
    print("WARNING: Legislation re-scraper is running! This probe will share the PLS session.")
    print("Proceeding with caution (read-only probe, no downloads)...")

s.post(f"{BASE}/Login/ClearLoginHistory", data={"Login.UserName": user, "Login.Password": pw}, timeout=30)
time.sleep(2)
print("Logged in\n")

# Probe ALL years from 1947 to 1975 (before the scraper's range)
# Plus check gaps in 1972-1975
years_to_check = list(range(1947, 1976)) + list(range(2026, 2027))
total = 0

for y in years_to_check:
    resp = s.post(f"{BASE}/Login/CitationSearch", data={"year": y, "book": "PLC(CS)", "code": "", "court": "", "judge": "", "lawyer": "", "party": ""}, timeout=30)
    count = resp.text.count("caseType")
    if count > 0:
        total += count
        print(f"  {y}: {count} cases FOUND")
    time.sleep(2.5)

print(f"\nTotal in early years: {total}")
if total > 0:
    print("ACTION NEEDED: Run scraper for these years")
else:
    print("No early cases found - 1970 is the true start")
