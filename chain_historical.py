"""
Chain historical scraping: waits for current scraper to finish,
then launches 1979→1900 to catch pre-partition common law cases,
then auto-starts the legislation scraper (D-Z, resumes from progress.json).
"""
import subprocess
import time
import os
import sys

SCRAPER_DIR = os.path.dirname(os.path.abspath(__file__))

def is_scraper_running():
    try:
        result = subprocess.run(
            ["powershell", "-Command",
             "Get-CimInstance Win32_Process | Where-Object { "
             "$_.CommandLine -match 'historical_scraper' -and $_.Name -match 'python' "
             "-and $_.ProcessId -ne " + str(os.getpid()) + " } | Measure-Object | Select-Object -ExpandProperty Count"],
            capture_output=True, text=True, timeout=15
        )
        return int(result.stdout.strip() or "0") > 0
    except:
        return False

# Phase 1: Wait for current historical scraper, then launch 1979→1900
print(f"[{time.strftime('%H:%M')}] Waiting for current scraper to finish...")
while is_scraper_running():
    time.sleep(60)

print(f"[{time.strftime('%H:%M')}] Scraper finished. Launching 1979→1900...")
subprocess.run(
    [sys.executable, "-u", "historical_scraper.py", "--from-year", "1979", "--to-year", "1900"],
    cwd=SCRAPER_DIR
)
print(f"[{time.strftime('%H:%M')}] 1979→1900 complete.")

# Phase 2: Launch legislation scraper (resumes from progress.json — A,B,C done, starts at D)
print(f"[{time.strftime('%H:%M')}] Starting legislation scraper (D-Z)...")
subprocess.run(
    [sys.executable, "-u", "legislation_scraper.py"],
    cwd=SCRAPER_DIR
)
print(f"[{time.strftime('%H:%M')}] Legislation scraper complete.")
