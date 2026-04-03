"""Wait for PCrLJ 1978 scraper to finish, then scrape PLC 1985."""
import subprocess, sys, time, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Wait for any running PLS scraper to finish
while True:
    result = subprocess.run(
        ["powershell", "-Command", "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'pls_scraper_v2' -and $_.Name -match 'python' } | Measure-Object | Select-Object -ExpandProperty Count"],
        capture_output=True, text=True, timeout=10
    )
    count = int(result.stdout.strip()) if result.stdout.strip().isdigit() else 0
    if count == 0:
        break
    print(f"Waiting for PCrLJ scraper to finish ({count} running)...")
    time.sleep(30)

print("PLS free. Starting PLC 1985 scrape...")
time.sleep(5)

subprocess.run(
    ["python", "-X", "utf8", "pls_scraper_v2.py", "scrape", "--reporter", "PLC", "--start-year", "1985", "--end-year", "1985", "--no-continue"],
    timeout=3600,
)
print("PLC 1985 done")
