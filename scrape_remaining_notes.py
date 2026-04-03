"""Scrape CLCN (from 2020) and PLC(CS)N (from 2023) — the ones that were missed."""
import subprocess, sys, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Wait for any running scraper to finish
time.sleep(5)

tasks = [
    ("PLC(CS)N", "2023"),
]

for reporter, start_year in tasks:
    print(f"\n{'='*50}")
    print(f"SCRAPING: {reporter} from {start_year}")
    print(f"{'='*50}")
    subprocess.run(
        ["python", "-X", "utf8", "scrape_new_reporters.py", "--reporter", reporter, "--start-year", start_year, "--end-year", "1947"],
        timeout=7200,
    )
    time.sleep(10)

print("\nDONE")
