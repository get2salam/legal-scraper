"""Scrape ALL Notes reporters sequentially. One PLS session at a time."""
import subprocess
import sys
import time

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

REPORTERS = ["PCRLJN", "YLRN", "CLCN", "PLC(CS)N"]

for reporter in REPORTERS:
    print(f"\n{'='*50}")
    print(f"SCRAPING: {reporter}")
    print(f"{'='*50}")
    
    result = subprocess.run(
        ["python", "-X", "utf8", "scrape_new_reporters.py", "--reporter", reporter, "--start-year", "2026", "--end-year", "1947"],
        capture_output=False,
        timeout=7200,  # 2 hour timeout per reporter
    )
    
    print(f"\n{reporter}: exit code {result.returncode}")
    time.sleep(10)  # Brief pause between reporters

print("\n\nALL NOTES REPORTERS COMPLETE")
