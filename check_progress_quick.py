import json, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
p = json.load(open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/data_v2/federal_laws/progress.json', encoding='utf-8'))
print(f"Phase 1 complete: {p.get('phase1_complete')}")
print(f"Phase 2 complete: {p.get('phase2_complete')}")
print(f"Phase 3 complete: {p.get('phase3_complete')}")
print(f"Laws parsed: {p.get('laws_parsed')}")
print(f"PDF URLs found: {p.get('pdf_urls_found', 0)}")
print(f"PDFs downloaded: {p.get('pdfs_downloaded', 0)}")
print(f"Texts extracted: {p.get('texts_extracted', 0)}")
print(f"HTMLs generated: {p.get('htmls_generated', 0)}")
print(f"Failures: {len(p.get('failures', []))}")
print(f"Last updated: {p.get('last_updated')}")

# Check stderr log last 5 lines
try:
    lines = open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/data_v2/federal_laws/logs/stderr.log', encoding='utf-8').readlines()
    print(f"\nLast 5 log lines:")
    for line in lines[-5:]:
        print(f"  {line.rstrip()}")
except:
    pass

# Check if process still running
import subprocess
result = subprocess.run(['tasklist', '/FI', 'PID eq 32400'], capture_output=True, text=True)
if '32400' in result.stdout:
    print("\nScraper process (PID 32400) is RUNNING")
else:
    print("\nScraper process (PID 32400) has FINISHED")
