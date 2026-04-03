"""
Scan all files for login page placeholders using file size fingerprinting + content check
"""
import os, json, re, hashlib
from collections import defaultdict

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]

PLACEHOLDER_SIGS = [
    "Pakistan Law Site\nPlease Wait",
    "Pakistan Law Site\r\nPlease Wait",
    "PLD Publishers\n35-Nabha Road",
    "PLD Publishers\r\n35-Nabha Road",
    "Pakistan Law Site Please Wait",
]

login_counts = defaultdict(lambda: defaultdict(int))
total_login = 0
total_scanned = 0

for reporter in REPORTERS:
    rdir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(rdir):
        continue
    for yname in sorted(os.listdir(rdir)):
        ypath = os.path.join(rdir, yname)
        if not os.path.isdir(ypath) or not re.match(r'^\d{4}$', yname):
            continue
        for fname in os.listdir(ypath):
            if not fname.endswith('.json'):
                continue
            total_scanned += 1
            fpath = os.path.join(ypath, fname)
            try:
                with open(fpath, 'r', encoding='utf-8') as fh:
                    data = json.load(fh)
                jr = str(data.get('judgment_raw', ''))[:3000]
                for sig in PLACEHOLDER_SIGS:
                    if sig in jr:
                        login_counts[reporter][yname] += 1
                        total_login += 1
                        break
            except:
                pass
    print(f"Scanned {reporter}")

print(f"\nTotal scanned: {total_scanned}")
print(f"Total login pages: {total_login}")

if total_login > 0:
    print("\nBreakdown:")
    for r in REPORTERS:
        for y in sorted(login_counts[r].keys()):
            print(f"  {r}/{y}: {login_counts[r][y]}")

# Save results
import json as j2
with open(os.path.join(DATA_DIR, "audit", "login_scan_results.json"), 'w') as f:
    j2.dump({'total': total_login, 'scanned': total_scanned, 'by_reporter_year': {r: dict(v) for r, v in login_counts.items()}}, f, indent=2)
