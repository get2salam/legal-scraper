"""
Check how many files contain the PLS login/placeholder page instead of real judgments
"""
import os
import json
import sys

DATA_DIR = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
REPORTERS = ["SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR", "PTD", "PLC", "CLD", "GBLR"]

# The signature of the login/placeholder page
PLACEHOLDER_SIGS = [
    "Pakistan Law Site\nPlease Wait",
    "Pakistan Law Site\r\nPlease Wait",
    "Customer Care Office",
    "PLD Publishers\n35-Nabha Road",
    "PLD Publishers\r\n35-Nabha Road",
]

login_page_files = {}
total_login = 0
total_files = 0

for reporter in REPORTERS:
    reporter_dir = os.path.join(DATA_DIR, reporter)
    if not os.path.isdir(reporter_dir):
        continue
    
    login_page_files[reporter] = {}
    
    for year_name in sorted(os.listdir(reporter_dir)):
        year_path = os.path.join(reporter_dir, year_name)
        if not os.path.isdir(year_path) or not year_name.isdigit():
            continue
        
        year_login = 0
        year_total = 0
        
        for fname in os.listdir(year_path):
            if not fname.endswith('.json'):
                continue
            
            year_total += 1
            total_files += 1
            fpath = os.path.join(year_path, fname)
            
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Check judgment_raw for login page signature
                judgment = data.get('judgment_raw', '') or ''
                if not judgment:
                    judgment = data.get('judgment', '') or ''
                
                # Extract just text content (strip HTML)
                text = judgment[:2000]
                
                is_login = False
                for sig in PLACEHOLDER_SIGS:
                    if sig in text:
                        is_login = True
                        break
                
                if is_login:
                    year_login += 1
                    total_login += 1
            except:
                pass
        
        if year_login > 0:
            login_page_files[reporter][year_name] = {
                'login': year_login,
                'total': year_total,
                'pct': round(year_login / year_total * 100, 1)
            }

print("=" * 80)
print("FILES CONTAINING PLS LOGIN/PLACEHOLDER PAGE INSTEAD OF JUDGMENTS")
print("=" * 80)
print(f"\nTotal files checked: {total_files}")
print(f"Total login page files: {total_login}")
print(f"Percentage: {round(total_login / total_files * 100, 2)}%")

print(f"\n{'Reporter':<10} {'Year':<6} {'Login':<8} {'Total':<8} {'Pct':<8}")
print("-" * 45)

for reporter in REPORTERS:
    if reporter not in login_page_files:
        continue
    for year in sorted(login_page_files[reporter].keys()):
        d = login_page_files[reporter][year]
        print(f"{reporter:<10} {year:<6} {d['login']:<8} {d['total']:<8} {d['pct']:<8.1f}%")

# Summary by reporter
print("\nSUMMARY BY REPORTER:")
print("-" * 40)
for reporter in REPORTERS:
    if reporter in login_page_files and login_page_files[reporter]:
        total_r = sum(d['login'] for d in login_page_files[reporter].values())
        print(f"  {reporter}: {total_r} login page files")
    else:
        print(f"  {reporter}: 0 login page files")

# Save
with open(os.path.join(DATA_DIR, "audit", "loginpage_results.json"), 'w') as f:
    json.dump({
        'total_files': total_files,
        'total_login': total_login,
        'by_reporter_year': login_page_files
    }, f, indent=2)

print("\nResults saved to audit/loginpage_results.json")
