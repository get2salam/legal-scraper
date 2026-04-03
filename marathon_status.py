"""Quick status for marathon monitor cron job."""
import os, subprocess

DATA = r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2'
REPORTERS = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']

# PLS counts
r = {}
for rep in REPORTERS:
    rd = os.path.join(DATA, rep)
    if not os.path.isdir(rd): continue
    for yd in os.listdir(rd):
        if not yd.isdigit(): continue
        yp = os.path.join(rd, yd)
        if os.path.isdir(yp):
            c = len([f for f in os.listdir(yp) if f.endswith('.json')])
            r[rep] = r.get(rep, 0) + c
total = sum(r.values())
print(f"PLS: {total:,}")
for k, v in sorted(r.items(), key=lambda x: -x[1]):
    print(f"  {k}: {v:,}")

# Court counts
print()
court_dir = os.path.join(DATA, 'court_cases')
if os.path.isdir(court_dir):
    court_total = 0
    for c in ['SC','SHC','IHC','LHC','FSC','SST']:
        cp = os.path.join(court_dir, c)
        if os.path.isdir(cp):
            cnt = sum(1 for root, dirs, files in os.walk(cp) for f in files if f.endswith('.json'))
            print(f"{c}: {cnt} JSON")
            court_total += cnt
    print(f"Court total: {court_total:,}")

# Federal laws
fed_dir = os.path.join(DATA, 'federal_laws', 'json')
if os.path.isdir(fed_dir):
    fed = len([f for f in os.listdir(fed_dir) if f.endswith('.json')])
    print(f"\nFederal laws: {fed}")

# Running scrapers
print("\nRunning scrapers:")
try:
    result = subprocess.run(
        ['powershell', '-Command',
         "Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'scraper|fill_' } | ForEach-Object { \"PID $($_.ProcessId): $($_.CommandLine.Substring(0, [Math]::Min(120, $_.CommandLine.Length)))\" }"],
        capture_output=True, text=True, timeout=15
    )
    out = result.stdout.strip()
    print(out if out else "  None running")
except Exception as e:
    print(f"  Error checking: {e}")
