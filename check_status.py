import subprocess, json, os, glob

# Check running python processes
result = subprocess.run(['wmic', 'process', 'where', "Name='python.exe'", 'get', 'ProcessId,CommandLine', '/format:list'], capture_output=True, text=True)
lines = result.stdout.strip().split('\n')
scrapers = []
current = {}
for line in lines:
    line = line.strip()
    if line.startswith('CommandLine='):
        current['cmd'] = line[12:]
    elif line.startswith('ProcessId='):
        current['pid'] = line[10:]
        if current.get('cmd') and any(k in current['cmd'] for k in ['historical', 'pls_scraper', 'verify', 'scraper_chain', 'fill_']):
            scrapers.append(current.copy())
        current = {}

print("=== RUNNING SCRAPERS ===")
if scrapers:
    for s in scrapers:
        print(f"  PID {s['pid']}: {s['cmd'][:120]}")
else:
    print("  None running")

# Check latest log
log_path = r'logs\historical_stderr.log'
if os.path.exists(log_path):
    with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
        lines = f.readlines()
    print(f"\n=== LATEST LOG (last 5 lines) ===")
    for l in lines[-5:]:
        print(f"  {l.rstrip()}")

# Count cases per year for recent years
print("\n=== LOCAL CASE COUNTS ===")
reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']
base = 'data_v2'
for year in range(1987, 2016):
    count = 0
    for r in reporters:
        d = os.path.join(base, r, str(year))
        if os.path.isdir(d):
            count += len(glob.glob(os.path.join(d, '*.json')))
    if count > 0:
        print(f"  {year}: {count}")
