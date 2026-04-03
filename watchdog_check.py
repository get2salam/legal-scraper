import os, json, subprocess, sys
from datetime import datetime

base = r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2'
state_file = r'C:\Users\gempo\.openclaw\workspace\memory\scraper-state.json'

# Count cases
reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD']
cases = {}
total_cases = 0
for r in reporters:
    rpath = os.path.join(base, r)
    if not os.path.isdir(rpath): continue
    for y in sorted(os.listdir(rpath)):
        ypath = os.path.join(rpath, y)
        if not os.path.isdir(ypath): continue
        c = len([f for f in os.listdir(ypath) if f.endswith('.json')])
        if c > 0:
            cases.setdefault(r, {})[y] = c
            total_cases += c

# Count legislation
legpath = os.path.join(base, 'legislation')
leg = {}
leg_total = 0
if os.path.isdir(legpath):
    for letter in sorted(os.listdir(legpath)):
        lpath = os.path.join(legpath, letter)
        if os.path.isdir(lpath) and len(letter) == 1:
            c = len([f for f in os.listdir(lpath) if f.endswith('.json')])
            if c > 0:
                leg[letter] = c
                leg_total += c

# Progress file
progress = None
pf = os.path.join(legpath, 'progress.json')
if os.path.exists(pf):
    with open(pf) as f:
        progress = json.load(f)

# Running python processes
try:
    result = subprocess.run(
        ['powershell', '-Command', 'Get-Process python -ErrorAction SilentlyContinue | Select-Object Id, StartTime | ConvertTo-Json'],
        capture_output=True, text=True, timeout=10
    )
    procs = result.stdout.strip()
except:
    procs = "[]"

# Load previous state
prev = None
if os.path.exists(state_file):
    with open(state_file) as f:
        prev = json.load(f)

# Save current state
current = {
    "timestamp": datetime.utcnow().isoformat() + "Z",
    "total_cases": total_cases,
    "total_legislation": leg_total,
    "cases": cases,
    "legislation": leg,
    "progress": progress
}
os.makedirs(os.path.dirname(state_file), exist_ok=True)
with open(state_file, 'w') as f:
    json.dump(current, f, indent=2)

# Output
print(f"=== WATCHDOG REPORT ===")
print(f"Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
print(f"\n--- CASES ---")
for r in reporters:
    if r in cases:
        rtotal = sum(cases[r].values())
        years = ', '.join(f"{y}:{c}" for y,c in sorted(cases[r].items()))
        print(f"  {r}: {rtotal} ({years})")
print(f"  TOTAL: {total_cases}")

print(f"\n--- LEGISLATION ---")
for letter, count in sorted(leg.items()):
    print(f"  {letter}: {count} files")
print(f"  TOTAL: {leg_total}")

if progress:
    print(f"\n--- PROGRESS ---")
    if 'completed_alphabets' in progress:
        print(f"  Completed: {progress['completed_alphabets']}")
    if 'current_alphabet' in progress:
        print(f"  Current: {progress['current_alphabet']}")

print(f"\n--- PROCESSES ---")
print(f"  Raw: {procs}")

# Delta
if prev:
    case_delta = total_cases - prev.get('total_cases', 0)
    leg_delta = leg_total - prev.get('total_legislation', 0)
    print(f"\n--- DELTA (since last check) ---")
    print(f"  Cases: {'+' if case_delta >= 0 else ''}{case_delta}")
    print(f"  Legislation: {'+' if leg_delta >= 0 else ''}{leg_delta}")
    
    # Check per-reporter changes
    prev_cases = prev.get('cases', {})
    for r in reporters:
        if r in cases:
            for y, c in cases[r].items():
                pc = prev_cases.get(r, {}).get(y, 0)
                if c != pc:
                    print(f"  {r}/{y}: {pc} -> {c} (+{c-pc})")
