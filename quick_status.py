import os, json
from datetime import datetime

base = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']

# Total counts
grand_total = 0
reporter_totals = {}
for r in reporters:
    count = 0
    rdir = os.path.join(base, r)
    if os.path.isdir(rdir):
        for year_dir in os.listdir(rdir):
            ypath = os.path.join(rdir, year_dir)
            if os.path.isdir(ypath) and year_dir != 'original':
                count += len([f for f in os.listdir(ypath) if f.endswith('.json')])
    reporter_totals[r] = count
    grand_total += count

print(f"GRAND TOTAL: {grand_total:,}")
print(f"\nBy reporter:")
for r in sorted(reporter_totals, key=reporter_totals.get, reverse=True):
    print(f"  {r}: {reporter_totals[r]:,}")

# 1992 and 1991 breakdown
for year in [1992, 1991]:
    print(f"\n--- {year} ---")
    ytotal = 0
    for r in reporters:
        ypath = os.path.join(base, r, str(year))
        if os.path.isdir(ypath):
            c = len([f for f in os.listdir(ypath) if f.endswith('.json')])
            if c > 0:
                print(f"  {r}: {c}")
                ytotal += c
    print(f"  SUBTOTAL: {ytotal}")

# Recent activity - files created in last 2 hours
print(f"\n--- Last 2hr activity ---")
now = datetime.now().timestamp()
recent = 0
for r in reporters:
    rdir = os.path.join(base, r)
    if os.path.isdir(rdir):
        for year_dir in os.listdir(rdir):
            ypath = os.path.join(rdir, year_dir)
            if os.path.isdir(ypath) and year_dir != 'original':
                for f in os.listdir(ypath):
                    if f.endswith('.json'):
                        fp = os.path.join(ypath, f)
                        if now - os.path.getctime(fp) < 7200:
                            recent += 1
print(f"Cases scraped in last 2h: {recent}")
print(f"Rate: ~{recent // 2}/hr" if recent > 0 else "Rate: 0/hr")

# Check progress.json for current activity
pfile = os.path.join(os.path.dirname(base), "historical_progress.json")
if os.path.exists(pfile):
    with open(pfile) as f:
        prog = json.load(f)
    for year in ['1992', '1991']:
        if year in prog:
            entry = prog[year]
            print(f"\n--- progress.json {year} ---")
            print(f"  last_updated: {entry.get('last_updated','?')}")
            if 'reporters' in entry:
                for rname, rdata in entry['reporters'].items():
                    status = rdata.get('status', '?')
                    fetched = rdata.get('fetched', 0)
                    skipped = rdata.get('skipped', 0)
                    total = rdata.get('total', '?')
                    print(f"  {rname}: {status} ({fetched} fetched, {skipped} skipped, {total} total)")
