import json
from pathlib import Path

DATA_DIR = Path("data_v2")
REPORTERS = ["SCMR","PLD","MLD","CLC","PCrLJ","PTD","PLC","YLR","CLD","GBLR"]

progress = json.loads((DATA_DIR / "progress.json").read_text(encoding="utf-8"))
cases_fetched = set(progress.get("cases_fetched", []))
print(f"Main scraper tracked: {len(cases_fetched)} citations")

extra = 0
extra_by_year = {}
total_files = 0

for r in REPORTERS:
    rd = DATA_DIR / r
    if not rd.exists():
        continue
    for yd in sorted(rd.iterdir()):
        if not yd.is_dir() or not yd.name.isdigit():
            continue
        year = yd.name
        for f in yd.glob("*.json"):
            total_files += 1
            try:
                d = json.loads(f.read_text(encoding="utf-8"))
                cit = d.get("citation", "")
                if cit and cit not in cases_fetched:
                    extra += 1
                    extra_by_year[year] = extra_by_year.get(year, 0) + 1
            except Exception:
                pass

print(f"Total files on disk: {total_files}")
print(f"NOT in main scraper progress: {extra}")
if extra_by_year:
    print("By year:")
    for y in sorted(extra_by_year.keys(), reverse=True):
        print(f"  {y}: {extra_by_year[y]}")
