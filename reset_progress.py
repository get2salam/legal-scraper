"""Reset suspicious completed_searches in progress.json"""
import json
import shutil
from datetime import datetime

# Backup first
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
shutil.copy("data_v2/progress.json", f"data_v2/progress_backup_{ts}.json")
print(f"Backup saved: data_v2/progress_backup_{ts}.json")

with open("data_v2/progress.json", encoding="utf-8") as f:
    p = json.load(f)

# Suspicious combos to reset (counts way below expected)
to_reset = set([
    # 2021: All reporters except SCMR (341 = full year)
    "2021-PLD", "2021-MLD", "2021-CLC", "2021-PCrLJ",
    "2021-PTD", "2021-PLC", "2021-YLR", "2021-CLD", "2021-GBLR",
    # 2020: Reporters that had <20 cases (SCMR/PLD/MLD/CLC were full)
    "2020-PCrLJ", "2020-PTD", "2020-PLC", "2020-YLR", "2020-CLD", "2020-GBLR",
    # 2019: ALL reporters (4-19 cases each, clearly incomplete)
    "2019-SCMR", "2019-PLD", "2019-MLD", "2019-CLC", "2019-PCrLJ",
    "2019-PTD", "2019-PLC", "2019-YLR", "2019-CLD", "2019-GBLR",
])

before = len(p["completed_searches"])
p["completed_searches"] = [s for s in p["completed_searches"] if s not in to_reset]
after = len(p["completed_searches"])

removed = before - after
print(f"Removed {removed} entries from completed_searches ({before} -> {after})")

for entry in sorted(to_reset):
    print(f"  RESET: {entry}")

p["last_updated"] = datetime.now().isoformat()

with open("data_v2/progress.json", "w", encoding="utf-8") as f:
    json.dump(p, f, indent=2)

print("\nProgress reset complete! Scraper will re-search these combos on next run.")
