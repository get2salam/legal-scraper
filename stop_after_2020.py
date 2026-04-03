"""Re-add 2019 completed_searches so scraper stops after 2020."""
import json
from datetime import datetime

with open("data_v2/progress.json", encoding="utf-8") as f:
    p = json.load(f)

reporters = ["SCMR", "PLD", "MLD", "CLC", "PCrLJ", "PTD", "PLC", "YLR", "CLD", "GBLR"]
cs = set(p["completed_searches"])

added = 0
for r in reporters:
    key = f"2019-{r}"
    if key not in cs:
        p["completed_searches"].append(key)
        cs.add(key)
        added += 1
        print(f"Re-added: {key}")

# Also add 2018 and below just to be safe (they were never reset anyway)
p["last_updated"] = datetime.now().isoformat()

with open("data_v2/progress.json", "w", encoding="utf-8") as f:
    json.dump(p, f, indent=2)

print(f"\nAdded {added} entries. Scraper will now stop after completing 2020.")
