import json
from pathlib import Path
p = json.loads(Path("data_v2/progress.json").read_text(encoding="utf-8"))
entries = [s for s in p["completed_searches"] if s.startswith("2018-")]
print(f"2018 completed: {entries}")
reporters = ["SCMR","PLD","MLD","CLC","PCrLJ","PTD","PLC","YLR","CLD","GBLR"]
for r in reporters:
    d = Path("data_v2") / r / "2018"
    c = len(list(d.glob("*.json"))) if d.exists() else 0
    done = f"2018-{r}" in entries
    tag = " [DONE]" if done else ""
    print(f"  {r}: {c}{tag}")
