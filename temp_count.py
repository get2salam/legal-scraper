import os
from pathlib import Path

DATA = Path("data_v2")
REPORTERS = ["SCMR","PLD","MLD","CLC","PCrLJ","PTD","PLC","YLR","CLD","GBLR"]
grand = 0
for y in range(2010, 2026):
    c = 0
    for r in REPORTERS:
        d = DATA / r / str(y)
        if d.exists():
            c += len([f for f in d.glob("*.json")])
    grand += c
    print(f"{y}: {c}")
print(f"TOTAL: {grand}")
