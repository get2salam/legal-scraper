from pathlib import Path
data = Path('data_v2')
reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']
total = 0
for decade_start in range(1940, 2030, 10):
    decade_total = 0
    for y in range(decade_start, decade_start+10):
        for r in reporters:
            d = data / r / str(y)
            if d.exists():
                c = len(list(d.glob('*.json')))
                decade_total += c
    if decade_total > 0:
        total += decade_total
        print(f"{decade_start}s: {decade_total} cases")
print(f"\nTOTAL: {total}")
