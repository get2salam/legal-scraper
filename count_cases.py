import os

reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']
base = r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2'
grand_total = 0
reporter_totals = {}

for r in reporters:
    reporter_totals[r] = 0

for year in range(1980, 2027):
    year_count = 0
    year_detail = {}
    for r in reporters:
        d = os.path.join(base, r, str(year))
        if os.path.isdir(d):
            c = len([f for f in os.listdir(d) if f.endswith('.json')])
            year_count += c
            reporter_totals[r] += c
            if c > 0:
                year_detail[r] = c
    if year_count > 0:
        grand_total += year_count
        detail_str = ', '.join(f'{r}:{c}' for r, c in sorted(year_detail.items(), key=lambda x: -x[1]))
        print(f"{year}: {year_count:,}  ({detail_str})")

print(f"\nGRAND TOTAL: {grand_total:,}")
print(f"\nBy Reporter:")
for r in sorted(reporter_totals.keys(), key=lambda x: -reporter_totals[x]):
    print(f"  {r}: {reporter_totals[r]:,}")
