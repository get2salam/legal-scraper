import json
d = json.load(open('data_v2/progress.json'))
cs = d['completed_searches']
for y in range(2018, 2009, -1):
    year_entries = sorted([x for x in cs if x.startswith(str(y))])
    if year_entries:
        print(f"{y}: {', '.join(year_entries)}")
    else:
        print(f"{y}: (none)")
print(f"\nTotal completed searches: {len(cs)}")
