import json
d = json.load(open('progress.json'))
for year in ['2018','2017','2016','2015','2014','2013','2012','2011','2010']:
    if year in d:
        print(f"\n=== {year} ===")
        for k, v in d[year].items():
            print(f"  {k}: {v}")
