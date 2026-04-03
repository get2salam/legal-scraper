import glob, os, time
now = time.time()
recent = []
for rp in ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']:
    for f in glob.glob(f'data_v2/{rp}/*/*.json'):
        if now - os.path.getmtime(f) < 300:
            yr = f.replace('\\','/').split('/')[-2]
            recent.append(yr)
from collections import Counter
c = Counter(recent)
print(f"{len(recent)} files in 5min = ~{len(recent)*12}/hr")
print(f"Active: {dict(c.most_common())}")
total = sum(1 for rp in ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR'] for _ in glob.glob(f'data_v2/{rp}/*/*.json'))
print(f"Grand total: {total:,}")
