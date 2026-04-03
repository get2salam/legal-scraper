import os
from datetime import datetime
from collections import defaultdict

reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']
hourly = defaultdict(int)
grand = 0

for r in reporters:
    for year in range(1990, 2027):
        d = f'data_v2/{r}/{year}'
        if not os.path.isdir(d): continue
        for f in os.listdir(d):
            if f.endswith('.json'):
                grand += 1
                try:
                    ct = os.path.getctime(os.path.join(d, f))
                    dt = datetime.fromtimestamp(ct)
                    if dt.date() == datetime(2026, 2, 15).date():
                        hourly[dt.hour] += 1
                except:
                    pass

today = sum(hourly.values())
hours_done = sorted(hourly.keys())
last_full_hour = hours_done[-2] if len(hours_done) > 1 else hours_done[-1]
current_hour = hours_done[-1]

print(f'Grand total: {grand:,}')
print(f'Today (Feb 15): {today:,}')
print(f'Last full hour ({last_full_hour:02d}:00): {hourly[last_full_hour]:,}/hr')
print(f'Current hour ({current_hour:02d}:00): {hourly[current_hour]:,} so far')
print()
for h in sorted(hourly.keys()):
    c = hourly[h]
    bar = '#' * (c // 25)
    print(f'{h:02d}:00  {c:>5}  {bar}')
print(f'---')
print(f'Avg/hr: {today // max(len(hourly),1):,}')
