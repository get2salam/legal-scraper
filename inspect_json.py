import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
leg_dir = Path('data_v2/legislation')
files = list(leg_dir.glob('*.json'))
print(f'Total files: {len(files)}')
if files:
    d = json.load(open(files[0], encoding='utf-8'))
    print(f'Type: {type(d)}')
    if isinstance(d, list):
        print(f'List len: {len(d)}')
        if d:
            print(f'First item keys: {list(d[0].keys())}')
            item = d[0]
            for k, v in item.items():
                val = str(v)[:80] if v else ''
                print(f'  {k}: {val}')
    else:
        print(f'Keys: {list(d.keys())}')
        for k, v in d.items():
            val = str(v)[:80] if v else ''
            print(f'  {k}: {val}')
