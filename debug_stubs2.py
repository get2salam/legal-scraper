import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

data_dir = Path('data_v2')

# Check what 19-byte full_text actually contains
jsonl_a = data_dir / 'legislation_A.jsonl'
with open(jsonl_a, encoding='utf-8') as f:
    for line in f:
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
            ft = rec.get('full_text','')
            if ft and len(ft) <= 50:
                print(f'Short full_text ({len(ft)}): {repr(ft[:100])}  | Title: {rec.get("title","")[:60]}')
        except:
            pass

print()

# Check if sections have any text
print('Checking sections field in stubs...')
for f in sorted(Path('data_v2/legislation/A').glob('*.json'))[:5]:
    try:
        d = json.load(open(f, encoding='utf-8'))
        sections = d.get('sections', [])
        ft = d.get('full_text','')
        title = d.get('title','')
        print(f'\n{title[:60]}')
        print(f'  full_text: {len(ft)} chars')
        print(f'  sections: {len(sections)} items')
        if sections:
            print(f'  First section: {repr(str(sections[0])[:150])}')
    except Exception as e:
        print(f'Error: {e}')

print()
# Check missing_legislation_registry for context
reg = data_dir / 'missing_legislation_registry.json'
if reg.exists():
    d = json.load(open(reg, encoding='utf-8'))
    print(f'missing_legislation_registry keys: {list(d.keys())[:10]}')
    if isinstance(d, list):
        print(f'  Records: {len(d)}')
        if d:
            print(f'  First: {json.dumps(d[0], indent=2)[:300]}')
    elif isinstance(d, dict):
        for k, v in list(d.items())[:3]:
            print(f'  {k}: {repr(str(v)[:100])}')
