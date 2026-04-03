import json, os

sample = 'data_v2/legislation/A'
files = [f for f in os.listdir(sample) if f.endswith('.json')][:2]
for fn in files:
    with open(os.path.join(sample, fn), encoding='utf-8') as f:
        data = json.load(f)
    print(f'=== {fn} ===')
    print(f'Keys: {list(data.keys())}')
    secs = data.get('sections', [])
    print(f'Sections: {len(secs)}')
    if secs:
        s = secs[0]
        print(f'  Sample section keys: {list(s.keys())}')
        print(f'  Section title: {str(s.get("title", "N/A"))[:80]}')
    lc = data.get('linked_cases', data.get('cases', []))
    print(f'Linked cases: {len(lc)}')
    print(f'Title: {str(data.get("title", "N/A"))[:80]}')
    print()
