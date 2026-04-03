import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

data_dir = Path('data_v2')
leg_dir = data_dir / 'legislation'

# Check a few JSONL titles vs stub titles
stub_titles_sample = set()
for f in list(leg_dir.rglob('*.json'))[:100]:
    rel = f.relative_to(leg_dir)
    if len(rel.parts) == 1:
        continue
    try:
        d = json.load(open(f, encoding='utf-8'))
        title = d.get('title','')
        if title:
            full_text = d.get('full_text','') or d.get('body','') or d.get('text','')
            if not full_text or len(str(full_text).strip()) < 100:
                stub_titles_sample.add(title.upper().strip())
    except:
        pass

print(f'Sample stub titles ({len(stub_titles_sample)}):')
for t in sorted(stub_titles_sample)[:10]:
    print(f'  {t}')

print()

# Check JSONL titles
jsonl_a = data_dir / 'legislation_A.jsonl'
print(f'Sample JSONL titles from {jsonl_a.name}:')
with open(jsonl_a, encoding='utf-8') as f:
    count = 0
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            title = rec.get('title','')
            full_text = rec.get('full_text','')
            print(f'  [{len(full_text) if full_text else 0}] {title[:60]}')
            count += 1
        except:
            pass
        if count >= 20:
            break

print()
# Check if there's overlap - search for any stub title in jsonl
print('Looking for overlaps...')
for t in sorted(stub_titles_sample)[:5]:
    print(f'  Stub: "{t}"')

# Scan JSONL A for any match
matched = []
with open(jsonl_a, encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            jt = rec.get('title','').upper().strip()
            if jt in stub_titles_sample:
                matched.append(jt)
        except:
            pass

print(f'\nMatches found in legislation_A.jsonl: {len(matched)}')
for m in matched[:5]:
    print(f'  {m}')
