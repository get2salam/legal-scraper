import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

base = Path('data_v2/federal_laws')
total = 0; has_body = 0; no_body = 0

# Read the JSONL
jsonl = base / 'all_federal_laws.jsonl'
if jsonl.exists():
    with open(jsonl, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                d = json.loads(line)
                total += 1
                body = d.get('body','') or d.get('text','') or d.get('content','') or d.get('full_text','') or ''
                if body and len(body.strip()) > 200:
                    has_body += 1
                else:
                    no_body += 1
            except: pass

print(f'Federal laws JSONL: {total} entries')
print(f'  Has body: {has_body} ({100*has_body//max(total,1)}%)')
print(f'  No body:  {no_body} ({100*no_body//max(total,1)}%)')

# Also check index.json
idx = base / 'index.json'
if idx.exists():
    data = json.load(open(idx, encoding='utf-8'))
    print(f'\nindex.json: {type(data).__name__}, {len(data) if isinstance(data,(list,dict)) else "?"} entries')
