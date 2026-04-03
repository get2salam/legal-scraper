import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Check what's in the legislation JSONL files - these might have bodies!
f = Path('data_v2/legislation_A.jsonl')
count = 0
has_body = 0
with open(f, encoding='utf-8', errors='replace') as fh:
    for line in fh:
        if not line.strip():
            continue
        try:
            d = json.loads(line)
            count += 1
            body = d.get('body','') or d.get('text','') or d.get('full_text','') or ''
            if len(body.strip()) > 100:
                has_body += 1
            if count <= 3:
                print(f'Keys: {list(d.keys())}')
                title = d.get('title','')
                print(f'Title: {title}')
                print(f'Body len: {len(body)}')
                print(f'Body preview: {body[:200]}')
                print()
        except Exception as e:
            pass
        if count >= 100:
            break

print(f'Checked {count} records, {has_body} have body text')
