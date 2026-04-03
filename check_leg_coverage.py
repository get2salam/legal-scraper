import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

leg_dir = Path('data_v2/legislation')
total = 0; has_body = 0; no_body = 0; short_body = 0; stubs = []

for f in leg_dir.glob('*.json'):
    try:
        raw = json.load(open(f, encoding='utf-8'))
        d = raw if isinstance(raw, dict) else (raw[0] if isinstance(raw, list) and raw else {})
        total += 1
        body = d.get('body','') or d.get('text','') or d.get('content','') or ''
        body = body.strip()
        if not body:
            no_body += 1
            stubs.append(f'EMPTY: {f.name} | {d.get("title","")[:50]}')
        elif len(body) < 200:
            short_body += 1
            stubs.append(f'SHORT({len(body)}): {f.name} | {body[:60]}')
        else:
            has_body += 1
    except Exception as e:
        stubs.append(f'ERROR: {f.name} - {e}')

print(f'Total: {total}')
print(f'Has body (>=200 chars): {has_body} ({100*has_body//total}%)')
print(f'Short body (<200 chars): {short_body}')
print(f'Empty body: {no_body}')
print(f'\nProblematic files:')
for s in stubs[:20]:
    print(f'  {s}')
