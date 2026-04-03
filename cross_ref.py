import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Strategy: Cross-reference stubs with legislation_*.jsonl files
# The JSONL files have full_text. The individual JSON files in legislation/A/, B/, etc. are stubs.
# We need to find if the JSONL files can fill the gaps.

data_dir = Path('data_v2')
leg_dir = data_dir / 'legislation'

# First, collect all stub titles
stubs_by_title = {}
for f in leg_dir.rglob('*.json'):
    rel = f.relative_to(leg_dir)
    parts = rel.parts
    if len(parts) == 1:  # system file
        continue
    try:
        d = json.load(open(f, encoding='utf-8'))
        title = d.get('title','')
        if not title:
            continue
        full_text = d.get('full_text','') or d.get('body','') or d.get('text','') or d.get('content','')
        if not full_text or len(str(full_text).strip()) < 100:
            stubs_by_title[title.upper().strip()] = {'file': str(f), 'title': title, 'data': d}
    except:
        pass

print(f'Stubs to fill: {len(stubs_by_title)}')

# Now scan legislation_*.jsonl files to find matching records
jsonl_files = sorted(data_dir.glob('legislation_*.jsonl'))
print(f'JSONL files to scan: {len(jsonl_files)}')

filled = 0
skipped_html = 0
not_found = 0

for jf in jsonl_files[:3]:  # Test first 3 files
    print(f'\nScanning {jf.name}...')
    matches = 0
    with open(jf, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                title = rec.get('title','').upper().strip()
                if title in stubs_by_title:
                    full_text = rec.get('full_text','')
                    if full_text and len(full_text) > 200:
                        # Check if it's real legislation text or HTML boilerplate
                        # Real text: contains legal keywords, not just HTML nav
                        text_lower = full_text.lower()
                        has_legal = any(kw in text_lower for kw in ['section', 'act', 'ordinance', 'shall', 'regulation', 'pursuant'])
                        is_html_only = full_text.strip().startswith('<html') or full_text.strip().startswith('<!DOCTYPE')
                        
                        if has_legal:
                            matches += 1
                            # We found content! Update the stub file.
                            stub_info = stubs_by_title[title]
                            d = stub_info['data']
                            d['full_text'] = full_text
                            d['source_jsonl'] = jf.name
                            with open(stub_info['file'], 'w', encoding='utf-8') as fout:
                                json.dump(d, fout, ensure_ascii=False, indent=2)
                            filled += 1
                            print(f'  FILLED: {stub_info["title"][:60]}')
                        else:
                            skipped_html += 1
            except Exception as e:
                pass
    print(f'  Matches found: {matches}')

print(f'\nResult: {filled} filled, {skipped_html} skipped (HTML only)')
