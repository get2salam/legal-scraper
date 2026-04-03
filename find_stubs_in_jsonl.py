import json, sys, re
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Goal: find the stub titles in the JSONL files and check if they have content there
# Load all stubs first
leg_dir = Path('data_v2/legislation')
stubs = {}  # normalized_title -> file path
for letter_dir in sorted(leg_dir.iterdir()):
    if not letter_dir.is_dir() or letter_dir.name in ['audit','html','original']:
        continue
    for f in sorted(letter_dir.glob('*.json')):
        try:
            d = json.load(open(f, encoding='utf-8'))
            if not isinstance(d, dict): continue
            sections = d.get('sections', [])
            all_unavail = all(s.get('content_status') == 'unavailable' for s in sections) if sections else True
            if all_unavail and sections:
                title = (d.get('title') or '').strip().upper()
                stubs[title] = str(f)
        except:
            pass

print(f'Total stubs: {len(stubs)}')
print('Sample stub titles:')
for t in list(stubs.keys())[:10]:
    print(f'  {t}')

# Now scan JSONL files for these titles
print()
found_in_jsonl = {}
jsonl_files = sorted(Path('data_v2').glob('legislation_*.jsonl'))
print(f'JSONL files to scan: {len(jsonl_files)}')

for jf in jsonl_files:
    letter = jf.stem.replace('legislation_', '')
    if len(letter) != 1:
        continue
    print(f'Scanning {jf.name}...')
    with open(jf, encoding='utf-8', errors='replace') as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                d = json.loads(line)
                title = (d.get('title') or '').strip().upper()
                if title in stubs:
                    full_text = d.get('full_text','') or ''
                    sections = d.get('sections', [])
                    # Check if it has real content
                    good_sections = [s for s in sections if s.get('text','') and '[Content not available' not in s.get('text','') and len(s.get('text','').strip()) > 50]
                    if good_sections:
                        found_in_jsonl[title] = {
                            'stub_file': stubs[title],
                            'jsonl_file': str(jf),
                            'section_count': len(good_sections),
                            'preview': good_sections[0].get('text','')[:200]
                        }
                        print(f'  FOUND: {title[:60]} ({len(good_sections)} sections)')
            except Exception as e:
                pass

print(f'\nTotal stubs found in JSONL with content: {len(found_in_jsonl)}')
if found_in_jsonl:
    # Save the mapping
    out = Path('stubs_in_jsonl.json')
    json.dump(found_in_jsonl, open(out, 'w', encoding='utf-8'), indent=2, ensure_ascii=False)
    print(f'Saved to {out}')
