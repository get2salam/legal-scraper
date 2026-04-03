"""Check legislation data quality for A and B."""
import json
from pathlib import Path

for letter in ['A', 'B']:
    d = Path(f'data_v2/legislation/{letter}')
    files = sorted(d.glob('*.json'))
    
    total_sections = 0
    available = 0
    unavailable = 0
    empty = 0
    
    # Sample first file
    if files:
        with open(files[0], encoding='utf-8') as fh:
            data = json.load(fh)
        print(f"=== Sample: {files[0].name} ===")
        print(f"Keys: {list(data.keys())}")
        secs = data.get('sections', [])
        if secs:
            s = secs[0]
            print(f"Section keys: {list(s.keys())}")
            for key, val in s.items():
                if isinstance(val, str):
                    preview = val[:150].replace('\n', ' ')
                    print(f"  {key} ({len(val)} chars): {preview}")
        print()
    
    # Full count
    for f in files:
        with open(f, encoding='utf-8') as fh:
            data = json.load(fh)
        for s in data.get('sections', []):
            total_sections += 1
            content = s.get('content', '')
            text = s.get('text', '')
            actual = content or text
            if not actual or actual.strip() == '':
                empty += 1
            elif '[Content not available' in actual or actual.strip() == '-1':
                unavailable += 1
            else:
                available += 1
    
    print(f"{letter}: {len(files)} statutes, {total_sections} sections")
    print(f"  Available: {available}")
    print(f"  Unavailable: {unavailable}")
    print(f"  Empty: {empty}")
    print()
