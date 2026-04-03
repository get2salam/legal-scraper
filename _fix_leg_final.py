"""Final pass: regenerate original HTML files that contain \\u003c or "-1" patterns."""
import glob, os, json, re
from pathlib import Path

DATA_DIR = Path(__file__).parent / 'data_v2' / 'legislation'
fixed = 0
ok = 0

for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
    letter_dir = DATA_DIR / letter
    if not letter_dir.exists():
        continue
    
    for orig in glob.glob(str(letter_dir / 'original' / '*.html')):
        with open(orig, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for problems: \\u003c, "-1", or starts with "
        has_problems = ('\\u003c' in content or 
                       '"\\u003c' in content or
                       content.strip().startswith('"') or
                       ('"-1"' in content and '<html' not in content.lower()))
        
        if not has_problems:
            ok += 1
            continue
        
        # Find corresponding JSON
        safe_name = os.path.splitext(os.path.basename(orig))[0]
        json_path = letter_dir / f'{safe_name}.json'
        if not json_path.exists():
            continue
        
        # Regenerate from JSON
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Try to use full_text if it has real HTML
        full_text = data.get('full_text', '')
        if full_text and not full_text.startswith('"') and '<' in full_text[:100] and '\\u003c' not in full_text:
            with open(orig, 'w', encoding='utf-8') as f:
                f.write(full_text)
            fixed += 1
            continue
        
        # Build from sections
        parts = [f"<h1>{data.get('title', '')}</h1>"]
        if data.get('enactment_date'):
            parts.append(f"<p><b>Enacted:</b> {data['enactment_date']}</p>")
        if data.get('jurisdiction'):
            parts.append(f"<p><b>Jurisdiction:</b> {data['jurisdiction']}</p>")
        
        for sec in data.get('sections', []):
            sec_num = sec.get('number', '')
            sec_title = sec.get('title', '')
            sec_text = sec.get('text', '')
            parts.append(f"<h3>Section {sec_num}: {sec_title}</h3>")
            if sec_text:
                parts.append(f"<div>{sec_text}</div>")
            else:
                parts.append("<p><em>[Content not available on source]</em></p>")
        
        with open(orig, 'w', encoding='utf-8') as f:
            f.write("\n".join(parts))
        fixed += 1

print(f"OK: {ok} | Fixed: {fixed}")
