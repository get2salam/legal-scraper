"""
Fix legislation format gaps for already-scraped letters (A, B, C).
1. Decode JSON-encoded original HTML files
2. Generate missing original HTML from JSON data
3. Generate missing readable HTML (dark theme)
"""
import json, glob, os, re, time
from pathlib import Path

DATA_DIR = Path(__file__).parent / 'data_v2' / 'legislation'


def fix_json_encoded_html(filepath):
    """Decode JSON-encoded or quote-wrapped HTML file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if not content.startswith('"'):
        return False
    
    # Try proper JSON decode first
    try:
        decoded = json.loads(content)
        if isinstance(decoded, str) and ('<' in decoded or '&lt;' in decoded):
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(decoded)
            return True
    except (json.JSONDecodeError, ValueError):
        pass
    
    # Check if content is only "-1" responses (PLS unavailable sections)
    # Pattern: '"-1"\n\n\n"-1"\n\n\n"-1"...' or just '"-1"'
    import re as _re
    cleaned = _re.sub(r'[\s"\\n\\r\\t-]', '', content)
    cleaned_digits = _re.sub(r'[^0-9a-zA-Z<>]', '', content)
    if cleaned_digits.replace('1', '') == '' or set(content.replace('\n', '').replace('\r', '').replace(' ', '')) <= {'"', '-', '1'}:
        return 'UNAVAILABLE'
    
    # Fallback: strip outer quotes and unescape sequences
    # PLS legislation returns HTML wrapped in quotes with \n escapes but unescaped internal quotes
    stripped = content.strip()
    if stripped.startswith('"') and stripped.endswith('"'):
        inner = stripped[1:-1]
        # If it's just "-1" (PLS unavailable), return special marker
        if inner.replace('\n', '').replace('\r', '').replace(' ', '').replace('"', '').replace('-1', '') == '':
            return 'UNAVAILABLE'
        inner = inner.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
        if '<' in inner[:200]:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(inner)
            return True
    return False


def generate_original_html(json_path, orig_path):
    """Generate original HTML from JSON data."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # If full_text exists and has content, use it
    full_text = data.get('full_text', '')
    if full_text and '<' in full_text:
        # Decode if JSON-encoded
        if full_text.startswith('"'):
            try:
                full_text = json.loads(full_text)
            except:
                pass
        orig_path.parent.mkdir(parents=True, exist_ok=True)
        with open(orig_path, 'w', encoding='utf-8') as f:
            f.write(full_text)
        return True
    
    # Build from sections
    sections = data.get('sections', [])
    parts = [f"<h1>{data.get('title', '')}</h1>"]
    if data.get('enactment_date'):
        parts.append(f"<p><b>Enacted:</b> {data['enactment_date']}</p>")
    if data.get('jurisdiction'):
        parts.append(f"<p><b>Jurisdiction:</b> {data['jurisdiction']}</p>")
    
    has_content = False
    for sec in sections:
        sec_num = sec.get('number', '')
        sec_title = sec.get('title', '')
        sec_text = sec.get('text', '')
        parts.append(f"<h3>Section {sec_num}: {sec_title}</h3>")
        if sec_text:
            parts.append(f"<p>{sec_text}</p>")
            has_content = True
        else:
            parts.append("<p><em>[Content not available on source]</em></p>")
    
    orig_path.parent.mkdir(parents=True, exist_ok=True)
    with open(orig_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(parts))
    return True


def generate_readable_html(json_path, readable_path):
    """Generate dark-theme readable HTML from JSON data."""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    title = data.get('title', 'Untitled')
    jurisdiction = data.get('jurisdiction', 'N/A')
    enacted = data.get('enactment_date', 'N/A')
    status = data.get('status', 'in_force')
    sections = data.get('sections', [])
    
    sections_html = ""
    for sec in sections:
        sec_num = sec.get('number', '')
        sec_title = sec.get('title', '')
        sec_text = sec.get('text', '')
        case_links = sec.get('case_links', [])
        
        sections_html += f'<div class="section"><h3>Section {sec_num}'
        if sec_title:
            sections_html += f': {sec_title}'
        sections_html += '</h3>'
        
        if sec_text:
            sections_html += f'<div class="section-text">{sec_text}</div>'
        else:
            sections_html += '<p class="unavailable"><em>[Content not available on source]</em></p>'
        
        if case_links:
            sections_html += '<div class="case-links"><h4>Related Cases</h4><ul>'
            for cl in case_links:
                cit = cl.get('citation', cl.get('text', ''))
                sections_html += f'<li>{cit}</li>'
            sections_html += '</ul></div>'
        sections_html += '</div>'
    
    readable = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px;
       line-height: 1.8; color: #e0e0e0; background: #1a1a2e; }}
h1 {{ color: #64ffda; font-size: 1.5em; border-bottom: 2px solid #64ffda; padding-bottom: 10px; }}
h3 {{ color: #bb86fc; margin-top: 30px; }}
h4 {{ color: #03dac6; font-size: 0.9em; }}
.meta {{ color: #888; margin-bottom: 20px; padding: 15px; background: #16213e; border-radius: 8px; }}
.section {{ margin: 20px 0; padding: 15px; border-left: 3px solid #333; }}
.section:hover {{ border-left-color: #64ffda; }}
.section-text {{ white-space: pre-wrap; margin: 10px 0; }}
.unavailable {{ color: #666; font-style: italic; }}
.case-links {{ margin-top: 10px; padding: 10px; background: #0d1b2a; border-radius: 5px; }}
.case-links ul {{ margin: 5px 0; padding-left: 20px; }}
.case-links li {{ color: #03dac6; font-size: 0.9em; }}
</style>
</head>
<body>
<h1>{title}</h1>
<div class="meta">
<strong>Jurisdiction:</strong> {jurisdiction}<br>
<strong>Enacted:</strong> {enacted}<br>
<strong>Sections:</strong> {len(sections)}<br>
<strong>Status:</strong> {status}
</div>
{sections_html}
</body>
</html>"""
    
    readable_path.parent.mkdir(parents=True, exist_ok=True)
    with open(readable_path, 'w', encoding='utf-8') as f:
        f.write(readable)
    return True


def main():
    start = time.time()
    print("=" * 60)
    print("LEGISLATION FORMAT FIXER")
    print("=" * 60)
    
    orig_decoded = 0
    orig_generated = 0
    readable_generated = 0
    
    for letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
        letter_dir = DATA_DIR / letter
        if not letter_dir.exists():
            continue
        
        json_files = list(letter_dir.glob('*.json'))
        if not json_files:
            continue
        
        print(f"\n--- Letter {letter}: {len(json_files)} statutes ---")
        
        for jf in json_files:
            safe_name = jf.stem
            orig_path = letter_dir / 'original' / f'{safe_name}.html'
            readable_path = DATA_DIR / 'html' / letter / f'{safe_name}.html'
            
            # Fix original HTML — if it doesn't start with <, fix it
            if orig_path.exists():
                with open(orig_path, 'r', encoding='utf-8') as fh:
                    first_chars = fh.read(10).strip()
                
                if first_chars.startswith('<'):
                    pass  # Already proper HTML
                else:
                    # Try decode first
                    result = fix_json_encoded_html(orig_path)
                    if result == 'UNAVAILABLE' or not result:
                        # Regenerate from JSON
                        try:
                            if generate_original_html(jf, orig_path):
                                orig_generated += 1
                        except Exception as e:
                            print(f"  Error regenerating orig for {safe_name}: {e}")
                    elif result is True:
                        orig_decoded += 1
            else:
                # Generate missing original HTML
                try:
                    if generate_original_html(jf, orig_path):
                        orig_generated += 1
                except Exception as e:
                    print(f"  Error generating orig for {safe_name}: {e}")
            
            # Generate missing readable HTML
            if not readable_path.exists():
                try:
                    if generate_readable_html(jf, readable_path):
                        readable_generated += 1
                except Exception as e:
                    print(f"  Error generating readable for {safe_name}: {e}")
        
        # Count results for this letter
        jsons = len(json_files)
        origs = len(list((letter_dir / 'original').glob('*.html'))) if (letter_dir / 'original').exists() else 0
        reads = len(list((DATA_DIR / 'html' / letter).glob('*.html'))) if (DATA_DIR / 'html' / letter).exists() else 0
        print(f"  JSON={jsons} | OrigHTML={origs} | ReadableHTML={reads}")
    
    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"COMPLETE in {elapsed:.1f}s")
    print(f"  Original HTML decoded: {orig_decoded}")
    print(f"  Original HTML generated: {orig_generated}")
    print(f"  Readable HTML generated: {readable_generated}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
