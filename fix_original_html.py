"""
Fix original HTML files that were saved as JSON-encoded strings.
PLS API returns HTML wrapped as JSON string with \\u003c escapes.
This script decodes them to proper HTML.

Also regenerates readable HTML files from the decoded content.
"""
import json
import glob
import os
import re
import sys
import time

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data_v2')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

READABLE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{citation}</title>
<style>
body {{ font-family: Georgia, serif; max-width: 900px; margin: 40px auto; padding: 0 20px;
       line-height: 1.8; color: #e0e0e0; background: #1a1a2e; }}
h1, h2, h3 {{ color: #64ffda; }}
.meta {{ color: #888; margin-bottom: 20px; border-bottom: 1px solid #333; padding-bottom: 10px; }}
.judgment {{ white-space: pre-wrap; }}
.headnotes {{ background: #16213e; padding: 15px; border-radius: 8px; margin: 20px 0; }}
</style>
</head>
<body>
<h1>{citation}</h1>
<div class="meta">
<strong>Court:</strong> {court}<br>
<strong>Judges:</strong> {judges}<br>
<strong>Date:</strong> {date_decided}
</div>
{headnotes_html}
<div class="judgment">{judgment_raw}</div>
</body>
</html>"""


def fix_original_html(filepath):
    """Decode a JSON-encoded HTML file to proper HTML. Returns True if fixed."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception:
        return False

    if not content.startswith('"'):
        return False  # Already OK

    try:
        decoded = json.loads(content)
        if isinstance(decoded, str) and ('<' in decoded or '&lt;' in decoded):
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(decoded)
            return True
    except (json.JSONDecodeError, ValueError):
        pass
    return False


def fix_readable_html(orig_path, reporter, year, citation_safe):
    """Regenerate readable HTML from the (now fixed) original HTML and JSON data."""
    # Find corresponding JSON file
    json_dir = os.path.join(DATA_DIR, reporter, str(year))
    json_path = os.path.join(json_dir, f"{citation_safe}.json")
    
    if not os.path.exists(json_path):
        return False
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            case_data = json.load(f)
    except Exception:
        return False

    # Read the fixed original HTML for judgment_raw
    try:
        with open(orig_path, 'r', encoding='utf-8') as f:
            judgment_raw = f.read()
    except Exception:
        return False

    citation = case_data.get('citation', citation_safe.replace('_', ' '))
    court = case_data.get('court', '')
    judges = case_data.get('judges', '')
    date_decided = case_data.get('date_decided', '')
    headnotes = case_data.get('headnotes', '')
    
    headnotes_html = f'<div class="headnotes"><h3>Headnotes</h3>{headnotes}</div>' if headnotes else ''

    readable = READABLE_TEMPLATE.format(
        citation=citation,
        court=court,
        judges=judges,
        date_decided=date_decided,
        headnotes_html=headnotes_html,
        judgment_raw=judgment_raw,
    )

    # Write readable HTML
    html_dir = os.path.join(DATA_DIR, 'html', reporter, str(year))
    os.makedirs(html_dir, exist_ok=True)
    readable_path = os.path.join(html_dir, f"{citation_safe}.html")
    
    with open(readable_path, 'w', encoding='utf-8') as f:
        f.write(readable)
    return True


def fix_json_judgment_raw(json_path):
    """Fix judgment_raw field inside JSON files if it's JSON-encoded."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return False

    raw = data.get('judgment_raw', '')
    if not raw or not raw.startswith('"'):
        return False

    try:
        decoded = json.loads(raw)
        if isinstance(decoded, str) and '<' in decoded:
            data['judgment_raw'] = decoded
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            return True
    except (json.JSONDecodeError, ValueError):
        pass
    return False


def main():
    start = time.time()
    
    # Phase 1: Fix original HTML files
    print("=" * 60)
    print("PHASE 1: Fixing original HTML files")
    print("=" * 60)
    
    orig_fixed = 0
    orig_ok = 0
    orig_err = 0
    
    orig_files = glob.glob(os.path.join(DATA_DIR, '*', '*', 'original', '*.html'))
    total = len(orig_files)
    print(f"Found {total:,} original HTML files")
    
    for i, fpath in enumerate(orig_files):
        if (i + 1) % 5000 == 0:
            print(f"  Progress: {i+1:,}/{total:,} ({orig_fixed:,} fixed)")
        
        result = fix_original_html(fpath)
        if result:
            orig_fixed += 1
        elif result is False:
            # Check if it was already OK or an error
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    c = f.read(5)
                if c.startswith('"'):
                    orig_err += 1
                else:
                    orig_ok += 1
            except:
                orig_err += 1
    
    print(f"\n  Fixed: {orig_fixed:,}")
    print(f"  Already OK: {orig_ok:,}")
    print(f"  Errors: {orig_err:,}")
    
    # Phase 2: Fix JSON judgment_raw fields
    print("\n" + "=" * 60)
    print("PHASE 2: Fixing JSON judgment_raw fields")
    print("=" * 60)
    
    json_fixed = 0
    json_files = []
    for reporter in REPORTERS:
        json_files.extend(glob.glob(os.path.join(DATA_DIR, reporter, '*', '*.json')))
    
    total_json = len(json_files)
    print(f"Found {total_json:,} JSON files")
    
    for i, fpath in enumerate(json_files):
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i+1:,}/{total_json:,} ({json_fixed:,} fixed)")
        result = fix_json_judgment_raw(fpath)
        if result:
            json_fixed += 1
    
    print(f"\n  Fixed: {json_fixed:,}")
    
    # Phase 3: Regenerate readable HTML
    print("\n" + "=" * 60)
    print("PHASE 3: Regenerating readable HTML")
    print("=" * 60)
    
    readable_fixed = 0
    for i, fpath in enumerate(orig_files):
        if (i + 1) % 5000 == 0:
            print(f"  Progress: {i+1:,}/{len(orig_files):,} ({readable_fixed:,} regenerated)")
        
        # Parse path: data_v2/REPORTER/YEAR/original/CITATION.html
        parts = fpath.replace('\\', '/').split('/')
        try:
            idx = parts.index('original')
            reporter = parts[idx - 2]
            year = parts[idx - 1]
            citation_safe = os.path.splitext(parts[idx + 1])[0]
            
            if fix_readable_html(fpath, reporter, int(year), citation_safe):
                readable_fixed += 1
        except (ValueError, IndexError):
            continue
    
    print(f"\n  Regenerated: {readable_fixed:,}")
    
    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"COMPLETE in {elapsed:.0f}s")
    print(f"  Original HTML fixed: {orig_fixed:,}")
    print(f"  JSON judgment_raw fixed: {json_fixed:,}")
    print(f"  Readable HTML regenerated: {readable_fixed:,}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
