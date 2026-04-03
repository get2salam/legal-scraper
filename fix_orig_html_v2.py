"""
Aggressive original HTML fixer — handles ALL edge cases:
1. Proper JSON-encoded (\u003c) → json.loads()
2. Quote-wrapped with unescaped quotes → strip + unescape
3. "-1" error responses → skip (no real content)
4. "Please Wait" error pages → skip
"""
import json, glob, os, time

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data_v2')
REPORTERS = ['SCMR', 'PLD', 'MLD', 'CLC', 'PCrLJ', 'PTD', 'PLC', 'YLR', 'CLD', 'GBLR']

def fix_file(filepath):
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        content = f.read()
    
    stripped = content.strip()
    
    # Already proper HTML
    if stripped.startswith('<') or stripped.startswith('<!'):
        return 'OK'
    
    # Skip error pages
    if stripped.startswith('Pakistan Law Site'):
        return 'ERROR_PAGE'
    
    # Not quote-wrapped
    if not stripped.startswith('"'):
        return 'UNKNOWN'
    
    # Try json.loads first (handles \u003c properly)
    try:
        decoded = json.loads(stripped)
        if isinstance(decoded, str):
            if decoded.strip() == '-1':
                return 'NEG1'
            if '<' in decoded[:200]:
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(decoded)
                return 'FIXED_JSON'
    except (json.JSONDecodeError, ValueError):
        pass
    
    # Strip outer quotes + unescape (handles unescaped internal quotes)
    if stripped.endswith('"'):
        inner = stripped[1:-1]
    else:
        inner = stripped[1:]  # No closing quote
    
    # Unescape common sequences
    inner = inner.replace('\\n', '\n').replace('\\t', '\t').replace('\\r', '\r')
    
    # Check for -1 pattern
    clean = inner.replace('\n', '').replace('\r', '').replace(' ', '').replace('"', '').replace('-1', '')
    if clean == '':
        return 'NEG1'
    
    if '<' in inner[:300] or '<html' in inner.lower()[:500]:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(inner)
        return 'FIXED_STRIP'
    
    # Binary/corrupt content
    return 'CORRUPT'


def main():
    start = time.time()
    stats = {'OK': 0, 'FIXED_JSON': 0, 'FIXED_STRIP': 0, 'NEG1': 0, 'ERROR_PAGE': 0, 'CORRUPT': 0, 'UNKNOWN': 0}
    
    files = []
    for r in REPORTERS:
        files.extend(glob.glob(os.path.join(DATA_DIR, r, '*', 'original', '*.html')))
    
    total = len(files)
    print(f"Found {total:,} original HTML files")
    
    for i, f in enumerate(files):
        result = fix_file(f)
        stats[result] = stats.get(result, 0) + 1
        if (i + 1) % 10000 == 0:
            fixed = stats['FIXED_JSON'] + stats['FIXED_STRIP']
            print(f"  {i+1:,}/{total:,} — {fixed:,} fixed")
    
    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.0f}s")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v:,}")
    fixed = stats['FIXED_JSON'] + stats['FIXED_STRIP']
    print(f"\n  TOTAL FIXED: {fixed:,}")
    print(f"  TOTAL OK: {stats['OK']:,}")

if __name__ == '__main__':
    main()
