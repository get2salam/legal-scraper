import glob, os, json

# Check a few bad files
bad_files = []
for letter in 'AB':
    for f in glob.glob(f'data_v2/legislation/{letter}/original/*.html'):
        with open(f, 'r', encoding='utf-8') as fh:
            start = fh.read(100)
        if start.startswith('"') or '\\u003c' in start:
            bad_files.append(f)
            if len(bad_files) <= 5:
                print(f"BAD: {os.path.basename(f)}")
                print(f"  First 80: {repr(start[:80])}")
                print(f"  Starts with quote: {start[0] == chr(34)}")
                # Try the fix
                with open(f, 'r', encoding='utf-8') as fh:
                    content = fh.read()
                stripped = content.strip()
                print(f"  Ends with quote: {stripped[-1] == chr(34)}")
                print(f"  Has \\u003c: {'\\\\u003c' in repr(content[:500])}")
                print()

print(f"\nTotal BAD: {len(bad_files)}")

# Check what the bad ones look like
if bad_files:
    with open(bad_files[0], 'r', encoding='utf-8') as f:
        c = f.read()
    # Try json.loads
    try:
        d = json.loads(c)
        print(f"json.loads on first bad: SUCCESS")
    except Exception as e:
        print(f"json.loads on first bad: {str(e)[:80]}")
    # Check if it has \u003c style encoding
    if '\\u003c' in c[:500]:
        print("Has literal \\u003c in content")
    if '\u003c' in c[:500]:
        print("Has unicode \\u003c (which is <)")
