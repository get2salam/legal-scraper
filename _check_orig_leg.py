import json

path = 'data_v2/legislation/A/original/Abandoned_Properties__Taking_over_and_Management__Act_1975.html'
with open(path, 'r', encoding='utf-8') as f:
    c = f.read()

print(f"Length: {len(c)}")
print(f"First char: {repr(c[0])}")
print(f"Last char: {repr(c[-1])}")
print(f"First 100: {repr(c[:100])}")

# Try json.loads
try:
    d = json.loads(c)
    print(f"json.loads: SUCCESS, type={type(d).__name__}, starts with: {str(d)[:80]}")
except Exception as e:
    print(f"json.loads: FAILED - {str(e)[:100]}")

# Try stripping outer quotes
if c.startswith('"'):
    # Find if it ends with quote
    stripped = c.strip()
    if stripped.endswith('"'):
        inner = stripped[1:-1]
        # Replace escaped sequences
        inner = inner.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"')
        print(f"\nAfter strip+unescape:")
        print(f"  Starts with: {repr(inner[:80])}")
        print(f"  Has <html: {'<html' in inner[:200]}")
        print(f"  Has <: {'<' in inner[:200]}")
