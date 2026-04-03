import json
from pathlib import Path

data_dir = Path(r'C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2\legislation')

short_texts = []
neg1_count = 0
empty_count = 0
valid_short = 0

for f in sorted((data_dir / 'A').glob('*.json')):
    data = json.load(open(f, encoding='utf-8'))
    for s in data.get('sections', []):
        text = s.get('text', '').strip()
        if text == '-1':
            neg1_count += 1
        elif text == '':
            empty_count += 1
        elif len(text) < 10:
            short_texts.append(f"{f.stem[:50]} | sec {s.get('number','?')} | text={repr(text[:50])}")

print(f"A: Sections with text='-1': {neg1_count}")
print(f"A: Sections with empty text: {empty_count}")
print(f"A: Sections with 1-9 chars: {len(short_texts)}")
print(f"\nSample short texts (first 30):")
for t in short_texts[:30]:
    print(f"  {t}")

# Same for B
short_b = []
neg1_b = 0
empty_b = 0
for f in sorted((data_dir / 'B').glob('*.json')):
    data = json.load(open(f, encoding='utf-8'))
    for s in data.get('sections', []):
        text = s.get('text', '').strip()
        if text == '-1':
            neg1_b += 1
        elif text == '':
            empty_b += 1
        elif len(text) < 10:
            short_b.append(f"{f.stem[:50]} | sec {s.get('number','?')} | text={repr(text[:50])}")

print(f"\nB: Sections with text='-1': {neg1_b}")
print(f"B: Sections with empty text: {empty_b}")
print(f"B: Sections with 1-9 chars: {len(short_b)}")
print(f"\nSample short texts B (first 15):")
for t in short_b[:15]:
    print(f"  {t}")
