import json, re, os, html

def strip_html(text):
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_judgment_text(data):
    jc = data.get('judgment_clean', '')
    if jc and len(jc) > 100:
        return jc
    jr = data.get('judgment_raw', '')
    if jr:
        return strip_html(jr)
    j = data.get('judgment', '')
    if j:
        try:
            j = json.loads(j)
        except:
            pass
        return strip_html(j)
    return ''

# Check unique PLD court patterns across a sample
pld_courts = set()
count = 0
for year_dir in sorted(os.listdir('data_v2/PLD'))[-5:]:  # last 5 years
    ypath = f'data_v2/PLD/{year_dir}'
    if not os.path.isdir(ypath):
        continue
    for fname in os.listdir(ypath):
        if not fname.endswith('.json'):
            continue
        d = json.load(open(f'{ypath}/{fname}', 'r', encoding='utf-8'))
        court = d.get('court', '')
        if court:
            pld_courts.add(court)
        count += 1

print(f"Checked {count} PLD files")
print("Unique courts in PLD:")
for c in sorted(pld_courts):
    print(f"  - '{c}'")

# Also check a sample of text for (YEAR) format
for rep in ['CLC', 'YLR', 'MLD']:
    years = sorted(os.listdir(f'data_v2/{rep}'))
    y = years[-2]
    fpath = f'data_v2/{rep}/{y}'
    files = sorted(os.listdir(fpath))[:3]
    for fname in files:
        d = json.load(open(f'{fpath}/{fname}', 'r', encoding='utf-8'))
        text = get_judgment_text(d)
        # Look for (YEAR) format
        matches = re.findall(r'\(\d{4}\)\s+\w+\s+\d+', text)
        if matches:
            print(f"\n{rep}/{y}/{fname}: (YEAR) format citations:")
            for m in matches[:5]:
                print(f"  - {m}")
            break
