import json, re, os, html

def strip_html(text):
    """Strip HTML tags and decode entities."""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def get_judgment_text(data):
    """Get clean judgment text from case data."""
    # Try judgment_clean first
    jc = data.get('judgment_clean', '')
    if jc and len(jc) > 100:
        return jc
    # Fall back to judgment_raw
    jr = data.get('judgment_raw', '')
    if jr:
        return strip_html(jr)
    # Fall back to judgment (may be JSON-encoded HTML)
    j = data.get('judgment', '')
    if j:
        try:
            j = json.loads(j)  # it might be double-encoded
        except:
            pass
        return strip_html(j)
    return ''

# Check a larger case that likely cites many cases
# SC cases tend to cite heavily
test_files = [
    'data_v2/SCMR/2024/2024_SCMR_101.json',
    'data_v2/PLD/2024/2024_PLD_100.json',
    'data_v2/SCMR/2020/2020_SCMR_1.json',
]

patterns = [
    # PLD YEAR COURT PAGE: "PLD 2024 SC 123", "PLD 2024 Lahore 123"
    r'PLD\s+\d{4}\s+(?:SC|Supreme\s+Court|Lahore|Karachi|Peshawar|Quetta|Islamabad|FSC|AJK|Federal\s+Shariat\s+Court)\s+\d+',
    # YEAR REPORTER PAGE: "2024 SCMR 456"
    r'\d{4}\s+(?:SCMR|PCrLJ|PCr\.?LJ|MLD|CLC|YLR|PTD|PLC|CLD|GBLR|NLR|PLJ)\s+\d+',
    # (YEAR) REPORTER PAGE
    r'\(\d{4}\)\s+(?:SCMR|PCrLJ|MLD|CLC|YLR|PTD|PLC|CLD|GBLR|NLR|PLJ)\s+\d+',
    # PLD YEAR PAGE (without court - some references)
    r'PLD\s+\d{4}\s+\d+',
]

for fpath in test_files:
    if not os.path.exists(fpath):
        continue
    d = json.load(open(fpath, 'r', encoding='utf-8'))
    text = get_judgment_text(d)
    print(f"\n=== {fpath} (text len: {len(text)}) ===")
    print(f"Citation: {d.get('citation')}")
    
    found = []
    for pat in patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        if matches:
            found.extend(matches[:5])
    
    if found:
        print(f"Found {len(found)} sample citations:")
        for c in found[:15]:
            print(f"  - {c}")
    else:
        print("No citations found in this case")
