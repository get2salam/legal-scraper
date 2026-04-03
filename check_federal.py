import json, os

base = 'data_v2/federal_laws'
p = os.path.join(base, 'progress.json')
if os.path.exists(p):
    d = json.load(open(p, encoding='utf-8'))
    print(f"Phase: {d.get('current_phase', '?')}")
    print(f"Laws discovered: {d.get('total_laws', '?')}")
    comp = d.get('completed_slugs', d.get('completed_pdf_urls', d.get('completed_downloads', {})))
    if isinstance(comp, dict):
        print(f"Completed: {len(comp)}")
    elif isinstance(comp, list):
        print(f"Completed: {len(comp)}")
    for k, v in d.items():
        if k not in ('completed_slugs', 'completed_pdf_urls', 'completed_downloads', 'laws'):
            print(f"  {k}: {v}")
else:
    print("No progress.json yet")

# Count actual files
jsons = pdfs = txts = htmls = 0
for root, dirs, files in os.walk(base):
    for f in files:
        if f.endswith('.json') and 'progress' not in f and 'index' not in f:
            jsons += 1
        elif f.endswith('.pdf'):
            pdfs += 1
        elif f.endswith('.txt'):
            txts += 1
        elif f.endswith('.html'):
            htmls += 1

print(f"\nFiles on disk:")
print(f"  JSON: {jsons}")
print(f"  PDF:  {pdfs}")
print(f"  TXT:  {txts}")
print(f"  HTML: {htmls}")

jl = os.path.join(base, 'all_federal_laws.jsonl')
if os.path.exists(jl):
    lines = sum(1 for _ in open(jl, encoding='utf-8'))
    print(f"  JSONL: {lines} lines")
