import json, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

idx = json.load(open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/data_v2/federal_laws/index.json', encoding='utf-8'))
print(f"Index has {idx['total_laws']} laws")
print(f"Types: {idx['types']}")
print(f"Categories: {len(idx['categories'])}")

# Count laws with PDF URLs
with_pdf = sum(1 for l in idx['laws'] if l.get('pdf_url'))
print(f"Laws with PDF URL: {with_pdf}")

# Show first law with pdf_url
for law in idx['laws']:
    if law.get('pdf_url'):
        print(f"\nSample law with PDF URL:")
        print(json.dumps(law, indent=2, ensure_ascii=False))
        break
