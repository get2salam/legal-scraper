import sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from collections import Counter

base = Path('data_v2/federal_laws')
all_pdfs = list(base.rglob('*.pdf'))
print(f'Total PDFs found recursively: {len(all_pdfs)}')
dirs = Counter(str(p.parent.relative_to(base)) for p in all_pdfs)
for d, c in sorted(dirs.items()):
    print(f'  {d}: {c} PDFs')

laws = []
with open(base / 'all_federal_laws.jsonl', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                laws.append(json.loads(line))
            except Exception:
                pass

print()
print('Sample pdf_path values:')
for law in laws[:5]:
    pp = law.get('pdf_path', '')
    t = law.get('type', '')
    print(f'  type={t!r} pdf_path={pp!r}')
