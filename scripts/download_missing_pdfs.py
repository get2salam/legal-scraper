# download_missing_pdfs.py — Fix pdf_path mapping and download missing PDFs
import sys, json, time, urllib.request, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path

BASE = Path('data_v2/federal_laws')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Load laws
laws = []
with open(BASE / 'all_federal_laws.jsonl', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                laws.append(json.loads(line))
            except Exception:
                pass
print(f'Loaded: {len(laws)} laws')

# Build index of existing PDFs by filename
existing_by_name = {}
for p in BASE.rglob('*.pdf'):
    existing_by_name[p.name] = p

print(f'Existing PDFs on disk: {len(existing_by_name)}')

# For each law, resolve correct pdf path
correct_paths = {}  # law slug -> absolute path
to_download = []

for law in laws:
    slug = law.get('slug', '')
    law_type = law.get('type', 'act')
    pdf_url = law.get('pdf_url', '')
    pdf_rel = law.get('pdf_path', '')  # e.g. "pdfs/xxx.pdf"

    # Get just the filename
    pdf_filename = Path(pdf_rel).name if pdf_rel else ''

    # Try to find it in existing PDFs
    if pdf_filename and pdf_filename in existing_by_name:
        correct_paths[slug] = existing_by_name[pdf_filename]
    else:
        # Not found - need to download
        if pdf_url:
            # Determine correct dir based on type
            type_dir = 'ordinances' if law_type == 'ordinance' else 'acts'
            dest_dir = BASE / type_dir / 'pdfs'
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest_path = dest_dir / (pdf_filename or f"{slug}.pdf")
            to_download.append((law, dest_path))

print(f'Already have PDF: {len(correct_paths)}')
print(f'Need to download: {len(to_download)}')

if to_download:
    print(f'\nDownloading {len(to_download)} missing PDFs...')
    downloaded = 0
    failed = 0
    failed_list = []

    for law, dest_path in to_download:
        pdf_url = law['pdf_url']
        try:
            req = urllib.request.Request(pdf_url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=20)
            content = resp.read()
            if len(content) > 1024 and content[:4] == b'%PDF':
                with open(dest_path, 'wb') as pf:
                    pf.write(content)
                downloaded += 1
                correct_paths[law.get('slug','')] = dest_path
                if downloaded % 10 == 0:
                    print(f'  Downloaded {downloaded}/{len(to_download)}...')
            else:
                failed += 1
                failed_list.append(f"{law.get('title','')} | {pdf_url} | size={len(content)} header={content[:8]!r}")
        except Exception as e:
            failed += 1
            failed_list.append(f"{law.get('title','')} | {pdf_url} | error={e}")

        time.sleep(1)

    print(f'Downloaded: {downloaded} | Failed: {failed}')

    if failed_list:
        fail_path = BASE / 'failed_downloads.txt'
        with open(fail_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(failed_list))
        print(f'Failed log: {fail_path}')
else:
    print('No downloads needed - all PDFs already on disk!')

# ── Final tally ──────────────────────────────────────────────────────────────
all_pdfs = list(BASE.rglob('*.pdf'))
print(f'\nTotal PDFs on disk now: {len(all_pdfs)}')
total_size = sum(p.stat().st_size for p in all_pdfs)
print(f'Total size: {total_size/1024/1024:.1f} MB')

# Print final breakdown
from collections import Counter
dirs = Counter(str(p.parent.relative_to(BASE)) for p in all_pdfs)
for d, c in sorted(dirs.items()):
    print(f'  {d}: {c} PDFs')

print('\nDone.')
