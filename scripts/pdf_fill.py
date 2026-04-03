# pdf_fill.py — Cross-match PLS stubs with PakistanCode federal laws, download PDFs
import json, sys, time, re, urllib.request, os
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE = Path('data_v2')
FEDERAL_BASE = BASE / 'federal_laws'
LEG_BASE = BASE / 'legislation'
PDFS_DIR = FEDERAL_BASE / 'pdfs'
PDFS_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ── Load federal laws ──────────────────────────────────────────────────────────
print("Loading federal laws...")
laws = []
with open(FEDERAL_BASE / 'all_federal_laws.jsonl', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if line:
            try:
                laws.append(json.loads(line))
            except Exception:
                pass
print(f"  {len(laws)} laws loaded")

# ── Check existing PDFs ────────────────────────────────────────────────────────
has_pdf = 0
missing_pdf = 0
to_download = []

for law in laws:
    rel = law.get('pdf_path', '')
    if rel:
        pdf_path = FEDERAL_BASE / rel
        if pdf_path.exists() and pdf_path.stat().st_size > 1024:
            has_pdf += 1
        else:
            missing_pdf += 1
            if law.get('pdf_url'):
                to_download.append(law)
    else:
        missing_pdf += 1
        if law.get('pdf_url'):
            to_download.append(law)

print(f"  Already have PDF: {has_pdf}")
print(f"  Missing PDF: {missing_pdf}")
print(f"  Have pdf_url to download: {len(to_download)}")

# ── Download missing PDFs ──────────────────────────────────────────────────────
print(f"\nDownloading up to {len(to_download)} PDFs (all missing)...")
downloaded = 0
failed = 0
failed_urls = []

for i, law in enumerate(to_download):
    pdf_url = law['pdf_url']
    rel = law.get('pdf_path', '')
    if not rel:
        slug = re.sub(r'[^\w-]', '-', law.get('slug', law.get('title', 'unknown'))[:80])
        rel = f"pdfs/{slug}.pdf"
    pdf_path = FEDERAL_BASE / rel
    pdf_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        req = urllib.request.Request(pdf_url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=20)
        content = resp.read()
        if len(content) > 1024 and content[:4] == b'%PDF':
            with open(pdf_path, 'wb') as pf:
                pf.write(content)
            downloaded += 1
            if downloaded % 20 == 0:
                print(f"  Downloaded {downloaded}/{len(to_download)}...")
        else:
            failed += 1
            failed_urls.append(pdf_url)
    except Exception as e:
        failed += 1
        failed_urls.append(f"{pdf_url} -> {e}")

    time.sleep(1)  # polite delay

print(f"\nDownload results: {downloaded} downloaded | {failed} failed")

# ── Cross-match PLS legislation with federal laws ──────────────────────────────
print("\nCross-matching PLS legislation with federal laws...")

def normalize(s):
    """Normalize title for matching."""
    s = s.lower()
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    # Remove common suffixes
    s = re.sub(r'\b(act|ordinance|rules|regulations|order|schedule)\b', '', s)
    # Normalize year formats
    s = re.sub(r'\b(19|20)\d{2}\b', lambda m: m.group(0), s)
    return s.strip()

# Build lookup from federal laws
fed_by_norm = {}
for law in laws:
    title = law.get('title', '')
    norm = normalize(title)
    fed_by_norm[norm] = law

    # Also index by year+partial title for fuzzy matching
    year = law.get('year', '')
    words = norm.split()
    if len(words) >= 3:
        key3 = ' '.join(words[:3])
        if key3 not in fed_by_norm:
            fed_by_norm[key3] = law

print(f"  Federal law index: {len(fed_by_norm)} entries")

# Scan PLS legislation files
matched = 0
filled = 0
total_scanned = 0
match_log = []

for letter_dir in sorted(LEG_BASE.iterdir()):
    if not letter_dir.is_dir() or letter_dir.name in ('audit', 'html'):
        continue
    for json_file in sorted(letter_dir.glob('*.json')):
        total_scanned += 1
        try:
            with open(json_file, encoding='utf-8') as f:
                pls = json.load(f)
        except Exception:
            continue

        title = pls.get('title', '')
        if not title:
            continue

        norm_title = normalize(title)

        # Try exact match first
        fed_law = fed_by_norm.get(norm_title)

        # Try partial match if no exact
        if not fed_law:
            # Try matching first N words
            words = norm_title.split()
            for n in [5, 4, 3]:
                if len(words) >= n:
                    key = ' '.join(words[:n])
                    if key in fed_by_norm:
                        fed_law = fed_by_norm[key]
                        break

        if not fed_law:
            continue

        matched += 1

        # Check if PLS stub needs filling
        full_text = pls.get('full_text', '')
        sections = pls.get('sections', [])
        has_content = (
            (full_text and len(full_text) > 200) or
            any(s.get('text', '') and len(s.get('text', '')) > 100 and
                '[Content not available' not in s.get('text', '')
                for s in sections)
        )

        fed_text = fed_law.get('extracted_text', '')
        fed_pdf_rel = fed_law.get('pdf_path', '')
        fed_pdf_path = (FEDERAL_BASE / fed_pdf_rel) if fed_pdf_rel else None
        fed_pdf_exists = fed_pdf_path and fed_pdf_path.exists() and fed_pdf_path.stat().st_size > 1024

        match_log.append({
            'pls_file': str(json_file.relative_to(LEG_BASE)),
            'pls_title': title,
            'fed_title': fed_law.get('title', ''),
            'pls_has_content': has_content,
            'fed_has_text': bool(fed_text and len(fed_text) > 100),
            'fed_pdf_exists': fed_pdf_exists,
            'fed_pdf_path': fed_pdf_rel,
            'fed_pdf_url': fed_law.get('pdf_url', ''),
        })

        # Fill body if PLS stub is empty but federal law has text
        if not has_content and fed_text and len(fed_text) > 100:
            # Add extracted_text as full_text to PLS stub
            pls['full_text'] = fed_text
            pls['text_source'] = 'pakistancode.gov.pk'
            pls['pdf_url'] = fed_law.get('pdf_url', '')
            if fed_pdf_exists:
                pls['pdf_path'] = str(fed_pdf_path)

            with open(json_file, 'w', encoding='utf-8') as out:
                json.dump(pls, out, ensure_ascii=False, indent=2)
            filled += 1

print(f"  Total PLS files scanned: {total_scanned}")
print(f"  Matched to federal laws: {matched}")
print(f"  PLS stubs filled with text: {filled}")

# ── Summary stats ──────────────────────────────────────────────────────────────
stubs_empty = sum(1 for m in match_log if not m['pls_has_content'])
stubs_filled = sum(1 for m in match_log if not m['pls_has_content'] and m['fed_has_text'])
pdfs_available = sum(1 for m in match_log if m['fed_pdf_exists'])

print(f"\n── SUMMARY ──")
print(f"  Cross-matched: {matched}")
print(f"    PLS stubs without content: {stubs_empty}")
print(f"    Can fill from federal text: {stubs_filled}")
print(f"    Federal PDFs available: {pdfs_available}")
print(f"  PDFs downloaded this run: {downloaded} / {len(to_download)}")

# Save match log
log_path = FEDERAL_BASE / 'cross_match_log.jsonl'
with open(log_path, 'w', encoding='utf-8') as f:
    for entry in match_log:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')
print(f"\nMatch log saved: {log_path} ({len(match_log)} entries)")

if failed_urls:
    failed_path = FEDERAL_BASE / 'failed_downloads.txt'
    with open(failed_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(failed_urls))
    print(f"Failed URLs saved: {failed_path}")

print("\nDone.")
