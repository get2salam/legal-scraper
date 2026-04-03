# cross_match_pls_fed.py — Cross-match PLS stubs with federal laws (PDFs already downloaded)
import json, sys, re
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE = Path('data_v2')
FEDERAL_BASE = BASE / 'federal_laws'
LEG_BASE = BASE / 'legislation'

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
print(f"  {len(laws)} federal laws loaded")

# Count PDFs already on disk
pdf_count = len(list((FEDERAL_BASE / 'pdfs').rglob('*.pdf'))) if (FEDERAL_BASE / 'pdfs').exists() else 0
# Also check if PDFs might be elsewhere in federal_laws dir
all_fed_pdfs = len(list(FEDERAL_BASE.rglob('*.pdf')))
print(f"  PDFs on disk (pdfs/): {pdf_count}")
print(f"  PDFs on disk (all federal_laws/): {all_fed_pdfs}")

# ── Normalize helper ───────────────────────────────────────────────────────────
def normalize(s):
    s = s.lower()
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_loose(s):
    s = normalize(s)
    # Remove common suffixes/noise words for fuzzy matching
    s = re.sub(r'\b(the|act|ordinance|rules|regulations|order|schedule|pakistan|of|for|and|a|an)\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# ── Build lookup indexes ───────────────────────────────────────────────────────
print("\nBuilding federal law indexes...")
fed_by_norm = {}       # exact normalized title
fed_by_loose = {}      # loose (noise-removed) title
fed_by_slug = {}       # slug

for law in laws:
    title = law.get('title', '')
    slug = law.get('slug', '')
    year = law.get('year', '')

    norm = normalize(title)
    loose = normalize_loose(title)

    fed_by_norm[norm] = law
    if loose and loose not in fed_by_loose:
        fed_by_loose[loose] = law
    if slug:
        fed_by_slug[slug] = law

    # Also index year+first-3-words for partial matching
    words = norm.split()
    if year and len(words) >= 3:
        key = f"{year} {' '.join(words[:3])}"
        if key not in fed_by_norm:
            fed_by_norm[key] = law

print(f"  Exact index: {len(fed_by_norm)}")
print(f"  Loose index: {len(fed_by_loose)}")

# ── Scan PLS legislation files ─────────────────────────────────────────────────
print("\nScanning PLS legislation files...")
total_scanned = 0
matched = 0
filled = 0
already_have_content = 0
no_fed_text = 0
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
        loose_title = normalize_loose(title)

        # Try match strategies in order
        fed_law = None
        match_type = None

        # 1. Exact normalized match
        if norm_title in fed_by_norm:
            fed_law = fed_by_norm[norm_title]
            match_type = 'exact'

        # 2. Loose match (remove noise words)
        if not fed_law and loose_title and loose_title in fed_by_loose:
            fed_law = fed_by_loose[loose_title]
            match_type = 'loose'

        # 3. Slug match from filename
        if not fed_law:
            stem = json_file.stem.lower().replace('_', '-')
            if stem in fed_by_slug:
                fed_law = fed_by_slug[stem]
                match_type = 'slug'

        # 4. Partial match: first 4 words + year
        if not fed_law:
            words = norm_title.split()
            year_match = re.search(r'\b(19|20)\d{2}\b', title)
            year = year_match.group(0) if year_match else ''
            if year and len(words) >= 3:
                key = f"{year} {' '.join(words[:3])}"
                if key in fed_by_norm:
                    fed_law = fed_by_norm[key]
                    match_type = 'partial_year'

        if not fed_law:
            continue

        matched += 1

        # Check if PLS stub has content
        full_text = pls.get('full_text', '')
        sections = pls.get('sections', [])
        has_content = (
            (full_text and len(full_text.strip()) > 200) or
            any(
                s.get('text', '') and len(s.get('text', '').strip()) > 100 and
                '[Content not available' not in s.get('text', '') and
                '<html' not in s.get('text', '').lower()[:50]
                for s in sections
            )
        )

        fed_text = fed_law.get('extracted_text', '')
        fed_pdf_rel = fed_law.get('pdf_path', '')
        fed_pdf_abs = (FEDERAL_BASE / fed_pdf_rel) if fed_pdf_rel else None
        fed_pdf_ok = bool(fed_pdf_abs and fed_pdf_abs.exists() and fed_pdf_abs.stat().st_size > 1024)

        match_log.append({
            'pls_file': str(json_file.relative_to(LEG_BASE)),
            'pls_title': title,
            'fed_title': fed_law.get('title', ''),
            'match_type': match_type,
            'pls_has_content': has_content,
            'fed_has_text': bool(fed_text and len(fed_text.strip()) > 100),
            'fed_text_len': len(fed_text) if fed_text else 0,
            'fed_pdf_ok': fed_pdf_ok,
            'fed_pdf_path': fed_pdf_rel,
            'fed_pdf_url': fed_law.get('pdf_url', ''),
            'fed_source_url': fed_law.get('source_url', ''),
        })

        if has_content:
            already_have_content += 1
            continue

        # Fill body if PLS stub is empty but federal law has text
        if fed_text and len(fed_text.strip()) > 100:
            pls['full_text'] = fed_text
            pls['text_source'] = 'pakistancode.gov.pk'
            pls['pdf_url'] = fed_law.get('pdf_url', '')
            if fed_pdf_ok:
                pls['local_pdf_path'] = str(fed_pdf_abs)

            with open(json_file, 'w', encoding='utf-8') as out:
                json.dump(pls, out, ensure_ascii=False, indent=2)
            filled += 1
        else:
            no_fed_text += 1

    letter = letter_dir.name
    if letter in 'ACFHKLNOSWZ':  # Print progress at key letters
        print(f"  [{letter}] scanned {total_scanned}, matched {matched}, filled {filled}")

# ── Final stats ────────────────────────────────────────────────────────────────
print(f"\n── RESULTS ──────────────────────────────────────────────")
print(f"  PLS files scanned:            {total_scanned}")
print(f"  Matched to federal law:       {matched}")
print(f"    Already had content:        {already_have_content}")
print(f"    Filled with federal text:   {filled}")
print(f"    Fed law had no text:        {no_fed_text}")
print(f"  PDFs on disk:                 {all_fed_pdfs}")

# breakdown by match type
from collections import Counter
mc = Counter(m['match_type'] for m in match_log)
print(f"\n  Match type breakdown:")
for k, v in mc.most_common():
    print(f"    {k}: {v}")

# ── Save outputs ───────────────────────────────────────────────────────────────
log_path = FEDERAL_BASE / 'cross_match_log.jsonl'
with open(log_path, 'w', encoding='utf-8') as f:
    for entry in match_log:
        f.write(json.dumps(entry, ensure_ascii=False) + '\n')
print(f"\n  Match log saved: {log_path} ({len(match_log)} entries)")

# Summary CSV for quick review
csv_path = FEDERAL_BASE / 'cross_match_summary.csv'
with open(csv_path, 'w', encoding='utf-8') as f:
    f.write("pls_file,pls_title,fed_title,match_type,pls_has_content,fed_has_text,fed_pdf_ok\n")
    for m in match_log:
        row = [
            m['pls_file'], m['pls_title'].replace(',','|'),
            m['fed_title'].replace(',','|'), m['match_type'],
            str(m['pls_has_content']), str(m['fed_has_text']), str(m['fed_pdf_ok'])
        ]
        f.write(','.join(row) + '\n')
print(f"  Summary CSV saved: {csv_path}")

print("\nDone.")
