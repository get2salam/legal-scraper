"""Check original HTML files for JSON-encoded format issue."""
import os, glob

bad = 0
good = 0
samples = {}

for reporter in ['SCMR','PLD','CLC','PCrLJ','MLD','PTD','PLC','YLR','CLD','GBLR']:
    for year in range(1947, 2027):
        files = glob.glob(f'data_v2/{reporter}/{year}/original/*.html')
        if not files:
            continue
        # Check first file
        with open(files[0], 'r', encoding='utf-8') as f:
            start = f.read(20)
        
        is_bad = start.startswith('"') or '\\u003c' in start
        key = f'{reporter}/{year}'
        samples[key] = 'BAD' if is_bad else 'OK'
        if is_bad:
            bad += 1
        else:
            good += 1

print("=== SAMPLE RESULTS ===")
for k in sorted(samples):
    if samples[k] == 'BAD':
        print(f'  {k}: {samples[k]}')

print(f'\nTotal: {good} OK, {bad} BAD (out of {good+bad} year/reporter combos)')

# Count total bad files
print('\nCounting all bad files...')
total_bad = 0
total_good = 0
for f in glob.glob('data_v2/*/[0-9]*/original/*.html'):
    with open(f, 'r', encoding='utf-8') as fh:
        c = fh.read(5)
    if c.startswith('"'):
        total_bad += 1
    else:
        total_good += 1

print(f'Total files: {total_good} OK, {total_bad} BAD (out of {total_good+total_bad})')
