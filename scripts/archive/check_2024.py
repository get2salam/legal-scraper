import json

with open('data_v2/audit/2026-02-06_verification.json') as f:
    data = json.load(f)

print('=== 2024 VERIFICATION (08:53 AM) ===')
print()

total_pls = 0
total_local = 0
total_missing = 0

for r in data['results']:
    if r['year'] == 2024:
        missing = len(r['missing_cases'])
        status = 'OK' if missing == 0 else f'{missing} MISSING'
        print(f"{r['reporter']} 2024: PLS={r['pls_count']} Local={r['local_count']} {status}")
        total_pls += r['pls_count']
        total_local += r['local_count']
        total_missing += missing

print()
print('=== TOTAL 2024 ===')
print(f'PLS has:    {total_pls}')
print(f'We have:    {total_local}')
print(f'Missing:    {total_missing}')
