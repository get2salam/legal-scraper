import json

with open('data_v2/audit/court_audit_2026-02-24_1400.json') as f:
    prev = json.load(f)
with open('data_v2/audit/court_audit_2026-02-25_1401.json') as f:
    curr = json.load(f)

print('SUMMARY COMPARISON (2026-02-24 vs 2026-02-25):')
print('  Total cases:    {} -> {}'.format(prev.get('total_cases','?'), curr.get('total_cases','?')))
print('  Total errors:   {} -> {}'.format(prev.get('total_errors','?'), curr.get('total_errors','?')))
print('  Total warnings: {} -> {}'.format(prev.get('total_warnings','?'), curr.get('total_warnings','?')))

prev_courts = {r['court']: r for r in prev.get('results', [])}
curr_courts = {r['court']: r for r in curr.get('results', [])}

print()
print('PER-COURT COMPARISON (stats):')
for court in curr_courts:
    p = prev_courts.get(court, {})
    c = curr_courts[court]
    ps = p.get('stats', {})
    cs = c.get('stats', {})
    pj = ps.get('total_json', 0)
    cj = cs.get('total_json', 0)
    pp_f = ps.get('total_pdf', 0)
    cp_f = cs.get('total_pdf', 0)
    pd_hash = ps.get('duplicates_by_hash', 0)
    cd_hash = cs.get('duplicates_by_hash', 0)
    pd_num = ps.get('duplicates_by_number', 0)
    cd_num = cs.get('duplicates_by_number', 0)
    diff_j = cj - pj
    diff_p = cp_f - pp_f
    diff_dh = cd_hash - pd_hash
    diff_dn = cd_num - pd_num
    flag = ''
    if diff_j < 0:
        flag += ' *** DATA LOSS ***'
    if diff_dh > 50:
        flag += ' *** DUP SPIKE (hash) ***'
    if diff_dn > 50:
        flag += ' *** DUP SPIKE (num) ***'
    print('  {}: JSON {}->{}({:+d}), PDF {}->{}({:+d}), DupHash {}->{}({:+d}), DupNum {}->{}({:+d}){}'.format(
        court, pj, cj, diff_j, pp_f, cp_f, diff_p, pd_hash, cd_hash, diff_dh, pd_num, cd_num, diff_dn, flag))
