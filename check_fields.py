import json, os

for rep in ['SCMR', 'PLD', 'CLC', 'MLD', 'PCrLJ', 'YLR', 'PTD', 'PLC', 'CLD', 'GBLR']:
    rpath = f'data_v2/{rep}'
    if not os.path.isdir(rpath):
        continue
    years = sorted(os.listdir(rpath))
    for y in [years[0], years[-1]]:
        ypath = f'{rpath}/{y}'
        files = [f for f in os.listdir(ypath) if f.endswith('.json')]
        if files:
            try:
                d = json.load(open(f'{ypath}/{files[0]}', 'r', encoding='utf-8'))
                has_clean = bool(d.get('judgment_clean', ''))
                has_judgment = bool(d.get('judgment', ''))
                has_raw = bool(d.get('judgment_raw', ''))
                has_html = bool(d.get('judgment_html', ''))
                judg_keys = [k for k in d.keys() if 'judg' in k.lower()]
                print(f"{rep}/{y}/{files[0]}: clean={has_clean} judgment={has_judgment} raw={has_raw} html={has_html} keys={judg_keys}")
            except Exception as e:
                print(f"{rep}/{y}/{files[0]}: ERROR {e}")
