import os, json
total = sum(1 for l in os.listdir('data_v2/legislation') if os.path.isdir(os.path.join('data_v2/legislation',l)) and len(l)==1 for f in os.listdir(os.path.join('data_v2/legislation',l)) if f.endswith('.json'))
print(f'Legislation: {total} / 10,915 ({total/10915*100:.1f}%)')
print(f'+{total - 1568} today')
with open('data_v2/legislation/progress.json', encoding='utf-8') as f:
    p = json.load(f)
print(f'Done: {p.get("completed_alphabets", [])}')
print(f'Current: {p.get("current_alphabet")}')
