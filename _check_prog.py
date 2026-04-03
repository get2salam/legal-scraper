import json
with open('data_v2/legislation/progress.json', encoding='utf-8') as f:
    p = json.load(f)
print('Completed:', p.get('completed_alphabets', []))
print('Current:', p.get('current_alphabet'))
print('Scraped count:', len(p.get('statutes_scraped', [])))
print('Total:', p.get('total_statutes'))
print('Updated:', p.get('last_updated', 'Never'))
