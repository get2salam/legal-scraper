import json, sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

print('=== Data Quality Check ===')

# articles_lite
with open('data_v2/pls_extras/articles/articles_lite.json', encoding='utf-8') as f:
    a = json.load(f)
has_title = all(x.get('title') for x in a)
years = [x['year'] for x in a if x.get('year')]
print(f'Articles: {len(a)} entries, all have title: {has_title}')
print(f'  Years range: {min(years)} - {max(years)}')

# dictionary
with open('data_v2/pls_extras/dictionary/definitions_fixed.json', encoding='utf-8') as f:
    d = json.load(f)
has_term = all(x.get('term') for x in d)
has_def = all(x.get('definition') for x in d)
print(f'Dictionary: {len(d)} entries, all have term: {has_term}, all have def: {has_def}')

# topics
with open('data_v2/pls_extras/topics/topics_list.json', encoding='utf-8') as f:
    t = json.load(f)
has_title2 = all(x.get('title') for x in t)
print(f'Topics: {len(t)} entries, all have title: {has_title2}')

# words_phrases_all
with open('data_v2/pls_extras/words_phrases/words_phrases_all.json', encoding='utf-8') as f:
    wp = json.load(f)
has_name = all(x.get('name') for x in wp)
has_cid = all(x.get('casetypeid') for x in wp)
print(f'Words & Phrases: {len(wp)} entries, all have name: {has_name}, all have casetypeid: {has_cid}')

# legal_terms
with open('data_v2/pls_extras/legal_terms/legal_terms_all.json', encoding='utf-8') as f:
    lt = json.load(f)
has_name2 = all(x.get('name') for x in lt)
has_cid2 = all(x.get('casetypeid') for x in lt)
print(f'Legal Terms: {len(lt)} entries, all have name: {has_name2}, all have casetypeid: {has_cid2}')

print()
print('=== Sample entries per file ===')
print('Articles[0]:', json.dumps({k:v for k,v in a[0].items() if k != 'excerpt'}, ensure_ascii=False)[:200])
print('Dict[0]:', json.dumps(d[0], ensure_ascii=False)[:200])
print('Topics[0]:', json.dumps(t[0], ensure_ascii=False)[:200])
print('W&P[0]:', json.dumps(wp[0], ensure_ascii=False)[:200])
print('Terms[0]:', json.dumps(lt[0], ensure_ascii=False)[:200])
