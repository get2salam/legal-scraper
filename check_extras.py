import json, sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
base = Path('data_v2/pls_extras')

# Dictionary
d = json.load(open(base/'dictionary'/'definitions.json', encoding='utf-8'))
print(f'Dictionary: {len(d)} definitions')
for entry in d[:3]:
    print(f'  {entry["term"]}: {entry["definition"][:80]}')

# Topics
t = json.load(open(base/'topics'/'topics_list.json', encoding='utf-8'))
print(f'\nTopics: {len(t)} topics listed')
for topic in t[:5]:
    print(f'  {topic}')

# Topic detail pages downloaded
topic_htmls = list((base/'topics').glob('topic_*.html'))
print(f'Topic detail pages: {len(topic_htmls)}')

# Articles
a = json.load(open(base/'articles'/'all_years_meta.json', encoding='utf-8'))
print(f'\nArticles meta: {len(a)} entries')
titles_seen = set()
for art in a:
    titles_seen.add(art.get('title',''))
print(f'Unique article titles: {len(titles_seen)}')
for t2 in list(titles_seen)[:5]:
    print(f'  {t2[:70]}')

# Article detail pages
art_htmls = list((base/'articles').glob('article_*.html'))
print(f'Article HTML pages downloaded: {len(art_htmls)}')

# Words & Phrases
wp_files = list((base/'words_phrases').glob('letter_*.html'))
print(f'\nWords & Phrases: {len(wp_files)} letter pages (A-Z)')

# Summary
print('\n=== OVERALL ===')
total_size = sum(f.stat().st_size for f in base.rglob('*') if f.is_file())
print(f'Total size: {round(total_size/1024/1024, 1)} MB')
