"""
Create a lightweight articles index without the heavy HTML body content.
The body field in articles_full.json contains full HTML — strip it for the browse page.
"""
import json, sys, re
from pathlib import Path
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('data_v2/pls_extras/articles/articles_full.json', encoding='utf-8') as f:
    articles = json.load(f)

print(f"Total articles: {len(articles)}")

lite = []
for article in articles:
    # Strip the body HTML, extract just a text excerpt
    body = article.get('body', '')
    excerpt = ''
    if body:
        try:
            soup = BeautifulSoup(body, 'html.parser')
            text = soup.get_text(separator=' ', strip=True)
            # Get first 300 chars as excerpt
            excerpt = re.sub(r'\s+', ' ', text)[:300].strip()
        except:
            excerpt = body[:200]
    
    lite.append({
        'id': article.get('id', ''),
        'title': article.get('title', '').strip(),
        'author': article.get('author', '').strip(),
        'category': article.get('category', ''),
        'year': article.get('year', ''),
        'excerpt': excerpt
    })

# Sort by year descending
lite.sort(key=lambda x: x.get('year', '0'), reverse=True)

out = Path('data_v2/pls_extras/articles/articles_lite.json')
with open(out, 'w', encoding='utf-8') as f:
    json.dump(lite, f, ensure_ascii=False, indent=2)

size = out.stat().st_size
print(f"Saved articles_lite.json: {size/1024:.1f} KB")
print("Sample:")
for a in lite[:3]:
    print(f"  [{a['year']}] {a['title'][:60]} — {a['author'][:40]}")
    print(f"    excerpt: {a['excerpt'][:100]}")
