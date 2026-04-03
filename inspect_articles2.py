import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from bs4 import BeautifulSoup
from pathlib import Path

listing = Path('data_v2/pls_extras/articles/listing.html').read_text(encoding='utf-8')
soup = BeautifulSoup(listing, 'html.parser')

# Look for all JS - find article-related endpoints
print('=== ALL SCRIPT CONTENT RELATED TO ARTICLES ===')
for script in soup.find_all('script'):
    txt = script.get_text()
    if 'Article' in txt or 'article' in txt or 'caseLawYear' in txt or 'GetArticle' in txt:
        print(txt[:3000])
        print('---')

print('\n=== TABLE STRUCTURE ===')
for table in soup.find_all('table'):
    tid = table.get('id','')
    tcls = table.get('class','')
    print(f'table id={tid} class={tcls}')
    for th in table.find_all('th'):
        print(f'  th: {th.get_text(strip=True)[:60]}')

print('\n=== DIVs with article in id/class ===')
for div in soup.find_all('div'):
    did = div.get('id','').lower()
    dcls = ' '.join(div.get('class',[])).lower()
    if 'article' in did or 'article' in dcls:
        print(f'  div id={div.get("id","")} class={div.get("class","")}')
