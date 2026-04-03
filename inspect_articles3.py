import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from bs4 import BeautifulSoup
from pathlib import Path

listing = Path('data_v2/pls_extras/articles/listing.html').read_text(encoding='utf-8')
soup = BeautifulSoup(listing, 'html.parser')

# Print ALL script content
print('=== ALL SCRIPTS ===')
for i, script in enumerate(soup.find_all('script')):
    src = script.get('src','')
    txt = script.get_text().strip()
    if txt and len(txt) > 50:
        print(f'\n--- Script {i} (len={len(txt)}) ---')
        print(txt[:5000])

# Print the articlePageSection content
print('\n=== articlePageSection content ===')
section = soup.find('div', id='articlePageSection')
if section:
    print(section.prettify()[:3000])
else:
    print('NOT FOUND')

# Print table content
print('\n=== articleSearchTable content ===')
table = soup.find('table', id='articleSearchTable')
if table:
    rows = table.find_all('tr')
    print(f'Rows: {len(rows)}')
    for row in rows[:20]:
        print(row.prettify()[:500])
        print('---')
