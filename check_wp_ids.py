import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from bs4 import BeautifulSoup

# Check W&P rows for onclick or data attributes
html = Path('data_v2/pls_extras/words_phrases/listing.html').read_text(encoding='utf-8', errors='replace')
soup = BeautifulSoup(html, 'html.parser')
right = soup.find('div', id='rightmenu')
if right:
    table = right.find('table')
    if table:
        rows = table.find_all('tr')
        print(f"Total rows: {len(rows)}")
        # Print full HTML of first 3 rows to see all attributes
        for row in rows[:3]:
            print(f"\nRow HTML:\n{str(row)[:500]}")
        
        # Check all tds in row 1
        row1 = rows[1] if len(rows) > 1 else rows[0]
        print(f"\nRow 1 attrs: {dict(row1.attrs)}")
        for td in row1.find_all('td'):
            print(f"  TD attrs: {dict(td.attrs)}, HTML: {str(td)[:200]}")

# Check the JS click handler for searchCase rows
for sc in soup.find_all('script'):
    content = sc.get_text()
    if 'searchCase' in content:
        print(f"\nScript with 'searchCase':\n{content[:2000]}")
        break
