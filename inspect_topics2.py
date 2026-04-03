import sys
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from bs4 import BeautifulSoup

with open('data_v2/pls_extras/topics/listing.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()
soup = BeautifulSoup(html, 'html.parser')
right_div = soup.find('div', id='rightmenu')

# Check the full table HTML
if right_div:
    table = right_div.find('table')
    if table:
        print("Table HTML (first 3000 chars):")
        print(str(table)[:3000])

# Check JS for topic click behavior
print("\n\nJS scripts in topics page:")
for sc in soup.find_all('script'):
    content = sc.get_text()
    if 'topic' in content.lower() or 'Topic' in content:
        print(content[:3000])
        break
