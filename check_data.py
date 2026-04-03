import sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from bs4 import BeautifulSoup

# Check topic HTML content
print("=== TOPICS ===")
topic_files = list(Path('data_v2/pls_extras/topics').glob('topic_*.html'))
print(f"Topic HTML files: {len(topic_files)}")

# Sample one
if topic_files:
    html = topic_files[0].read_text(encoding='utf-8', errors='replace')
    print(f"Sample: {topic_files[0].name}, size={len(html)}")
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr')
    print(f"  Rows: {len(rows)}")
    for row in rows[:5]:
        print(f"  {row.get_text(strip=True)[:100]}")

# Check topics_list.json
topics = json.loads(Path('data_v2/pls_extras/topics/topics_list.json').read_text(encoding='utf-8', errors='replace'))
print(f"\ntopics_list.json: {len(topics)} entries")
for t in topics[:5]:
    print(f"  {t}")

# Check W&P listing
print("\n=== W&P listing ===")
html2 = Path('data_v2/pls_extras/words_phrases/listing.html').read_text(encoding='utf-8', errors='replace')
print(f"Listing size: {len(html2)}")

# Check letter files
letter_a = Path('data_v2/pls_extras/words_phrases/letter_A.html').read_text(encoding='utf-8', errors='replace')
print(f"letter_A size: {len(letter_a)}")
print(f"Same as listing? {len(letter_a) == len(html2)}")

soup_a = BeautifulSoup(letter_a, 'html.parser')
right = soup_a.find('div', id='rightmenu')
if right:
    table = right.find('table')
    if table:
        rows_a = table.find_all('tr')
        print(f"letter_A table rows: {len(rows_a)}")
        for row in rows_a[:5]:
            tds = row.find_all('td')
            if tds:
                print(f"  {[td.get_text(strip=True)[:40] for td in tds]}")

# Check W&P listing rightmenu
soup2 = BeautifulSoup(html2, 'html.parser')
right2 = soup2.find('div', id='rightmenu')
if right2:
    table2 = right2.find('table')
    if table2:
        rows2 = table2.find_all('tr')
        print(f"\nW&P listing table rows: {len(rows2)}")
        for row in rows2[:5]:
            print(f"  class={row.get('class')}, topicid={row.get('topicid')}, wordid={row.get('wordid')}")
            tds = row.find_all('td')
            if tds:
                print(f"  tds: {[td.get_text(strip=True)[:40] for td in tds]}")

# Check articles - what's in the 2026 vs 2025 pages
print("\n=== ARTICLES ===")
for year in [2020, 2021, 2022, 2023, 2024, 2025, 2026]:
    p = Path(f'data_v2/pls_extras/articles/year_{year}.html')
    if p.exists():
        h = p.read_text(encoding='utf-8', errors='replace')
        s = BeautifulSoup(h, 'html.parser')
        rows = s.find_all('tr', attrs={'casetypeid': True})
        print(f"  year_{year}: {len(rows)} rows with casetypeid")
        for row in rows[:3]:
            print(f"    [{row.get('casetypeid')}] {row.get_text(strip=True)[:80]}")

listing_html = Path('data_v2/pls_extras/articles/listing.html').read_text(encoding='utf-8', errors='replace')
sl = BeautifulSoup(listing_html, 'html.parser')
all_casetypeids = sl.find_all(attrs={'casetypeid': True})
print(f"\nListing casetypeids: {len(all_casetypeids)}")
for el in all_casetypeids:
    print(f"  tag={el.name}, id={el.get('casetypeid')}")
