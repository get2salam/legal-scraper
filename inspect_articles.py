import sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from bs4 import BeautifulSoup
from pathlib import Path

listing = Path('data_v2/pls_extras/articles/listing.html').read_text(encoding='utf-8')
soup = BeautifulSoup(listing, 'html.parser')

# Find all caseType rows
rows = soup.find_all('tr', class_='caseType')
print(f'caseType rows in listing: {len(rows)}')
for row in rows[:10]:
    tds = row.find_all('td')
    btn = row.find('input', attrs={'casetypeid': True})
    cid = btn.get('casetypeid','') if btn else ''
    title = tds[1].get_text(strip=True) if len(tds) >= 2 else ''
    print(f'  [{cid}] {title[:70]}')

# Look for year selectors
print('\nSelect elements:')
for sel in soup.find_all('select'):
    sid = sel.get('id','')
    sname = sel.get('name','')
    print(f'  select id={sid} name={sname}')
    for opt in sel.find_all('option'):
        val = opt.get('value','')
        txt = opt.get_text(strip=True)
        print(f'    option value={val} text={txt}')

print('\nForms:')
for form in soup.find_all('form'):
    action = form.get('action','')
    method = form.get('method','')
    print(f'  form action={action} method={method}')

print('\nButtons:')
for btn in soup.find_all(['button', 'input']):
    btype = btn.get('type','')
    if btype in ['submit', 'button'] or btn.name == 'button':
        val = btn.get('value','')
        bid = btn.get('id','')
        txt = btn.get_text(strip=True)[:40]
        print(f'  {btn.name} type={btype} value={val} id={bid} text={txt}')

print('\nLinks:')
for a in soup.find_all('a', href=True):
    href = a['href']
    txt = a.get_text(strip=True)
    if txt:
        print(f'  {txt[:60]} -> {href}')

# Look for scripts with year references
print('\nScript snippets with year:')
for script in soup.find_all('script'):
    txt = script.get_text()
    if 'year' in txt.lower() or 'Year' in txt:
        lines = txt.split('\n')
        for line in lines:
            if 'year' in line.lower():
                print(f'  {line.strip()[:120]}')
