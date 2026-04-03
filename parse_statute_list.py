#!/usr/bin/env python3
"""Parse the statute list response to understand the structure."""

from bs4 import BeautifulSoup
import json

with open('statuechar_search_response.html', 'r', encoding='utf-8') as f:
    html = f.read()
soup = BeautifulSoup(html, 'html.parser')

# Find the statute table
tables = soup.find_all('table')
print(f'Found {len(tables)} tables')

# Check table classes and IDs
for i, table in enumerate(tables):
    print(f"Table {i}: class={table.get('class')}, id={table.get('id')}")

statute_table = tables[2] if len(tables) > 2 else None

if statute_table:
    rows = statute_table.find_all('tr')
    print(f'\nFound {len(rows)} rows in statute table')
    
    # Parse first 20 rows
    statutes = []
    for row in rows[:20]:
        cells = row.find_all('td')
        if len(cells) >= 2:
            num = cells[0].get_text(strip=True)
            name = cells[1].get_text(strip=True)
            # Check for onclick or data attributes
            onclick = row.get('onclick', '')
            data_attrs = {k: v for k, v in row.attrs.items() if k.startswith('data-')}
            
            statutes.append({
                'num': num,
                'name': name,
                'onclick': onclick[:200] if onclick else '',
                'attrs': data_attrs
            })
    
    for s in statutes:
        print(f"\n{s['num']:3} | {s['name'][:70]}")
        if s['onclick']:
            print(f"    onclick: {s['onclick']}")
        if s['attrs']:
            print(f"    attrs: {s['attrs']}")

# Also look for JavaScript functions that handle statute clicks
print("\n\n=== Looking for statute-related JS functions ===")
scripts = soup.find_all('script')
for script in scripts:
    text = script.get_text()
    if 'statue' in text.lower() or 'statute' in text.lower():
        # Find function definitions
        import re
        funcs = re.findall(r'function\s+\w*[Ss]tatu[te]e?\w*\s*\([^)]*\)\s*\{[^}]{0,500}', text)
        for f in funcs:
            print(f"\n{f[:300]}")
