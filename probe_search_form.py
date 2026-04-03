import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('probe_mainpage.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()

# Get full script 15 (has the search logic)
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.I)
print(f'Script 15 (FULL - {len(scripts[15])} chars):')
print(scripts[15])

print('\n\n=== Script 16 (FULL) ===')
print(scripts[16])

# Find the search div HTML
search_div = re.search(r'id=["\']searchDiv["\'][^>]*>(.*?)(?=<div class="row">)', html, re.DOTALL | re.I)
if search_div:
    print('\n\n=== Search Div ===')
    print(search_div.group(0)[:3000])

# Find Advanced search inputs
adv_search = re.search(r'readMoreAdvanceSearch[^>]*>(.*?)(?=</div>\s*</div>)', html, re.DOTALL | re.I)
if adv_search:
    print('\n\n=== Advanced Search ===')
    print(adv_search.group(0)[:3000])

# Find ALL input elements with their context
inputs = re.finditer(r'<input[^>]+>', html)
for inp in inputs:
    tag = inp.group()
    name = re.search(r'name=["\']([^"\']+)["\']', tag)
    iid = re.search(r'id=["\']([^"\']+)["\']', tag)
    if name or iid:
        n = name.group(1) if name else ''
        i = iid.group(1) if iid else ''
        if 'search' in (n + i).lower() or 'keyword' in (n + i).lower() or 'year' in (n + i).lower() or 'book' in (n + i).lower():
            # Get context
            start = max(0, inp.start() - 100)
            end = min(len(html), inp.end() + 100)
            print(f'\nSearch input (id={i}, name={n}):')
            print(f'  Context: {html[start:end][:200]}')

# Find the CaseLaw link JS handler
caselaw_handler = re.search(r'latestCaseLaw.*?(?=\n\s*\})', html, re.DOTALL | re.I)
if caselaw_handler:
    print('\n\n=== CaseLaw click handler ===')
    print(caselaw_handler.group(0)[:500])

# Look for keyword search AJAX call
keyword_ajax = re.findall(r'Keyword[^\n]*ajax[^\n]*|ajax[^\n]*Keyword[^\n]*', html, re.I)
for k in keyword_ajax[:5]:
    print(f'\nKeyword AJAX: {k[:200]}')

# Find all POST/GET ajax calls
all_ajax = re.findall(r"""type:\s*["'](GET|POST)["'].*?url:\s*["']([^"']+)["'].*?data:\s*\{([^}]+)\}""", html, re.DOTALL | re.I)
print('\n\nAll AJAX calls (type/url/data):')
for ajax_type, url, data in all_ajax:
    print(f'  {ajax_type} {url} data={{{data.strip()[:100]}}}')
