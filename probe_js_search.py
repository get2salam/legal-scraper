import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('probe_mainpage.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()

# Find ALL JS files referenced
js_files = re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.I)
print('External JS files:')
for j in js_files:
    print(f'  {j}')

# Find caseName attribute usage
casenames = re.findall(r'caseName=["\']([^"\']+)["\']', html)
print(f'\ncaseName values (first 20):')
for c in casenames[:20]:
    print(f'  {c}')

# The caseName seems to be like "2026P204" - find more context
for li_tag in re.finditer(r'<li\s+caseName=["\']([^"\']+)["\'][^>]*>(.*?)</li>', html, re.DOTALL | re.I):
    print(f'\nLI caseName={li_tag.group(1)}: {li_tag.group(2)[:100]}')

# Find the search button JavaScript (caseLaw button handler)
# Look for all large JS chunks
all_scripts = []
for script in re.finditer(r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.I):
    content = script.group(1).strip()
    if len(content) > 200:
        all_scripts.append(content)

for i, script in enumerate(all_scripts):
    if 'caseLaw' in script.lower() or 'keyword' in script.lower() or 'CitationSearch' in script:
        print(f'\n=== Script with caseLaw/keyword ({len(script)} chars) ===')
        print(script[:3000])
        print('...(truncated)')

# Find the search tabs
tabs = re.findall(r'searchButton[^>]*>[^<]*(?:Citation|Index|Keyword|Full|CaseLaw)[^<]*</button>|class=["\']searchButton["\'][^>]*>([^<]+)', html, re.I)
print(f'\nSearch buttons: {tabs[:10]}')

# Find what bookSearch sends to
book_search_ctx = re.search(r'bookSearch.*?(?:ajax|url)[^\n]{0,300}', html, re.DOTALL | re.I)
if book_search_ctx:
    print(f'\nbookSearch context: {book_search_ctx.group(0)[:300]}')

# Look for the word search button
word_search_btn = re.findall(r'Advance_Search.*?(?:ajax|url|endpoint|action)[^\n]{0,200}', html, re.DOTALL | re.I)
for w in word_search_btn[:3]:
    print(f'\nAdvance search: {w[:200]}')

# Find ALL large HTML sections with "search" in them
search_sections = re.finditer(r'(?:Citation_Search_div|Advance_Search_div|keyword_search|Index_search|full_text)[^\n]{0,500}', html, re.I)
for s in search_sections:
    print(f'\nSearch section: {s.group(0)[:300]}')
