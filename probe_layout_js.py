import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('layoutScript.js', 'r', encoding='utf-8', errors='replace') as f:
    js = f.read()

print(f'layoutScript.js: {len(js)} chars')

# Find all AJAX/fetch/URL patterns
ajax_urls = re.findall(r"""['"](/Login/[^'"]+)['"]""", js)
print(f'\nAll /Login/ URLs:')
for u in sorted(set(ajax_urls)):
    print(f'  {u}')

# Find all function definitions
funcs = re.findall(r'function\s+(\w+)\s*\(', js)
print(f'\nFunctions ({len(funcs)}): {funcs}')

# Find search-related patterns
search_funcs = [f for f in funcs if 'search' in f.lower() or 'case' in f.lower() or 'cit' in f.lower()]
print(f'\nSearch-related functions: {search_funcs}')

# Find the CitationSearch usage
cit_search = re.findall(r'CitationSearch[^\n]{0,200}', js)
for c in cit_search:
    print(f'\nCitationSearch: {c[:200]}')

# Find keyword search
kw = re.findall(r'Keyword[^\n]{0,200}|keyword[^\n]{0,200}', js)
for k in kw[:10]:
    print(f'\nKeyword: {k[:200]}')

# Find the button click handlers for search
btn_handlers = re.findall(r"\.(?:click|on)\s*\([^)]*function[^{]*{[^}]{0,500}", js, re.DOTALL)
for b in btn_handlers[:5]:
    print(f'\nClick handler: {b[:300]}')

# Find AJAX calls with data
all_ajax = re.finditer(r'(ajax\s*\(\s*\{[^}]{0,1000}\})', js, re.DOTALL)
for ajax in all_ajax:
    print(f'\nAJAX call: {ajax.group(0)[:300]}')

# Find "type" : "POST" patterns to get full AJAX blocks
post_blocks = re.finditer(r'(?:type|method)\s*:\s*["\'](?:POST|GET)["\'][^\}]{0,500}', js, re.DOTALL)
for block in post_blocks:
    print(f'\nPOST/GET block: {block.group(0)[:300]}')

# Find the section with "caseLaw" button search
caselaw_btn = re.search(r'caseLaw[^;]{0,2000}CitationSearch|CitationSearch[^;]{0,2000}caseLaw', js, re.DOTALL | re.I)
if caselaw_btn:
    print(f'\ncaseLaw+CitationSearch: {caselaw_btn.group(0)[:500]}')

# Print sections containing "url:"
url_sections = re.finditer(r'url\s*:\s*["\'][^"\']+["\']', js)
for sec in url_sections:
    start = max(0, sec.start() - 100)
    end = min(len(js), sec.end() + 200)
    print(f'\nURL section: {js[start:end]}')
