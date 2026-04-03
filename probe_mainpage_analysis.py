import sys, re, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('probe_mainpage.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()

print(f'Page size: {len(html)} chars')

# All Login endpoints
urls = set(re.findall(r"[\"'](/Login/[A-Za-z][A-Za-z0-9]+)", html))
print('\nAll Login endpoints:')
for u in sorted(urls):
    print(f'  {u}')

# AJAX/fetch calls
ajax_urls = re.findall(r"""(?:url|action)\s*[:=]\s*['"]([^'"]+)['"]""", html)
print('\nAJAX URLs:')
for u in sorted(set(ajax_urls))[:20]:
    print(f'  {u}')

# Search-related vars
search_js = re.findall(r'(?:SearchText|GetCaseFile|GetStatues)[^\n]{0,150}', html)
print('\nSearch/case JS patterns:')
for s in search_js[:10]:
    print(f'  {s.strip()[:120]}')

# Look for GetCaseFile and GetStatuesSearch
gcf = re.findall(r'GetCaseFile[^\n]{0,200}', html)
print('\nGetCaseFile patterns:')
for g in gcf[:5]:
    print(f'  {g.strip()[:150]}')

gss = re.findall(r'GetStatuesSearch[^\n]{0,200}', html)
print('\nGetStatuesSearch patterns:')
for g in gss[:5]:
    print(f'  {g.strip()[:150]}')

# Look for citation search form
citation_form = re.findall(r'CitationSearch[^\n]{0,200}', html)
print('\nCitationSearch patterns:')
for c in citation_form[:5]:
    print(f'  {c.strip()[:150]}')

# Look for keyword/full-text search
keyword_search = re.findall(r'[Kk]ey[Ww]ord[^\n]{0,200}', html)
print('\nKeyword patterns:')
for k in keyword_search[:5]:
    print(f'  {k.strip()[:150]}')

# Count mentions of various numbers that could be total cases
all_text = re.sub(r'<[^>]+>', ' ', html)
large_numbers = re.findall(r'\b(\d{5,})\b', all_text)
print('\nLarge numbers in page text:')
from collections import Counter
cnt = Counter(large_numbers)
for num, count in cnt.most_common(20):
    print(f'  {num} (appears {count}x)')

# Find the full-text or word search endpoint
ft_patterns = re.findall(r'(?:FullText|WordSearch|KeySearch|JudgmentSearch|CaseLaw)[^\n]{0,200}', html)
print('\nFull-text search patterns:')
for f in ft_patterns[:5]:
    print(f'  {f.strip()[:150]}')

# Find the search forms by looking at script tags
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.I)
print(f'\nScript tags: {len(scripts)}')
for i, script in enumerate(scripts):
    if any(kw in script for kw in ['CitationSearch', 'Search', 'ajax', 'post']):
        print(f'\nScript {i} ({len(script)} chars):')
        print(script[:500])
        print('...')
