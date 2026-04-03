import sys, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('probe_mainpage.html', 'r', encoding='utf-8', errors='replace') as f:
    html = f.read()

# Get all script tags content
scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL | re.I)
print(f'Total script tags: {len(scripts)}')

for i, script in enumerate(scripts):
    if len(script) > 100:
        print(f'\n=== Script {i} ({len(script)} chars) ===')
        print(script[:2000])

# Also find the keyword search input
kw_input = re.findall(r'Keyword_Search[^\n]{0,300}', html)
print('\nKeyword search input context:')
for kw in kw_input:
    print(f'  {kw[:200]}')

# Find the container around keyword search
kw_idx = html.find('Keyword_Search')
if kw_idx > 0:
    print(f'\nContext around Keyword_Search (500 chars):')
    print(html[max(0, kw_idx-200):kw_idx+300])

# Find GetCaseFile usage
gcf_idx = html.find('GetCaseFile')
if gcf_idx > 0:
    print(f'\nContext around GetCaseFile (500 chars):')
    print(html[max(0, gcf_idx-100):gcf_idx+400])

# Find all anchor href with login paths
anchors = re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>([^<]+)</a>', html)
print('\nAll anchor links:')
for href, text in anchors[:30]:
    if href.startswith('/') or href.startswith('http'):
        print(f'  {href} -> {text.strip()[:50]}')
