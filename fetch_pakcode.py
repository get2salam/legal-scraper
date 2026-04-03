import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Try to fetch the actual archived pakistancode.gov.pk/english/ page
# to see what legislation links it has

print("Fetching pakistancode.gov.pk/english/ from 2019 archive...")
try:
    wayback_url = 'http://web.archive.org/web/20191123022612/http://pakistancode.gov.pk:80/english/'
    req = urllib.request.Request(wayback_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    resp = urllib.request.urlopen(req, timeout=25)
    html = resp.read().decode('utf-8', errors='replace')
    print(f'HTML size: {len(html)} chars')
    
    # Extract text
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    print(f'Text size: {len(text)} chars')
    print(f'Preview: {text[:800]}')
    
    # Find links
    links = re.findall(r'href=["\']([^"\']+)["\']', html)
    print(f'\nLinks ({len(links)}):')
    for link in links[:40]:
        if 'archive.org' not in link and 'static' not in link.lower():
            print(f'  {link}')
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()

time.sleep(3)

# Also try a different snapshot
print()
print("Fetching pakistancode from 2019 (different date)...")
try:
    wayback_url = 'http://web.archive.org/web/20190622095059/http://pakistancode.gov.pk:80/english/'
    req = urllib.request.Request(wayback_url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urllib.request.urlopen(req, timeout=25)
    html = resp.read().decode('utf-8', errors='replace')
    
    # Look for links that point to actual legislation items
    # Pattern: /english/UY... or /UY... or similar patterns
    all_links = re.findall(r'href=["\']([^"\']+)["\']', html)
    print(f'All links: {len(all_links)}')
    
    # Filter for potentially interesting ones
    for link in all_links:
        if any(x in link for x in ['UY', 'act', 'ordinance', 'rule', 'regulation', 'statute', 'law']):
            print(f'  INTERESTING: {link}')
    
    # Also look for any JavaScript-loaded content
    js_links = re.findall(r"'(/[^']+)'|\"(/[^\"]+)\"", html)
    for l1, l2 in js_links[:20]:
        link = l1 or l2
        if link and len(link) > 5 and 'static' not in link.lower():
            print(f'  JS: {link}')

except Exception as e:
    print(f'Error: {e}')
