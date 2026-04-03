import json, sys, time, urllib.request, urllib.parse, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Try fetching a molaw.gov.pk archive page to see if it has legislation content
print("Fetching archived molaw.gov.pk from 2012...")
try:
    wayback_url = 'http://web.archive.org/web/20120918194034/http://www.molaw.gov.pk/'
    req = urllib.request.Request(wayback_url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urllib.request.urlopen(req, timeout=20)
    html = resp.read().decode('utf-8', errors='replace')
    # Strip HTML
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    print(f'Text length: {len(text)}')
    print(f'Preview: {text[:500]}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

# Try 2007 snapshot
print()
print("Fetching archived molaw.gov.pk from 2007...")
try:
    wayback_url = 'http://web.archive.org/web/20070907145436/http://www.molaw.gov.pk:80/'
    req = urllib.request.Request(wayback_url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urllib.request.urlopen(req, timeout=20)
    html = resp.read().decode('utf-8', errors='replace')
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    print(f'Text length: {len(text)}')
    print(f'Preview: {text[:500]}')
    # Check for links to legislation
    links = re.findall(r'href=["\']([^"\']+)["\']', html)
    print(f'\nLinks found: {len(links)}')
    for link in links[:20]:
        print(f'  {link}')
except Exception as e:
    print(f'Error: {e}')

time.sleep(2)

# Try pakistancode.gov.pk archived
print()
print("Fetching archived pakistancode.gov.pk from 2015...")
try:
    wayback_url = 'http://web.archive.org/web/20150605194354/http://pakistancode.gov.pk/'
    req = urllib.request.Request(wayback_url, headers={'User-Agent': 'Mozilla/5.0'})
    resp = urllib.request.urlopen(req, timeout=20)
    html = resp.read().decode('utf-8', errors='replace')
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    print(f'Text length: {len(text)}')
    print(f'Preview: {text[:500]}')
    # Check for links
    links = re.findall(r'href=["\']([^"\']+)["\']', html)
    print(f'\nLinks found: {len(links)}')
    for link in links[:30]:
        print(f'  {link}')
except Exception as e:
    print(f'Error: {e}')
