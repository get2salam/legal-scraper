import json, sys, urllib.request, time, re
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

def get_text(html):
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# The Punjab Laws site has numbered pages (e.g. 328.html)
# We know 328 = Punjab Public Service Commission Ordinance 1978
# We need to find which numbers map to which acts

# First, get the full CDX catalog without collapsing
print('=== Getting ALL Punjab Laws snapshots ===')
api = 'http://web.archive.org/cdx/search/cdx?url=www.punjablaws.gov.pk/*&output=json&limit=200&fl=timestamp,original,statuscode&filter=statuscode:200&collapse=urlkey'
try:
    resp = urllib.request.urlopen(api, timeout=30)
    data = json.loads(resp.read())
    print(f'Pages found: {len(data)-1}')
    for row in data[1:30]:
        print(f'  {row[0]}: {row[1][:100]}')
    # Save 
    pages = [(row[0], row[1]) for row in data[1:] if '.html' in row[1] and not any(x in row[1] for x in ['css', 'js', 'jpg', 'png', 'gif'])]
    print(f'\nHTML act pages: {len(pages)}')
    json.dump(pages, open('punjab_pages.json', 'w'), indent=2)
except Exception as e:
    print(f'Error: {e}')
