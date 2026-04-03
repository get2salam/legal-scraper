import re
html = open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/law_page.html','r',encoding='utf-8').read()
# Find PDF references
pdfs = re.findall(r'[^\s"\']*pdffiles[^\s"\']*\.pdf[^\s"\']*', html)
for p in pdfs[:10]:
    print("PDF:", p)
print('---')
# Also look for iframe or embed
for tag in ['iframe', 'embed', 'object']:
    idx = html.lower().find(f'<{tag}')
    if idx >= 0:
        print(f'{tag} found at {idx}:', html[idx:idx+500])
        print('---')
# Look for any .pdf reference
all_pdfs = re.findall(r'[^\s"\'<>]+\.pdf', html)
for p in set(all_pdfs):
    print("Any PDF:", p)
