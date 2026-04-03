import re
from bs4 import BeautifulSoup

html = open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/list_page.html','r',encoding='utf-8').read()
soup = BeautifulSoup(html, 'html.parser')

# Check the secondary-legislation tab raw HTML
secondary = soup.find('div', id='secondary-legislation')
if secondary:
    raw = str(secondary)[:2000]
    print("SECONDARY TAB HTML (first 2000 chars):")
    print(raw)
else:
    print("No secondary tab found")

print("\n\n=== STATUS EXAMPLES ===")
primary = soup.find('div', id='primary-legislation')
sections = primary.find_all('div', class_='accordion-section')

# Find entries with different statuses
statuses_found = {}
for i, sec in enumerate(sections):
    content_div = sec.find('div', class_='accordion-section-content')
    if content_div:
        html_c = str(content_div)
        if 'Under Final Review' in html_c and 'under_final_review' not in statuses_found:
            statuses_found['under_final_review'] = i
        elif 'Under Review' in html_c and 'under_review' not in statuses_found:
            statuses_found['under_review'] = i
        elif 'Certified Authentic' in html_c and 'certified' not in statuses_found:
            statuses_found['certified'] = i

for status, idx in statuses_found.items():
    sec = sections[idx]
    title_div = sec.find('div', class_='accordion-section-title')
    content_div = sec.find('div', class_='accordion-section-content')
    link = title_div.find('a')
    print(f"\n--- {status} (entry #{idx+1}) ---")
    print(f"Title: {link.get_text(strip=True) if link else 'N/A'}")
    print(f"Content HTML: {str(content_div)[:500]}")

# Also check if Ordinances appear in the primary tab
ordinance_count = 0
act_count = 0
for sec in sections:
    title_div = sec.find('div', class_='accordion-section-title')
    link = title_div.find('a') if title_div else None
    if link:
        name = link.get_text(strip=True)
        if 'Ordinance' in name:
            ordinance_count += 1
        elif 'Act' in name or 'Order' in name or 'Rules' in name:
            act_count += 1

print(f"\n\nOrdinances in primary tab: {ordinance_count}")
print(f"Acts/Orders/Rules in primary tab: {act_count}")
print(f"Total: {len(sections)}")

# Check last few entries
print("\n=== LAST 3 ENTRIES ===")
for sec in sections[-3:]:
    title_div = sec.find('div', class_='accordion-section-title')
    content_div = sec.find('div', class_='accordion-section-content')
    link = title_div.find('a') if title_div else None
    print(f"Title: {link.get_text(strip=True) if link else 'N/A'}")
    if content_div:
        print(f"Content: {content_div.get_text(strip=True)[:200]}")
    print()
