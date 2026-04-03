import re
from bs4 import BeautifulSoup

html = open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/list_page.html','r',encoding='utf-8').read()
soup = BeautifulSoup(html, 'html.parser')

primary = soup.find('div', id='primary-legislation')
sections = primary.find_all('div', class_='accordion-section')

# Look at raw HTML of first 10 content divs for status patterns
for i in [0, 1, 5, 6, 42, 100]:
    if i >= len(sections):
        continue
    sec = sections[i]
    content_div = sec.find('div', class_='accordion-section-content')
    title_div = sec.find('div', class_='accordion-section-title')
    link = title_div.find('a') if title_div else None
    
    print(f"\n=== ENTRY {i+1}: {link.get_text(strip=True) if link else 'N/A'} ===")
    raw = str(content_div)
    # Show just the status-related part
    # Remove the category/date part to focus on status
    print("RAW HTML:")
    print(raw)
    print("---")

# Count statuses properly using visible <font> tags
print("\n\n=== STATUS COUNTS ===")
certified = 0
under_final = 0
under_review = 0
no_status = 0

for sec in sections:
    content_div = sec.find('div', class_='accordion-section-content')
    if content_div:
        # Get visible font tags (not in comments)
        fonts = content_div.find_all('font')
        has_status = False
        for font in fonts:
            text = font.get_text(strip=True)
            if 'Certified Authentic' in text:
                certified += 1
                has_status = True
            elif 'Under Final Review' in text:
                under_final += 1
                has_status = True
            elif 'Under Review' in text:
                under_review += 1
                has_status = True
        if not has_status:
            no_status += 1

print(f"Certified Authentic: {certified}")
print(f"Under Final Review: {under_final}")
print(f"Under Review: {under_review}")
print(f"No visible status: {no_status}")
print(f"Total: {certified + under_final + under_review + no_status}")
