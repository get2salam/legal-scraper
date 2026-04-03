import re
from bs4 import BeautifulSoup

html = open('C:/Users/gempo/.openclaw/workspace/projects/pakistan-legislation-scraper/list_page.html','r',encoding='utf-8').read()
soup = BeautifulSoup(html, 'html.parser')

# Check tabs
tabs = soup.find_all('div', class_='tab-pane')
for tab in tabs:
    tab_id = tab.get('id', 'unknown')
    sections = tab.find_all('div', class_='accordion-section')
    print(f"Tab: {tab_id}, {len(sections)} accordion sections")

print("---")

# Parse first 3 entries in detail
primary_tab = soup.find('div', id='primary-legislation')
if primary_tab:
    sections = primary_tab.find_all('div', class_='accordion-section')
    for i, sec in enumerate(sections[:5]):
        title_div = sec.find('div', class_='accordion-section-title')
        content_div = sec.find('div', class_='accordion-section-content')
        
        print(f"\n=== Entry {i+1} ===")
        if title_div:
            link = title_div.find('a')
            if link:
                print(f"Title: {link.get_text(strip=True)}")
                print(f"URL: {link.get('href')}")
            # Get full text for number
            full_text = title_div.get_text(strip=True)
            print(f"Title div text: {full_text[:100]}")
        
        if content_div:
            text = content_div.get_text(strip=True)
            print(f"Content: {text[:200]}")
            # Check for Certified Authentic or Under Review
            html_content = str(content_div)
            if 'Certified Authentic' in html_content:
                print("Status: Certified Authentic")
            elif 'Under Final Review' in html_content:
                print("Status: Under Final Review")
            elif 'Under Review' in html_content:
                print("Status: Under Review")

print("\n\n=== SUBORDINATE LEGISLATION ===")
secondary_tab = soup.find('div', id='secondary-legislation')
if secondary_tab:
    sections = secondary_tab.find_all('div', class_='accordion-section')
    print(f"Total subordinate sections: {len(sections)}")
    for i, sec in enumerate(sections[:3]):
        title_div = sec.find('div', class_='accordion-section-title')
        content_div = sec.find('div', class_='accordion-section-content')
        
        print(f"\n=== Sub Entry {i+1} ===")
        if title_div:
            link = title_div.find('a')
            if link:
                print(f"Title: {link.get_text(strip=True)}")
                print(f"URL: {link.get('href')}")
            full_text = title_div.get_text(strip=True)
            print(f"Title div text: {full_text[:150]}")
        
        if content_div:
            text = content_div.get_text(strip=True)
            print(f"Content: {text[:300]}")
else:
    print("No secondary-legislation tab found!")
    # Check all tab IDs
    for tab in tabs:
        print(f"  Tab ID: {tab.get('id')}")

# Count total entries
total_primary = len(primary_tab.find_all('div', class_='accordion-section')) if primary_tab else 0
total_secondary = len(secondary_tab.find_all('div', class_='accordion-section')) if secondary_tab else 0
print(f"\nTotal primary: {total_primary}")
print(f"Total secondary: {total_secondary}")
print(f"Grand total: {total_primary + total_secondary}")
