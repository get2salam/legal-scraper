import os, sys, time, json, re
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from dotenv import load_dotenv
load_dotenv('.env')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session(); s.impersonate = "chrome"
B = "https://www.pakistanlawsite.com"
u = os.getenv("PLS_USER",""); p = os.getenv("PLS_PASS","")
print(f"Logging in as: {u}")
s.post(f"{B}/Login/ClearLoginHistory", data={"Login.UserName":u,"Login.Password":p}, timeout=30)
time.sleep(2)

out_base = Path("data_v2/pls_extras")

def parse_listing_table(html):
    """Parse listing table: cols are [num, btn/id, title, type]"""
    soup = BeautifulSoup(html, "html.parser")
    right_div = soup.find("div", id="rightmenu") or soup
    table = right_div.find("table")
    entries = []
    if not table:
        return entries
    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        num = tds[0].get_text(strip=True)
        title = tds[2].get_text(strip=True)  # col 2 has the name
        entry_type = tds[3].get_text(strip=True) if len(tds) > 3 else ""
        
        # Get ID from the button/input in col 1
        entry_id = None
        btn = tds[1].find(attrs={"casetypeid": True}) or tds[1].find(attrs={"topicid": True}) or tds[1].find(attrs={"wordid": True})
        if btn:
            entry_id = btn.get("casetypeid") or btn.get("topicid") or btn.get("wordid")
        
        if title and not title.startswith("#") and title != "Title":
            entries.append({"id": entry_id, "num": num, "name": title, "type": entry_type})
    return entries

# ============================================================
# TOPICS
# ============================================================
print("\n=== TOPICS ===")
topics_dir = out_base / "topics"

with open(topics_dir / "listing.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()

topics_list = parse_listing_table(html)
print(f"Topics: {len(topics_list)}")
for t in topics_list[:5]:
    print(f"  [{t['id']}] {t['name']}")

with open(topics_dir / "topics_list.json", "w", encoding="utf-8") as f:
    json.dump(topics_list, f, ensure_ascii=False, indent=2)

# Inspect what the table actually has (col 1 element names)
soup = BeautifulSoup(html, "html.parser")
right_div = soup.find("div", id="rightmenu")
if right_div:
    table = right_div.find("table")
    if table:
        rows = table.find_all("tr")[:5]
        print(f"\nFirst 5 rows raw inspection:")
        for row in rows:
            tds = row.find_all("td")
            if tds:
                print(f"  Row: {[str(td)[:100] for td in tds]}")

# ============================================================
# WORDS & PHRASES - Use char search endpoint
# ============================================================
print("\n=== WORDS & PHRASES (char search) ===")
wp_dir = out_base / "words_phrases"

# The endpoint is: /Login/WordsAndPhrasesCharSearch
# Let's test it
time.sleep(2)
resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": "a", "type": "words"}, timeout=15)
print(f"WP CharSearch 'a' words: status={resp.status_code}, size={len(resp.text)}")
print(f"Preview:\n{resp.text[:500]}")

with open(wp_dir / "char_a_test.html", "w", encoding="utf-8") as f:
    f.write(resp.text)

time.sleep(1)
# Also try without type
resp2 = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": "a"}, timeout=15)
print(f"\nWP CharSearch 'a' (no type): status={resp2.status_code}, size={len(resp2.text)}")

time.sleep(1)
# Try maxim type
resp3 = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": "a", "type": "maxim"}, timeout=15)
print(f"\nWP CharSearch 'a' maxim: status={resp3.status_code}, size={len(resp3.text)}")
print(f"Preview:\n{resp3.text[:300]}")

# ============================================================
# W&P individual entry content - use SearchCaseByCaseTypeId
# ============================================================
# Parse the W&P listing to get IDs
wp_entries = parse_listing_table(open(wp_dir / "listing.html", encoding="utf-8", errors="replace").read())
print(f"\nW&P listing entries: {len(wp_entries)}")
for e in wp_entries[:5]:
    print(f"  [{e['id']}] {e['name']}")

# Now try to fetch individual W&P entry
if wp_entries and wp_entries[0]["id"]:
    time.sleep(2)
    test_id = wp_entries[0]["id"]
    resp = s.post(f"{B}/Login/SearchCaseByCaseTypeId", data={"caseTypeId": test_id}, timeout=15)
    print(f"\nSearchCaseByCaseTypeId '{test_id}': status={resp.status_code}, size={len(resp.text)}")
    print(f"Preview: {resp.text[:300]}")
    with open(wp_dir / f"entry_test_{test_id}.html", "w", encoding="utf-8") as f:
        f.write(resp.text)
else:
    # Try with a known ID format - check what format IDs are in
    print("\nNo IDs found in W&P listing, checking raw HTML...")
    soup = BeautifulSoup(open(wp_dir / "listing.html", encoding="utf-8", errors="replace").read(), "html.parser")
    right_div = soup.find("div", id="rightmenu")
    if right_div:
        table = right_div.find("table")
        if table:
            # Check all attributes on td/tr elements
            for row in table.find_all("tr")[:5]:
                for td in row.find_all("td"):
                    attrs = {k: v for k, v in td.attrs.items() if k != "class"}
                    for child in td.children:
                        if hasattr(child, 'attrs') and child.attrs:
                            print(f"  child: {child.name}, attrs: {child.attrs}")

# ============================================================
# LEGAL TERMS - Parse and get IDs
# ============================================================
print("\n=== LEGAL TERMS ===")
lt_dir = out_base / "legal_terms"
lt_entries = parse_listing_table(open(lt_dir / "legal_terms.html", encoding="utf-8", errors="replace").read())
print(f"Legal Terms entries: {len(lt_entries)}")
for e in lt_entries[:5]:
    print(f"  [{e['id']}] {e['name']}")

# Check for IDs
print("\nChecking raw structure of legal terms table:")
soup = BeautifulSoup(open(lt_dir / "legal_terms.html", encoding="utf-8", errors="replace").read(), "html.parser")
right_div = soup.find("div", id="rightmenu")
if right_div:
    table = right_div.find("table")
    if table:
        for row in table.find_all("tr")[:5]:
            tds = row.find_all("td")
            if tds:
                # Print raw HTML of each TD
                for td in tds[:4]:
                    print(f"  TD: {str(td)[:150]}")
                print("  ---")

# ============================================================
# MAXIMS - Parse and get IDs
# ============================================================
print("\n=== MAXIMS ===")
maxims_dir = out_base / "maxims"
maxim_entries = parse_listing_table(open(maxims_dir / "maxims.html", encoding="utf-8", errors="replace").read())
print(f"Maxim entries: {len(maxim_entries)}")
for e in maxim_entries[:5]:
    print(f"  [{e['id']}] {e['name']}")

print("\nDone!")
