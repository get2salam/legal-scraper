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

# ============================================================
# TOPICS: topicid is on <tr class="topicType" topicid="...">
# ============================================================
print("\n=== TOPICS ===")
topics_dir = out_base / "topics"

with open(topics_dir / "listing.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")
right_div = soup.find("div", id="rightmenu") or soup

topics_list = []
for row in right_div.find_all("tr", class_="topicType"):
    topicid = row.get("topicid")
    tds = row.find_all("td")
    if tds and topicid:
        num = tds[0].get_text(strip=True)
        title = tds[1].get_text(strip=True) if len(tds) > 1 else ""
        if title:
            topics_list.append({"topicid": topicid, "num": num, "title": title})

print(f"Topics: {len(topics_list)}")
for t in topics_list[:5]:
    print(f"  [{t['topicid']}] {t['title']}")

with open(topics_dir / "topics_list.json", "w", encoding="utf-8") as f:
    json.dump(topics_list, f, ensure_ascii=False, indent=2)

# Fetch sub-topic content for each topic
print(f"\nFetching sub-topics for {len(topics_list)} topics...")
topics_full = []
errors = []

for i, topic in enumerate(topics_list):
    time.sleep(1.5)
    try:
        resp = s.post(f"{B}/Login/GetSubTopic", data={"topicId": topic["topicid"]}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 100:
            tsoup = BeautifulSoup(resp.text, "html.parser")
            
            # Parse case list from subtopic
            cases = []
            for row in tsoup.find_all("tr"):
                casetypeid = row.get("casetypeid")
                tds_r = row.find_all("td")
                if tds_r and len(tds_r) >= 2:
                    text = row.get_text(strip=True)
                    if text and len(text) > 5:
                        cases.append({"casetypeid": casetypeid, "text": text[:300]})
            
            plain_text = tsoup.get_text(separator="\n", strip=True)
            safe_id = str(topic["topicid"]).replace("/", "_")
            with open(topics_dir / f"topic_{safe_id}.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            
            topics_full.append({
                **topic,
                "cases": cases[:100],  # cap at 100
                "case_count": len(cases),
                "preview": plain_text[:2000],
            })
            print(f"  [{i+1}/{len(topics_list)}] {topic['title'][:50]}: {len(cases)} cases")
        else:
            print(f"  [{i+1}] {topic['title'][:50]}: status={resp.status_code}")
            topics_full.append({**topic, "cases": [], "error": f"status={resp.status_code}"})
    except Exception as e:
        print(f"  [{i+1}] {topic['title'][:50]}: ERROR: {e}")
        topics_full.append({**topic, "cases": [], "error": str(e)})

with open(topics_dir / "topics_full.json", "w", encoding="utf-8") as f:
    json.dump(topics_full, f, ensure_ascii=False, indent=2)
print(f"Topics saved: {len(topics_full)}")

# ============================================================
# WORDS & PHRASES: Use WordsAndPhrasesCharSearch
# ============================================================
print("\n=== WORDS & PHRASES ===")
wp_dir = out_base / "words_phrases"

# The listing page already has all W&P entries - extract them now
with open(wp_dir / "listing.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")
right_div = soup.find("div", id="rightmenu") or soup

# Find W&P table - entries have the phrase name in col 2 and type in col 3
# The JS shows: WordsAndPhrasesCharSearch and SearchCaseByCaseTypeId
# Let's find what IDs exist
wp_entries = []
for row in right_div.find_all("tr"):
    tds = row.find_all("td")
    if len(tds) >= 3:
        num = tds[0].get_text(strip=True)
        name = tds[2].get_text(strip=True)
        entry_type = tds[3].get_text(strip=True) if len(tds) > 3 else ""
        # Check for any ID attributes on the row or its children
        row_id = row.get("casetypeid") or row.get("wordid") or row.get("id")
        # Also check for buttons or anchors
        for child in row.find_all(["button","input","a"]):
            for attr in ["casetypeid","wordid","data-id","id"]:
                if child.get(attr):
                    row_id = child.get(attr)
                    break
        if name and len(name) > 2:
            wp_entries.append({"id": row_id, "num": num, "name": name, "type": entry_type})

print(f"W&P from listing: {len(wp_entries)}")
for e in wp_entries[:5]:
    print(f"  [{e['id']}] {e['name']}")

# Also check: does the table have a class or the rows have specific classes?
for row in right_div.find_all("tr")[:5]:
    print(f"  Row class={row.get('class')}, attrs: {dict(row.attrs)}")

# Try the CharSearch for each letter with 'words' type
print("\nFetching W&P by letter using CharSearch...")
all_wp = {}
seen_wp = set()

for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(1)
    try:
        resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": letter, "type": "words"}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, "html.parser")
            entries = []
            for row in csoup.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) >= 2:
                    # Check all attrs
                    row_id = row.get("casetypeid") or row.get("wordid")
                    for child in row.find_all(["button","input","a","td"]):
                        for attr in ["casetypeid","wordid","onclick"]:
                            if child.get(attr):
                                row_id = child.get(attr)
                                break
                    name = tds[-2].get_text(strip=True) if len(tds) >= 2 else tds[-1].get_text(strip=True)
                    if name and len(name) > 2 and name not in seen_wp:
                        seen_wp.add(name)
                        entries.append({"id": row_id, "name": name})
            if entries:
                all_wp[letter] = entries
                with open(wp_dir / f"char_words_{letter}.html", "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"  W&P words letter {letter}: {len(entries)} new entries")
            else:
                print(f"  W&P words letter {letter}: 0 new (response: {len(resp.text)} chars)")
    except Exception as e:
        print(f"  W&P {letter}: {e}")

wp_flat = [{"letter": letter, **e} for letter, entries in all_wp.items() for e in entries]
print(f"\nTotal W&P entries: {len(wp_flat)}")

with open(wp_dir / "words_phrases_all.json", "w", encoding="utf-8") as f:
    json.dump(wp_flat, f, ensure_ascii=False, indent=2)

# Also fetch 'maxim' type
print("\nFetching Maxims by letter using CharSearch...")
maxims_dir = out_base / "maxims"
all_maxims = {}
seen_maxims = set()

for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(0.8)
    try:
        resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": letter, "type": "maxim"}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, "html.parser")
            entries = []
            for row in csoup.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) >= 2:
                    row_id = row.get("casetypeid") or row.get("wordid")
                    name = tds[-2].get_text(strip=True) if len(tds) >= 2 else ""
                    if name and len(name) > 2 and name not in seen_maxims:
                        seen_maxims.add(name)
                        entries.append({"id": row_id, "name": name})
            if entries:
                all_maxims[letter] = entries
                with open(maxims_dir / f"char_maxim_{letter}.html", "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"  Maxim letter {letter}: {len(entries)} new entries")
    except Exception as e:
        print(f"  Maxim {letter}: {e}")

maxims_flat = [{"letter": letter, **e} for letter, entries in all_maxims.items() for e in entries]
print(f"\nTotal Maxims: {len(maxims_flat)}")
if maxims_flat:
    with open(maxims_dir / "maxims_all.json", "w", encoding="utf-8") as f:
        json.dump(maxims_flat, f, ensure_ascii=False, indent=2)

# Also fetch 'legal terms' type  
print("\nFetching Legal Terms by letter...")
lt_dir = out_base / "legal_terms"
all_lt = {}
seen_lt = set()

for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(0.8)
    try:
        resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": letter, "type": "legalterms"}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, "html.parser")
            entries = []
            for row in csoup.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) >= 2:
                    row_id = row.get("casetypeid") or row.get("wordid")
                    name = tds[-2].get_text(strip=True) if len(tds) >= 2 else ""
                    if name and len(name) > 2 and name not in seen_lt:
                        seen_lt.add(name)
                        entries.append({"id": row_id, "name": name})
            if entries:
                all_lt[letter] = entries
                with open(lt_dir / f"char_lt_{letter}.html", "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"  Legal Terms letter {letter}: {len(entries)} new entries")
    except Exception as e:
        print(f"  Legal Terms {letter}: {e}")

lt_flat = [{"letter": letter, **e} for letter, entries in all_lt.items() for e in entries]
print(f"\nTotal Legal Terms: {len(lt_flat)}")
if lt_flat:
    with open(lt_dir / "legal_terms_all.json", "w", encoding="utf-8") as f:
        json.dump(lt_flat, f, ensure_ascii=False, indent=2)

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n=== FINAL SUMMARY ===")
summary = {
    "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    "sections": {}
}
for section in ["articles","words_phrases","dictionary","topics","maxims","legal_terms"]:
    d = out_base / section
    if d.exists():
        files = list(d.iterdir())
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        summary["sections"][section] = {"files": len(files), "size_kb": round(total_size/1024,1)}

for k, v in summary["sections"].items():
    print(f"  {k}: {v['files']} files, {v['size_kb']} KB")

with open(out_base / "summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print("\nAll done!")
