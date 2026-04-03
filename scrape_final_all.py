import os, sys, time, json
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
# TOPICS - topicid is on <tr class="topicType" topicid="...">
# ============================================================
print("\n=== TOPICS ===")
topics_dir = out_base / "topics"

html = (topics_dir / "listing.html").read_text(encoding='utf-8', errors='replace')
soup = BeautifulSoup(html, 'html.parser')

topics_list = []
for row in soup.find_all("tr", class_="topicType"):
    topicid = row.get("topicid")
    tds = row.find_all("td")
    if tds and topicid:
        num = tds[0].get_text(strip=True)
        title = tds[1].get_text(strip=True) if len(tds) > 1 else ""
        if title:
            topics_list.append({"topicid": topicid, "num": num, "title": title})

print(f"Topics list: {len(topics_list)}")
(topics_dir / "topics_list.json").write_text(json.dumps(topics_list, ensure_ascii=False, indent=2), encoding='utf-8')

# Fetch subtopics - skip ones already downloaded
print(f"Fetching subtopics...")
topics_full = []
errors_t = []

for i, topic in enumerate(topics_list):
    safe_id = topic["topicid"]
    html_file = topics_dir / f"topic_{safe_id}.html"
    
    if html_file.exists():
        # Parse from existing
        th = html_file.read_text(encoding='utf-8', errors='replace')
    else:
        time.sleep(1.5)
        try:
            resp = s.post(f"{B}/Login/GetSubTopic", data={"topicId": topic["topicid"]}, timeout=15)
            if resp.status_code == 200 and len(resp.text) > 50:
                html_file.write_text(resp.text, encoding='utf-8')
                th = resp.text
            else:
                errors_t.append({"topic": topic["title"], "error": f"status={resp.status_code}"})
                print(f"  [{i+1}] {topic['title'][:40]}: failed status={resp.status_code}")
                continue
        except Exception as e:
            errors_t.append({"topic": topic["title"], "error": str(e)})
            print(f"  [{i+1}] {topic['title'][:40]}: ERROR {e}")
            continue

    # Parse subtopic HTML
    tsoup = BeautifulSoup(th, 'html.parser')
    cases = []
    for row in tsoup.find_all("tr", class_="searchCase"):
        cid = row.get("casetypeid")
        tds_r = row.find_all("td")
        text = row.get_text(separator=" | ", strip=True)
        if text:
            cases.append({"casetypeid": cid, "text": text[:300]})

    # Also try rows without class
    if not cases:
        for row in tsoup.find_all("tr"):
            tds_r = row.find_all("td")
            if len(tds_r) >= 2:
                text = row.get_text(strip=True)
                if text and len(text) > 5 and text != topic['title']:
                    cases.append({"casetypeid": row.get("casetypeid"), "text": text[:300]})

    topics_full.append({**topic, "case_count": len(cases), "cases": cases[:200]})
    print(f"  [{i+1}/{len(topics_list)}] {topic['title'][:45]}: {len(cases)} cases")

(topics_dir / "topics_full.json").write_text(json.dumps(topics_full, ensure_ascii=False, indent=2), encoding='utf-8')
print(f"Topics saved: {len(topics_full)}, errors: {len(errors_t)}")

# ============================================================
# WORDS & PHRASES - casetypeid is on <tr class="searchCase" casetypeid="...">
# ============================================================
print("\n=== WORDS & PHRASES ===")
wp_dir = out_base / "words_phrases"

html = (wp_dir / "listing.html").read_text(encoding='utf-8', errors='replace')
soup = BeautifulSoup(html, 'html.parser')

wp_entries = []
seen_wp_ids = set()

# Extract from listing
right = soup.find("div", id="rightmenu") or soup
for row in right.find_all("tr", class_="searchCase"):
    cid = row.get("casetypeid")
    tds = row.find_all("td")
    name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
    entry_type = tds[3].get_text(strip=True) if len(tds) > 3 else ""
    if name and cid and cid not in seen_wp_ids:
        seen_wp_ids.add(cid)
        wp_entries.append({"casetypeid": cid, "name": name, "type": entry_type})

print(f"W&P from listing: {len(wp_entries)}")

# Fetch more via WordsAndPhrasesCharSearch
print("Fetching W&P by letter using CharSearch...")
for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(1)
    try:
        resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": letter, "type": "words"}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, 'html.parser')
            new_count = 0
            for row in csoup.find_all("tr", class_="searchCase"):
                cid = row.get("casetypeid")
                tds = row.find_all("td")
                name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                entry_type = tds[3].get_text(strip=True) if len(tds) > 3 else ""
                if name and cid and cid not in seen_wp_ids:
                    seen_wp_ids.add(cid)
                    wp_entries.append({"casetypeid": cid, "name": name, "type": entry_type, "letter": letter})
                    new_count += 1
            if new_count:
                html_path = wp_dir / f"char_words_{letter}.html"
                html_path.write_text(resp.text, encoding='utf-8')
                print(f"  Letter {letter}: {new_count} new entries (total: {len(wp_entries)})")
    except Exception as e:
        print(f"  Letter {letter}: ERROR {e}")

print(f"\nTotal W&P: {len(wp_entries)}")
(wp_dir / "words_phrases_all.json").write_text(json.dumps(wp_entries, ensure_ascii=False, indent=2), encoding='utf-8')

# ============================================================
# MAXIMS - via WordsAndPhrasesCharSearch type=maxim
# ============================================================
print("\n=== MAXIMS ===")
maxims_dir = out_base / "maxims"

# Parse from existing HTML first
html = (maxims_dir / "maxims.html").read_text(encoding='utf-8', errors='replace')
soup = BeautifulSoup(html, 'html.parser')
right = soup.find("div", id="rightmenu") or soup

maxim_entries = []
seen_maxim_ids = set()

for row in right.find_all("tr", class_="searchCase"):
    cid = row.get("casetypeid")
    tds = row.find_all("td")
    name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
    if name and cid and cid not in seen_maxim_ids:
        seen_maxim_ids.add(cid)
        maxim_entries.append({"casetypeid": cid, "name": name})

print(f"Maxims from listing: {len(maxim_entries)}")

# Fetch by letter
for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(0.8)
    try:
        resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": letter, "type": "maxim"}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, 'html.parser')
            new_count = 0
            for row in csoup.find_all("tr", class_="searchCase"):
                cid = row.get("casetypeid")
                tds = row.find_all("td")
                name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                if name and cid and cid not in seen_maxim_ids:
                    seen_maxim_ids.add(cid)
                    maxim_entries.append({"casetypeid": cid, "name": name, "letter": letter})
                    new_count += 1
            if new_count:
                (maxims_dir / f"char_maxim_{letter}.html").write_text(resp.text, encoding='utf-8')
                print(f"  Maxim letter {letter}: {new_count} new")
    except Exception as e:
        print(f"  Maxim {letter}: ERROR {e}")

print(f"Total Maxims: {len(maxim_entries)}")
if maxim_entries:
    (maxims_dir / "maxims_all.json").write_text(json.dumps(maxim_entries, ensure_ascii=False, indent=2), encoding='utf-8')

# ============================================================
# LEGAL TERMS - via WordsAndPhrasesCharSearch type=legalterms
# ============================================================
print("\n=== LEGAL TERMS ===")
lt_dir = out_base / "legal_terms"

html = (lt_dir / "legal_terms.html").read_text(encoding='utf-8', errors='replace')
soup = BeautifulSoup(html, 'html.parser')
right = soup.find("div", id="rightmenu") or soup

lt_entries = []
seen_lt_ids = set()

for row in right.find_all("tr", class_="searchCase"):
    cid = row.get("casetypeid")
    tds = row.find_all("td")
    name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
    if name and cid and cid not in seen_lt_ids:
        seen_lt_ids.add(cid)
        lt_entries.append({"casetypeid": cid, "name": name})

print(f"Legal Terms from listing: {len(lt_entries)}")

# Fetch more by letter
for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(0.8)
    try:
        resp = s.get(f"{B}/Login/WordsAndPhrasesCharSearch", params={"text": letter, "type": "legalterms"}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, 'html.parser')
            new_count = 0
            for row in csoup.find_all("tr", class_="searchCase"):
                cid = row.get("casetypeid")
                tds = row.find_all("td")
                name = tds[2].get_text(strip=True) if len(tds) > 2 else ""
                if name and cid and cid not in seen_lt_ids:
                    seen_lt_ids.add(cid)
                    lt_entries.append({"casetypeid": cid, "name": name, "letter": letter})
                    new_count += 1
            if new_count:
                (lt_dir / f"char_lt_{letter}.html").write_text(resp.text, encoding='utf-8')
                print(f"  Legal Terms letter {letter}: {new_count} new")
    except Exception as e:
        print(f"  Legal Terms {letter}: ERROR {e}")

print(f"Total Legal Terms: {len(lt_entries)}")
if lt_entries:
    (lt_dir / "legal_terms_all.json").write_text(json.dumps(lt_entries, ensure_ascii=False, indent=2), encoding='utf-8')

# ============================================================
# ARTICLES - already have the 2026J1 article; also try ArticlesSearch
# ============================================================
print("\n=== ARTICLES ===")
articles_dir = out_base / "articles"

# Parse the listing we already have
html = (articles_dir / "listing.html").read_text(encoding='utf-8', errors='replace')
soup = BeautifulSoup(html, 'html.parser')

articles_all = []
seen_article_ids = set()

for row in soup.find_all("tr", attrs={"casetypeid": True}):
    cid = row.get("casetypeid")
    tds = row.find_all("td")
    if tds and cid and cid not in seen_article_ids:
        seen_article_ids.add(cid)
        articles_all.append({
            "casetypeid": cid,
            "num": tds[0].get_text(strip=True),
            "title": tds[1].get_text(strip=True) if len(tds) > 1 else "",
            "author": tds[2].get_text(strip=True) if len(tds) > 2 else "",
            "category": tds[3].get_text(strip=True) if len(tds) > 3 else "",
            "year": tds[4].get_text(strip=True) if len(tds) > 4 else "",
        })

print(f"Articles from listing: {len(articles_all)}")

# Try articles search with common letter combinations to find more
print("Searching for more articles by letter...")
for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(0.8)
    try:
        resp = s.get(f"{B}/Login/ArticlesSearch", params={"text": letter}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 200:
            csoup = BeautifulSoup(resp.text, 'html.parser')
            for row in csoup.find_all("tr", attrs={"casetypeid": True}):
                cid = row.get("casetypeid")
                tds = row.find_all("td")
                if tds and cid and cid not in seen_article_ids:
                    seen_article_ids.add(cid)
                    articles_all.append({
                        "casetypeid": cid,
                        "title": tds[1].get_text(strip=True) if len(tds) > 1 else "",
                        "author": tds[2].get_text(strip=True) if len(tds) > 2 else "",
                        "year": tds[4].get_text(strip=True) if len(tds) > 4 else "",
                        "search_letter": letter,
                    })
                    print(f"  Found via letter {letter}: [{cid}]")
    except Exception as e:
        print(f"  Article search {letter}: ERROR {e}")

print(f"\nTotal unique articles: {len(articles_all)}")
(articles_dir / "articles_all.json").write_text(json.dumps(articles_all, ensure_ascii=False, indent=2), encoding='utf-8')

# Fetch full article content for any we don't have yet
print("Fetching article content...")
articles_full = []
for art in articles_all:
    cid = art["casetypeid"]
    safe_id = cid.replace("/", "_")
    html_file = articles_dir / f"article_{safe_id}.html"
    
    if html_file.exists():
        ah = html_file.read_text(encoding='utf-8', errors='replace')
    else:
        time.sleep(2)
        resp = s.post(f"{B}/Login/GetArticleFile", data={"caseName": cid}, timeout=20)
        if resp.status_code == 200 and resp.text.strip() != "1" and len(resp.text) > 100:
            html_file.write_text(resp.text, encoding='utf-8')
            ah = resp.text
        else:
            print(f"  Failed to fetch {cid}: status={resp.status_code}")
            continue
    
    asoup = BeautifulSoup(ah, 'html.parser')
    body = asoup.get_text(separator="\n", strip=True)
    articles_full.append({**art, "body": body[:20000], "body_length": len(body)})
    print(f"  [{cid}] {art.get('title','')[:50]} ({len(body)} chars)")

(articles_dir / "articles_full.json").write_text(json.dumps(articles_full, ensure_ascii=False, indent=2), encoding='utf-8')
print(f"Articles with content: {len(articles_full)}")

# ============================================================
# DICTIONARY - already have 3057 entries in definitions.json
# ============================================================
print("\n=== DICTIONARY ===")
defs = json.loads((out_base / "dictionary" / "definitions.json").read_text(encoding='utf-8', errors='replace'))
print(f"Dictionary: {len(defs)} entries already saved")
for d in defs[:3]:
    print(f"  {d['term']}: {d['definition'][:60]}")

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n=== FINAL SUMMARY ===")
summary = {"scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"), "sections": {}}

counts = {
    "dictionary": len(defs),
    "topics": len(topics_full),
    "words_phrases": len(wp_entries),
    "maxims": len(maxim_entries),
    "legal_terms": len(lt_entries),
    "articles": len(articles_full),
}

for section in ["articles","words_phrases","dictionary","topics","maxims","legal_terms"]:
    d = out_base / section
    if d.exists():
        files = list(d.iterdir())
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        summary["sections"][section] = {
            "files": len(files),
            "size_kb": round(total_size/1024, 1),
            "entries": counts.get(section, 0)
        }
    print(f"  {section}: {counts.get(section,0)} entries")

(out_base / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
print("\nAll done!")
