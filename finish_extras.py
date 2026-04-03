import os, sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from dotenv import load_dotenv
load_dotenv('.env')
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session()
s.impersonate = "chrome"
B = "https://www.pakistanlawsite.com"
u = os.getenv("PLS_USER", "")
p = os.getenv("PLS_PASS", "")

print("Logging in...")
resp = s.post(f"{B}/Login/ClearLoginHistory",
              data={"Login.UserName": u, "Login.Password": p},
              timeout=30)
print(f"Login: {resp.status_code}")
time.sleep(3)

base = Path("data_v2/pls_extras")
topics_dir = base / "topics"
articles_dir = base / "articles"
wp_dir = base / "words_phrases"

# =============================================================================
# TASK 1: Download missing topic pages
# =============================================================================
print("\n=== TASK 1: Topics ===")
topics = json.load(open(topics_dir / "topics_list.json", encoding="utf-8"))
print(f"Total topics in list: {len(topics)}")

existing_ids = set(f.stem.replace("topic_", "") for f in topics_dir.glob("topic_*.html"))
print(f"Already downloaded: {len(existing_ids)}")

downloaded = 0
skipped = 0
failed = 0
seen_ids = set()

for topic in topics:
    tid = str(topic["topicid"])
    if tid in seen_ids:
        continue
    seen_ids.add(tid)

    out_file = topics_dir / f"topic_{tid}.html"
    if out_file.exists():
        skipped += 1
        continue

    time.sleep(1)
    try:
        resp = s.get(f"{B}/Login/TopicPage", params={"topicid": tid}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 1000:
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(resp.text)
            downloaded += 1
            print(f"  Downloaded topic {tid}: {topic['title'][:60]}")
        else:
            print(f"  Failed topic {tid}: status={resp.status_code} len={len(resp.text)}")
            failed += 1
    except Exception as e:
        print(f"  Error topic {tid}: {e}")
        failed += 1

print(f"\nTopics: downloaded={downloaded}, skipped={skipped}, failed={failed}")

# =============================================================================
# TASK 2: Find all articles using ArticlesCharSearch (A-Z) + year search
# =============================================================================
print("\n=== TASK 2: Articles ===")

all_articles = []
seen_article_ids = set()

def parse_article_rows(html, year_hint=None):
    """Parse caseType rows from ArticlesSearch/ArticlesCharSearch response."""
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr", class_="caseType")
    items = []
    for row in rows:
        tds = row.find_all("td")
        btn = row.find("input", attrs={"casetypeid": True})
        if not btn:
            # Also check the row itself for casetypeid attr
            cid = row.get("casetypeid", "")
        else:
            cid = btn.get("casetypeid", "")
        
        if not cid:
            # Try data-casetypeid
            cid = row.get("data-casetypeid", "")
        
        title = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
        author = tds[2].get_text(strip=True) if len(tds) >= 3 else ""
        category = tds[3].get_text(strip=True) if len(tds) >= 4 else ""
        year = tds[4].get_text(strip=True) if len(tds) >= 5 else (year_hint or "")
        
        if cid:
            items.append({
                "id": cid,
                "title": title,
                "author": author,
                "category": category,
                "year": year
            })
    return items

# Method 1: Search by year (2000-2026)
print("\nSearching by year...")
for year in range(2000, 2027):
    time.sleep(1)
    try:
        resp = s.get(f"{B}/Login/ArticlesSearch", params={"text": str(year)}, timeout=15)
        items = parse_article_rows(resp.text, str(year))
        new = 0
        for item in items:
            if item["id"] not in seen_article_ids:
                seen_article_ids.add(item["id"])
                all_articles.append(item)
                new += 1
        if items:
            print(f"  Year {year}: {len(items)} rows, {new} new")
        else:
            pass  # silently skip empty years
    except Exception as e:
        print(f"  Year {year}: error {e}")

print(f"\nAfter year search: {len(all_articles)} unique articles")

# Method 2: Search by A-Z character  
print("\nSearching by character A-Z...")
for char in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    time.sleep(1)
    try:
        resp = s.get(f"{B}/Login/ArticlesCharSearch", params={"character": char}, timeout=15)
        items = parse_article_rows(resp.text)
        new = 0
        for item in items:
            if item["id"] not in seen_article_ids:
                seen_article_ids.add(item["id"])
                all_articles.append(item)
                new += 1
        if items:
            print(f"  Char {char}: {len(items)} rows, {new} new")
    except Exception as e:
        print(f"  Char {char}: error {e}")

print(f"\nAfter A-Z search: {len(all_articles)} unique articles")

# Save articles list
if all_articles:
    with open(articles_dir / "all_articles_list.json", "w", encoding="utf-8") as f:
        json.dump(all_articles, f, ensure_ascii=False, indent=2)
    print(f"Saved all_articles_list.json with {len(all_articles)} entries")
else:
    print("No articles found!")

# =============================================================================
# TASK 3: Download full text of each article via GetArticleFile (POST)
# =============================================================================
print("\n=== TASK 3: Download article full text ===")

fetched = 0
empty = 0
articles_full = []
seen_fetch_ids = set()

for art in all_articles:
    cid = art.get("id", "")
    if not cid or cid in seen_fetch_ids:
        continue
    seen_fetch_ids.add(cid)

    time.sleep(2)
    try:
        resp = s.post(f"{B}/Login/GetArticleFile",
                      data={"caseName": cid},
                      timeout=30)
        text = resp.text
        
        # Try JSON parse
        try:
            parsed = json.loads(text)
            if isinstance(parsed, str):
                text = parsed
        except Exception:
            pass
        
        if isinstance(text, str) and len(text) > 100 and text.strip() != "1":
            art_data = dict(art)
            art_data["body"] = text[:100000]  # cap at 100KB
            articles_full.append(art_data)
            fetched += 1
            print(f"  Fetched [{cid}]: {art['title'][:60]} ({len(text)} chars)")
            
            # Also save individual HTML
            out_file = articles_dir / f"article_{cid}.html"
            with open(out_file, "w", encoding="utf-8") as f:
                f.write(text)
        else:
            print(f"  Empty/error [{cid}]: response={text[:80]}")
            empty += 1
    except Exception as e:
        print(f"  Error [{cid}]: {e}")
        empty += 1

if articles_full:
    with open(articles_dir / "articles_full.json", "w", encoding="utf-8") as f:
        json.dump(articles_full, f, ensure_ascii=False, indent=2)

print(f"\nArticles: fetched={fetched}, empty/error={empty}")

# =============================================================================
# TASK 4: Parse Words & Phrases from all letter HTML files
# =============================================================================
print("\n=== TASK 4: Words & Phrases ===")

all_entries = []
seen_phrase_ids = set()

# Parse each letter file
for letter in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    letter_file = wp_dir / f"letter_{letter}.html"
    if not letter_file.exists():
        continue
    
    try:
        html = letter_file.read_text(encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        
        rows = soup.find_all("tr", class_="caseType")
        for row in rows:
            tds = row.find_all("td")
            btn = row.find("input", attrs={"casetypeid": True})
            cid = btn.get("casetypeid", "") if btn else row.get("casetypeid", "")
            
            phrase = tds[0].get_text(strip=True) if len(tds) >= 1 else ""
            citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
            definition = tds[2].get_text(strip=True) if len(tds) >= 3 else ""
            
            key = cid if cid else phrase
            if phrase and key not in seen_phrase_ids:
                seen_phrase_ids.add(key)
                all_entries.append({
                    "phrase": phrase,
                    "citation": citation,
                    "definition": definition,
                    "id": cid,
                    "letter": letter
                })
    except Exception as e:
        print(f"  Error parsing letter {letter}: {e}")

# Also parse the main listing.html
listing_file = wp_dir / "listing.html"
if listing_file.exists():
    html = listing_file.read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr", class_="caseType")
    for row in rows:
        tds = row.find_all("td")
        btn = row.find("input", attrs={"casetypeid": True})
        cid = btn.get("casetypeid", "") if btn else row.get("casetypeid", "")
        
        phrase = tds[0].get_text(strip=True) if len(tds) >= 1 else ""
        citation = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
        definition = tds[2].get_text(strip=True) if len(tds) >= 3 else ""
        
        key = cid if cid else phrase
        if phrase and key not in seen_phrase_ids:
            seen_phrase_ids.add(key)
            all_entries.append({
                "phrase": phrase,
                "citation": citation,
                "definition": definition,
                "id": cid,
                "letter": phrase[0].upper() if phrase else ""
            })

print(f"Words & Phrases entries: {len(all_entries)}")
for e in all_entries[:5]:
    print(f"  [{e['letter']}] {e['phrase'][:50]}: {e['citation'][:50]}")

if all_entries:
    with open(wp_dir / "words_phrases.json", "w", encoding="utf-8") as f:
        json.dump(all_entries, f, ensure_ascii=False, indent=2)
    print(f"Saved words_phrases.json with {len(all_entries)} entries")

# =============================================================================
# FINAL SUMMARY
# =============================================================================
print("\n=== FINAL SUMMARY ===")
for section in ["topics", "articles", "words_phrases", "dictionary", "legal_terms", "maxims"]:
    section_dir = base / section
    if section_dir.exists():
        files = list(section_dir.iterdir())
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        print(f"{section}: {len(files)} files, {total_size/1024:.1f} KB total")

# Topics detail
topic_htmls = list(topics_dir.glob("topic_*.html"))
print(f"\nTopic detail pages: {len(topic_htmls)}/{len(topics)}")

# Articles detail
article_htmls = list(articles_dir.glob("article_*.html"))
print(f"Article full text files: {len(article_htmls)}")
if all_articles:
    print(f"Articles in list: {len(all_articles)}")
