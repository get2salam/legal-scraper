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
out_base.mkdir(parents=True, exist_ok=True)

# ============================================================
# STEP 2: Articles
# ============================================================
articles_dir = out_base / "articles"
articles_dir.mkdir(exist_ok=True)

resp = s.get(f"{B}/Login/ArticlePage", timeout=20)
soup = BeautifulSoup(resp.text, "html.parser")

articles = []
for item in soup.find_all(["div","tr","li"], class_=lambda c: c and any(x in c for x in ["article","item","result","entry"])):
    title_el = item.find(["h3","h4","a","strong"])
    if title_el:
        title = title_el.get_text(strip=True)
        link = title_el.get("href","") if title_el.name == "a" else ""
        if title and len(title) > 10:
            articles.append({"title": title, "link": link})

for a in soup.find_all("a", href=True):
    href = a["href"]
    txt = a.get_text(strip=True)
    if len(txt) > 20 and ("article" in href.lower() or "article" in txt.lower()):
        articles.append({"title": txt, "link": href})

print(f"Articles found on listing page: {len(articles)}")
print(f"Page size: {len(resp.text)} chars")

with open(articles_dir / "listing.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
with open(articles_dir / "listing_text.txt", "w", encoding="utf-8") as f:
    f.write(soup.get_text(separator="\n", strip=True))
print("Articles listing saved")

# ============================================================
# STEP 3: Words & Phrases
# ============================================================
wp_dir = out_base / "words_phrases"
wp_dir.mkdir(exist_ok=True)

time.sleep(2)
resp = s.get(f"{B}/Login/WordsAndPhrases", params={"type":"words"}, timeout=20)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"\nWords & Phrases: {len(resp.text)} chars")

with open(wp_dir / "listing.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
with open(wp_dir / "listing_text.txt", "w", encoding="utf-8") as f:
    f.write(soup.get_text(separator="\n", strip=True))

entries = []
for row in soup.find_all("tr"):
    tds = row.find_all("td")
    if len(tds) >= 1:
        text = row.get_text(strip=True)
        if text and len(text) > 5:
            entries.append({"text": text})

print(f"W&P rows found: {len(entries)}")

time.sleep(2)
for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    try:
        resp2 = s.get(f"{B}/Login/WordsAndPhrases", params={"type":"words","letter":letter}, timeout=10)
        if resp2.status_code == 200 and len(resp2.text) > 5000:
            soup2 = BeautifulSoup(resp2.text, "html.parser")
            rows = soup2.find_all("tr")
            if rows:
                with open(wp_dir / f"letter_{letter}.html", "w", encoding="utf-8") as f:
                    f.write(resp2.text)
                print(f"  W&P letter {letter}: {len(rows)} rows")
        time.sleep(0.5)
    except Exception as e:
        print(f"  W&P {letter}: {e}")

# ============================================================
# STEP 4: Dictionary
# ============================================================
dict_dir = out_base / "dictionary"
dict_dir.mkdir(exist_ok=True)

time.sleep(2)
resp = s.get(f"{B}/Login/DictionaryPage", timeout=30)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"\nDictionary: {len(resp.text)} chars")

with open(dict_dir / "full_dictionary.html", "w", encoding="utf-8") as f:
    f.write(resp.text)

definitions = []
for row in soup.find_all("tr", class_="caseType"):
    tds = row.find_all("td")
    if len(tds) >= 1:
        term = tds[0].get_text(strip=True) if len(tds) >= 1 else ""
        definition = tds[1].get_text(strip=True) if len(tds) >= 2 else ""
        citation = tds[2].get_text(strip=True) if len(tds) >= 3 else ""
        if term:
            definitions.append({"term": term, "definition": definition, "citation": citation})

# Fallback: try all table rows
if not definitions:
    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) >= 2:
            term = tds[0].get_text(strip=True)
            definition = tds[1].get_text(strip=True)
            citation = tds[2].get_text(strip=True) if len(tds) >= 3 else ""
            if term and len(term) > 1 and len(term) < 200:
                definitions.append({"term": term, "definition": definition, "citation": citation})

print(f"Dictionary definitions extracted: {len(definitions)}")
if definitions:
    with open(dict_dir / "definitions.json", "w", encoding="utf-8") as f:
        json.dump(definitions, f, ensure_ascii=False, indent=2)
    for d in definitions[:3]:
        print(f"  {d['term']}: {d['definition'][:80]}")

# Also save plain text
with open(dict_dir / "dictionary_text.txt", "w", encoding="utf-8") as f:
    f.write(soup.get_text(separator="\n", strip=True))

# ============================================================
# STEP 5: Topics
# ============================================================
topics_dir = out_base / "topics"
topics_dir.mkdir(exist_ok=True)

time.sleep(2)
resp = s.get(f"{B}/Login/TopicPage", timeout=20)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"\nTopics: {len(resp.text)} chars")

with open(topics_dir / "listing.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
with open(topics_dir / "listing_text.txt", "w", encoding="utf-8") as f:
    f.write(soup.get_text(separator="\n", strip=True))

topics = []
for a in soup.find_all("a", href=True):
    href = a["href"]
    txt = a.get_text(strip=True)
    if len(txt) > 3 and ("topic" in href.lower() or len(txt) < 100):
        topics.append({"name": txt, "url": href})
print(f"Topics found: {len(topics)}")
for t in topics[:10]:
    print(f"  {t['name']} -> {t['url']}")

# ============================================================
# STEP 6: Maxims & Legal Terms
# ============================================================
time.sleep(2)
resp = s.get(f"{B}/Login/Maxim", params={"type":"maxim"}, timeout=20)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"\nMaxims: {len(resp.text)} chars")
(out_base / "maxims").mkdir(exist_ok=True)
with open(out_base / "maxims" / "maxims.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
with open(out_base / "maxims" / "maxims.txt", "w", encoding="utf-8") as f:
    f.write(soup.get_text(separator="\n", strip=True))

time.sleep(2)
resp = s.get(f"{B}/Login/LegalTerms", timeout=20)
soup = BeautifulSoup(resp.text, "html.parser")
print(f"Legal Terms: {len(resp.text)} chars")
(out_base / "legal_terms").mkdir(exist_ok=True)
with open(out_base / "legal_terms" / "legal_terms.html", "w", encoding="utf-8") as f:
    f.write(resp.text)
with open(out_base / "legal_terms" / "legal_terms.txt", "w", encoding="utf-8") as f:
    f.write(soup.get_text(separator="\n", strip=True))

# ============================================================
# STEP 7: Find & fetch individual article pages
# ============================================================
time.sleep(2)
resp = s.get(f"{B}/Login/ArticlePage", timeout=20)
soup = BeautifulSoup(resp.text, "html.parser")

article_links = []
for a in soup.find_all("a", href=True):
    href = a["href"]
    txt = a.get_text(strip=True)
    if len(txt) > 15 and any(x in href for x in ["Article","article","GetArticle","ArticleDetail"]):
        full_url = href if href.startswith("http") else B+href
        article_links.append({"title": txt, "url": full_url})

print(f"\nArticle links: {len(article_links)}")
for al in article_links[:10]:
    print(f"  {al['title'][:60]} -> {al['url']}")

articles_data = []
for al in article_links[:5]:
    time.sleep(2)
    try:
        resp2 = s.get(al["url"], timeout=15)
        soup2 = BeautifulSoup(resp2.text, "html.parser")
        body_text = soup2.get_text(separator="\n", strip=True)
        articles_data.append({"title": al["title"], "url": al["url"], "body": body_text[:10000]})
        print(f"  Fetched: {al['title'][:50]} ({len(body_text)} chars)")
    except Exception as e:
        print(f"  Failed: {al['title'][:50]}: {e}")

if articles_data:
    with open(articles_dir / "articles.json", "w", encoding="utf-8") as f:
        json.dump(articles_data, f, ensure_ascii=False, indent=2)

# ============================================================
# STEP 8: Summary
# ============================================================
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

print("\n=== SUMMARY ===")
for k, v in summary["sections"].items():
    print(f"  {k}: {v['files']} files, {v['size_kb']} KB")

with open(out_base / "summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print("\nDone!")
