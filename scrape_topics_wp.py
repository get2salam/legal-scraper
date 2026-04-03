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
# TOPICS - Extract all topics and sub-topics
# ============================================================
topics_dir = out_base / "topics"
topics_dir.mkdir(exist_ok=True)

print("=== TOPICS ===")
with open(topics_dir / "listing.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")

# Find the topics table
table = soup.find("table")
topic_rows = []
if table:
    rows = table.find_all("tr")
    print(f"Topic table rows: {len(rows)}")
    for row in rows:
        topicid = row.get("topicid")
        tds = row.find_all("td")
        if not topicid:
            # Check buttons inside
            btn = row.find(attrs={"topicid": True})
            if btn:
                topicid = btn.get("topicid")
        if tds:
            num = tds[0].get_text(strip=True)
            title = tds[1].get_text(strip=True) if len(tds) > 1 else ""
            if title and title != "Title":  # Skip header
                topic_rows.append({"topicid": topicid, "num": num, "title": title})

print(f"Topics extracted: {len(topic_rows)}")
for t in topic_rows[:10]:
    print(f"  [{t['topicid']}] {t['title']}")

# Save topic list
with open(topics_dir / "topics_list.json", "w", encoding="utf-8") as f:
    json.dump(topic_rows, f, ensure_ascii=False, indent=2)

# Now fetch sub-topics for each topic via POST /Login/GetSubTopic
print(f"\nFetching sub-topics for {len(topic_rows)} topics...")
topics_with_subtopics = []

for i, topic in enumerate(topic_rows):
    if not topic["topicid"]:
        print(f"  [{i+1}] {topic['title'][:50]}: NO topicid, skipping")
        continue
    time.sleep(1.5)
    try:
        resp = s.post(f"{B}/Login/GetSubTopic", data={"topicId": topic["topicid"]}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 50:
            tsoup = BeautifulSoup(resp.text, "html.parser")
            subtopic_text = tsoup.get_text(separator="\n", strip=True)
            
            # Try to extract structured sub-topics
            subtopics = []
            for row in tsoup.find_all("tr"):
                tds = row.find_all("td")
                if tds:
                    text = row.get_text(strip=True)
                    if text and len(text) > 5:
                        subtopics.append({
                            "text": text,
                            "topicid": row.get("topicid") or (row.find(attrs={"topicid": True}).get("topicid") if row.find(attrs={"topicid": True}) else None)
                        })
            
            # Save raw HTML
            safe_id = str(topic["topicid"]).replace("/", "_")
            with open(topics_dir / f"topic_{safe_id}.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            
            topics_with_subtopics.append({
                **topic,
                "subtopics": subtopics,
                "subtopic_count": len(subtopics),
                "raw_text": subtopic_text[:5000],
            })
            print(f"  [{i+1}/{len(topic_rows)}] {topic['title'][:50]}: {len(subtopics)} subtopics")
        else:
            print(f"  [{i+1}] {topic['title'][:50]}: status={resp.status_code}, size={len(resp.text)}")
            topics_with_subtopics.append({**topic, "subtopics": [], "error": f"status={resp.status_code}"})
    except Exception as e:
        print(f"  [{i+1}] {topic['title'][:50]}: ERROR: {e}")
        topics_with_subtopics.append({**topic, "subtopics": [], "error": str(e)})

with open(topics_dir / "topics_full.json", "w", encoding="utf-8") as f:
    json.dump(topics_with_subtopics, f, ensure_ascii=False, indent=2)
print(f"\nTopics with subtopics saved: {len(topics_with_subtopics)}")

# ============================================================
# WORDS & PHRASES - Fix extraction
# ============================================================
print("\n=== WORDS & PHRASES ===")
wp_dir = out_base / "words_phrases"

with open(wp_dir / "listing.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")

# Let's look at the JS to find the correct AJAX endpoints
for sc in soup.find_all("script"):
    content = sc.get_text()
    if "ajax" in content.lower() and ("word" in content.lower() or "phrase" in content.lower()):
        print(f"W&P script:\n{content[:2000]}")
        break

# Find the actual W&P content in the page
# Let's search for the main table
right_div = soup.find("div", id="rightmenu")
if right_div:
    tables = right_div.find_all("table")
    print(f"\nTables in rightmenu: {len(tables)}")
    for tbl in tables:
        rows = tbl.find_all("tr")
        print(f"  Table rows: {len(rows)}")
        for row in rows[:5]:
            tds = row.find_all("td")
            if tds:
                print(f"    {[td.get_text(strip=True)[:50] for td in tds]}")

# Try W&P via different parameters
print("\nTrying W&P endpoints...")
time.sleep(2)

# Try the char search like articles
resp = s.get(f"{B}/Login/WordsAndPhrases", params={"type": "words", "char": "A"}, timeout=15)
print(f"W&P char=A: {resp.status_code}, {len(resp.text)} chars, preview: {resp.text[:200]}")
time.sleep(1)

resp = s.post(f"{B}/Login/GetWordsAndPhrases", data={"char": "A"}, timeout=15)
print(f"POST GetWordsAndPhrases: {resp.status_code}, {len(resp.text)} chars")
time.sleep(1)

# Check the script for W&P AJAX
for sc in soup.find_all("script"):
    content = sc.get_text()
    if "ajax" in content.lower():
        import re
        urls = re.findall(r'url\s*:\s*["\']([^"\']+)["\']', content)
        for url in urls:
            print(f"  AJAX URL in W&P page: {url}")

# Look at the actual W&P content - it was 112KB so it must have data
print(f"\nW&P listing page size: {len(html)} chars")
all_text = soup.get_text(separator="\n", strip=True)
# Find lines that look like W&P entries (longer lines)
content_lines = [l for l in all_text.split("\n") if len(l) > 20 and l.strip()]
print(f"Content lines (>20 chars): {len(content_lines)}")
print("Sample lines:")
for line in content_lines[30:60]:
    print(f"  {line}")

# ============================================================
# MAXIMS - Proper extraction
# ============================================================
print("\n=== MAXIMS CONTENT ===")
with open(out_base / "maxims" / "maxims.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")

right_div = soup.find("div", id="rightmenu")
if right_div:
    # Find actual maxim content
    tables = right_div.find_all("table")
    print(f"Tables in maxims page: {len(tables)}")
    for tbl in tables[:2]:
        rows = tbl.find_all("tr")
        print(f"  Table rows: {len(rows)}")
        for row in rows[:10]:
            tds = row.find_all("td")
            if tds:
                print(f"    {[td.get_text(strip=True)[:60] for td in tds]}")
    
    # Also check for any content elements
    content_els = right_div.find_all(["p","div","li"])
    all_text = right_div.get_text(separator="\n", strip=True)
    content_lines = [l for l in all_text.split("\n") if len(l) > 20]
    print(f"  Content lines: {len(content_lines)}")
    for line in content_lines[:20]:
        print(f"  {line}")

# ============================================================
# LEGAL TERMS - Proper extraction
# ============================================================
print("\n=== LEGAL TERMS CONTENT ===")
with open(out_base / "legal_terms" / "legal_terms.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()
soup = BeautifulSoup(html, "html.parser")

right_div = soup.find("div", id="rightmenu")
if right_div:
    tables = right_div.find_all("table")
    print(f"Tables in legal terms page: {len(tables)}")
    for tbl in tables[:2]:
        rows = tbl.find_all("tr")
        print(f"  Table rows: {len(rows)}")
        for row in rows[:10]:
            tds = row.find_all("td")
            if tds:
                print(f"    {[td.get_text(strip=True)[:60] for td in tds]}")
    
    all_text = right_div.get_text(separator="\n", strip=True)
    content_lines = [l for l in all_text.split("\n") if len(l) > 20]
    print(f"  Content lines: {len(content_lines)}")
    for line in content_lines[:20]:
        print(f"  {line}")

print("\nDone!")
