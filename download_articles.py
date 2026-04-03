import os, sys, time, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
from curl_cffi import requests as r
from bs4 import BeautifulSoup

s = r.Session(); s.impersonate = "chrome"
B = "https://www.pakistanlawsite.com"
u = os.getenv("PLS_USER",""); p = os.getenv("PLS_PASS","")
s.post(f"{B}/Login/ClearLoginHistory", data={"Login.UserName":u,"Login.Password":p}, timeout=30)
time.sleep(3)

base = Path("data_v2/pls_extras/articles")
articles = json.load(open(base / "articles_all.json", encoding="utf-8"))
print(f"Total articles: {len(articles)}")

# Extract full text from already-downloaded HTML pages
articles_full = []
seen_ids = set()
html_pages = list(base.glob("article_*.html"))
print(f"HTML pages available: {len(html_pages)}")

for html_file in html_pages:
    cid = html_file.stem.replace("article_", "")
    if cid in seen_ids: continue
    seen_ids.add(cid)
    try:
        html = html_file.read_text(encoding="utf-8", errors="replace")
        soup = BeautifulSoup(html, "html.parser")
        # Remove nav/header junk
        for tag in soup.find_all(["nav","header","footer","script","style"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        # Find matching article meta
        meta = next((a for a in articles if a.get("casetypeid","") == cid), {})
        if len(text) > 500:
            articles_full.append({
                "id": cid,
                "title": meta.get("title",""),
                "author": meta.get("author",""),
                "year": meta.get("year",""),
                "body": text[:30000]
            })
    except Exception as e:
        print(f"  Error {cid}: {e}")

print(f"Extracted full text from HTML: {len(articles_full)}")

# For remaining articles, fetch via GetCaseFile
all_ids = set(a.get("casetypeid","") for a in articles)
fetched_ids = seen_ids.copy()
remaining = [a for a in articles if a.get("casetypeid","") not in fetched_ids and a.get("casetypeid","")]
# Deduplicate
seen2 = set()
unique_remaining = []
for a in remaining:
    if a["casetypeid"] not in seen2:
        seen2.add(a["casetypeid"])
        unique_remaining.append(a)

print(f"Remaining to fetch via API: {len(unique_remaining)}")

fetched = 0
for art in unique_remaining[:200]:  # cap at 200 per run
    cid = art["casetypeid"]
    time.sleep(1.5)
    try:
        resp = s.post(f"{B}/Login/GetCaseFile", data={"caseName": cid, "headNotes": 0}, timeout=20)
        try:
            text = json.loads(resp.text)
        except:
            text = resp.text
        if isinstance(text, str) and len(text) > 200 and text.strip() != "1":
            articles_full.append({
                "id": cid,
                "title": art.get("title",""),
                "author": art.get("author",""),
                "year": art.get("year",""),
                "body": text[:30000]
            })
            fetched += 1
            if fetched % 20 == 0:
                print(f"  Fetched {fetched}... ({art.get('title','')[:40]})")
    except Exception as e:
        pass

print(f"Total articles with full text: {len(articles_full)}")
with open(base / "articles_full.json", "w", encoding="utf-8") as f:
    json.dump(articles_full, f, ensure_ascii=False, indent=2)
print("Saved to articles_full.json")
