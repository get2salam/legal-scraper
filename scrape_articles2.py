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

articles_dir = Path("data_v2/pls_extras/articles")
articles_dir.mkdir(parents=True, exist_ok=True)

# The JS reveals:
# - ArticlesCharSearch: filter by letter
# - ArticlesSearch: text search
# Let's use ArticlesCharSearch for each letter to find all articles

def parse_article_rows(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    # Find all tr with casetypeid
    for row in soup.find_all(["tr","div"], attrs={"casetypeid": True}):
        cid = row.get("casetypeid")
        tds = row.find_all("td")
        rows.append({
            "casetypeid": cid,
            "title": tds[1].get_text(strip=True) if len(tds) > 1 else "",
            "author": tds[2].get_text(strip=True) if len(tds) > 2 else "",
            "category": tds[3].get_text(strip=True) if len(tds) > 3 else "",
            "year": tds[4].get_text(strip=True) if len(tds) > 4 else "",
        })
    return rows

# First check what ArticlesCharSearch returns
print("Testing ArticlesCharSearch endpoint...")
resp = s.get(f"{B}/Login/ArticlesCharSearch", params={"text": "a"}, timeout=15)
print(f"Status: {resp.status_code}, Size: {len(resp.text)} chars")
print(f"Preview: {resp.text[:500]}")
time.sleep(1)

# Save test response
with open(articles_dir / "char_search_a_test.html", "w", encoding="utf-8") as f:
    f.write(resp.text)

# Parse and check
test_rows = parse_article_rows(resp.text)
print(f"Rows from 'a' char search: {len(test_rows)}")
for row in test_rows[:5]:
    print(f"  [{row['casetypeid']}] {row['title'][:60]} ({row['year']})")

print()
# If that works, collect all articles by letter
all_articles = {}
seen_ids = set()

for letter in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"):
    time.sleep(1.5)
    try:
        resp = s.get(f"{B}/Login/ArticlesCharSearch", params={"text": letter.lower()}, timeout=15)
        if resp.status_code == 200 and len(resp.text) > 100:
            rows = parse_article_rows(resp.text)
            new_rows = [row for row in rows if row["casetypeid"] not in seen_ids]
            if new_rows:
                for row in new_rows:
                    seen_ids.add(row["casetypeid"])
                all_articles[letter] = new_rows
                # Save raw HTML
                with open(articles_dir / f"char_{letter}.html", "w", encoding="utf-8") as f:
                    f.write(resp.text)
                print(f"  Letter {letter}: {len(new_rows)} new articles (total: {len(seen_ids)})")
            else:
                print(f"  Letter {letter}: 0 new articles ({len(rows)} total in response)")
        else:
            print(f"  Letter {letter}: status={resp.status_code}, size={len(resp.text)}")
    except Exception as e:
        print(f"  Letter {letter}: ERROR: {e}")

# Flatten all articles
all_articles_flat = []
for letter, arts in all_articles.items():
    all_articles_flat.extend(arts)

print(f"\nTotal unique articles found: {len(all_articles_flat)}")

# Save meta
with open(articles_dir / "all_articles_meta.json", "w", encoding="utf-8") as f:
    json.dump(all_articles_flat, f, ensure_ascii=False, indent=2)

# Now fetch full content for each article
print(f"\nFetching full content for {len(all_articles_flat)} articles...")
articles_full = []
errors = []

for i, art in enumerate(all_articles_flat):
    time.sleep(2)
    cid = art["casetypeid"]
    try:
        resp = s.post(f"{B}/Login/GetArticleFile", data={"caseName": cid}, timeout=20)
        if resp.status_code == 200 and resp.text.strip() != "1" and len(resp.text) > 100:
            asoup = BeautifulSoup(resp.text, "html.parser")
            body_text = asoup.get_text(separator="\n", strip=True)
            safe_id = cid.replace("/", "_").replace("\\", "_")
            with open(articles_dir / f"article_{safe_id}.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            articles_full.append({
                **art,
                "body": body_text[:20000],
                "body_length": len(body_text),
            })
            print(f"  [{i+1}/{len(all_articles_flat)}] {art['title'][:50]} ({len(body_text)} chars)")
        else:
            errors.append({"id": cid, "status": resp.status_code, "preview": resp.text[:100]})
            print(f"  [{i+1}] FAILED: {cid}")
    except Exception as e:
        errors.append({"id": cid, "error": str(e)})
        print(f"  [{i+1}] ERROR: {cid}: {e}")

print(f"\nFetched: {len(articles_full)}, Errors: {len(errors)}")

with open(articles_dir / "articles_full.json", "w", encoding="utf-8") as f:
    json.dump(articles_full, f, ensure_ascii=False, indent=2)
if errors:
    with open(articles_dir / "fetch_errors.json", "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

print("\nDone!")
