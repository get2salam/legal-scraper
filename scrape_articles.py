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

# 1. Extract all article rows from the listing HTML we already have
with open(articles_dir / "listing.html", "r", encoding="utf-8", errors="replace") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

# Find the article table rows with casetypeid
articles_meta = []
table = soup.find("table", id="articleSearchTable")
if table:
    rows = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")
    for row in rows:
        cid = row.get("casetypeid")
        if not cid:
            # check buttons inside
            btn = row.find(attrs={"casetypeid": True})
            cid = btn.get("casetypeid") if btn else None
        if cid:
            tds = row.find_all("td")
            articles_meta.append({
                "casetypeid": cid,
                "num": tds[0].get_text(strip=True) if len(tds) > 0 else "",
                "title": tds[1].get_text(strip=True) if len(tds) > 1 else "",
                "author": tds[2].get_text(strip=True) if len(tds) > 2 else "",
                "category": tds[3].get_text(strip=True) if len(tds) > 3 else "",
                "year": tds[4].get_text(strip=True) if len(tds) > 4 else "",
            })

print(f"Articles in table: {len(articles_meta)}")
for a in articles_meta[:5]:
    print(f"  [{a['casetypeid']}] {a['title'][:60]} by {a['author'][:30]}")

# Also check if there are more articles via alphabet filter
# The page shows results for year 2026 by default; let's also try fetching by year/letter
# First, look for the search year input - it defaults to 2026
# Let's also try getting article list for all years

# Save meta
with open(articles_dir / "articles_meta.json", "w", encoding="utf-8") as f:
    json.dump(articles_meta, f, ensure_ascii=False, indent=2)

# 2. Fetch full content for each article via POST /Login/GetArticleFile
print(f"\nFetching {len(articles_meta)} article files...")
articles_full = []
errors = []

for i, art in enumerate(articles_meta):
    time.sleep(2)
    try:
        resp = s.post(f"{B}/Login/GetArticleFile", data={"caseName": art["casetypeid"]}, timeout=20)
        if resp.status_code == 200 and resp.text != "1":
            # Parse the HTML response
            asoup = BeautifulSoup(resp.text, "html.parser")
            body_text = asoup.get_text(separator="\n", strip=True)
            
            # Save raw HTML
            safe_id = art["casetypeid"].replace("/", "_")
            with open(articles_dir / f"article_{safe_id}.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            
            articles_full.append({
                **art,
                "body": body_text[:20000],
                "body_length": len(body_text),
            })
            print(f"  [{i+1}/{len(articles_meta)}] {art['title'][:50]} ({len(body_text)} chars)")
        else:
            errors.append({"id": art["casetypeid"], "error": "returned 1 or non-200", "status": resp.status_code})
            print(f"  [{i+1}] FAILED: {art['casetypeid']} (status={resp.status_code}, text={resp.text[:50]})")
    except Exception as e:
        errors.append({"id": art["casetypeid"], "error": str(e)})
        print(f"  [{i+1}] ERROR: {art['casetypeid']}: {e}")

print(f"\nFetched: {len(articles_full)}, Errors: {len(errors)}")
with open(articles_dir / "articles_full.json", "w", encoding="utf-8") as f:
    json.dump(articles_full, f, ensure_ascii=False, indent=2)
if errors:
    with open(articles_dir / "errors.json", "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

# 3. Also try fetching articles for other years by posting different year search
# Check the year search mechanism
print("\nTrying year-based searches...")
all_years_articles = []

# The default page shows 2026; let's try 2023, 2024, 2025
for year in [2025, 2024, 2023, 2022, 2021, 2020]:
    time.sleep(2)
    try:
        # Try GET with year param
        resp = s.get(f"{B}/Login/ArticlePage", params={"year": year}, timeout=20)
        ysoup = BeautifulSoup(resp.text, "html.parser")
        table = ysoup.find("table", id="articleSearchTable")
        if table:
            rows_y = table.find("tbody").find_all("tr") if table.find("tbody") else table.find_all("tr")
            year_articles = []
            for row in rows_y:
                cid = row.get("casetypeid")
                if not cid:
                    btn = row.find(attrs={"casetypeid": True})
                    cid = btn.get("casetypeid") if btn else None
                if cid:
                    tds = row.find_all("td")
                    year_articles.append({
                        "casetypeid": cid,
                        "title": tds[1].get_text(strip=True) if len(tds) > 1 else "",
                        "author": tds[2].get_text(strip=True) if len(tds) > 2 else "",
                        "year": str(year),
                    })
            if year_articles:
                print(f"  Year {year}: {len(year_articles)} articles")
                all_years_articles.extend(year_articles)
                # Save year HTML
                with open(articles_dir / f"year_{year}.html", "w", encoding="utf-8") as f:
                    f.write(resp.text)
            else:
                print(f"  Year {year}: 0 articles in table (page size: {len(resp.text)})")
    except Exception as e:
        print(f"  Year {year} error: {e}")

if all_years_articles:
    with open(articles_dir / "all_years_meta.json", "w", encoding="utf-8") as f:
        json.dump(all_years_articles, f, ensure_ascii=False, indent=2)
    print(f"\nTotal multi-year articles found: {len(all_years_articles)}")

print("\nArticles scraping done!")
