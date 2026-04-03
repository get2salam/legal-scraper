#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PHC Scraper â€” Peshawar High Court Judgment Scraper
Saves each case in 4 formats:
  1. PDF           â€” original judgment PDF
  2. JSON          â€” structured metadata
  3. HTML          â€” original listing HTML snapshot  (original_html/ subfolder)
  4. JSONL         â€” one line appended to {year}_PHC.jsonl

Directory layout:
  data_v2/court_cases/PHC/{year}/
  â”œâ”€â”€ case_name.pdf
  â”œâ”€â”€ case_name.json
  â”œâ”€â”€ original_html/
  â”‚   â””â”€â”€ case_name.html
  â””â”€â”€ {year}_PHC.jsonl
"""

import re
import json
import time
import logging
import requests
from pathlib import Path
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

# â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
BASE_DIR   = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "data_v2" / "court_cases" / "PHC"
LOG_FILE   = BASE_DIR / "phc_scraper_progress.log"

SEARCH_URL   = "https://peshawarhighcourt.gov.pk/PHCCMS/reportedJudgments.php?action=search"
BASE_PDF_URL = "https://peshawarhighcourt.gov.pk/"
DELAY_SECONDS = 1          # delay only after a real HTTP download
MAX_DOWNLOADS = 5000       # increased for full scrape

YEARS = list(range(2010, 2027))   # 2010 â€“ 2026

# â”€â”€ Logging â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# â”€â”€ HTTP session â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0 Safari/537.36"
    ),
    "Referer": "https://peshawarhighcourt.gov.pk/PHCCMS/reportedJudgments.php",
})


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def safe_filename(name: str, max_len: int = 180) -> str:
    """Sanitise string for use as a filename (Windows-safe)."""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r'\s+', " ", name).strip().strip(".")
    return name[:max_len]


def make_safe_name(judgment: dict) -> str:
    """Return the final PDF filename (with .pdf extension) for a judgment."""
    raw = judgment["pdf_filename"] or (safe_filename(judgment["case_number"]) + ".pdf")
    name = safe_filename(raw)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name


def extract_year_from_date(date_str: str) -> str:
    if not date_str:
        return "unknown"
    m = re.search(r'\b(20\d{2})\b', date_str)
    return m.group(1) if m else "unknown"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_meta(judgment: dict, safe_name: str, scraped_at: str) -> dict:
    """Build the metadata dict shared by JSON and JSONL outputs."""
    return {
        "case_number":    judgment["case_number"],
        "petitioner":     judgment["petitioner"],
        "respondent":     judgment["respondent"],
        "parties":        judgment["case_number"],
        "date":           judgment["decision_date"],
        "decision_date":  judgment["decision_date"],
        "year":           judgment["year"],
        "bench":          judgment["bench"],
        "category":       judgment["category"],
        "judges":         judgment["judges"],
        "court":          judgment["court"],
        "phc_citation":   judgment["phc_citation"],
        "other_citation": judgment["other_citation"],
        "sc_status":      judgment["sc_status"],
        "remarks":        judgment["remarks"],
        "pdf_filename":   safe_name,
        "source_url":     judgment["source_url"],
        "pdf_url":        judgment["pdf_url"],
        "scraped_at":     scraped_at,
    }


def write_json(meta: dict, json_path: Path) -> None:
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(meta, fh, ensure_ascii=False, indent=2)
    log.info(f"  âœ“ JSON {json_path.name}")


def write_html(judgment: dict, safe_name: str, scraped_at: str, html_path: Path) -> None:
    content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>PHC Judgment â€” {judgment['case_number']}</title>
  <meta name="source" content="{judgment['source_url']}">
  <meta name="scraped_at" content="{scraped_at}">
</head>
<body>
<h2>Peshawar High Court â€” Judgment Record</h2>
<table border="1" cellpadding="6" cellspacing="0">
  <tr><th>Case Number</th><td>{judgment['case_number']}</td></tr>
  <tr><th>Decision Date</th><td>{judgment['decision_date']}</td></tr>
  <tr><th>Category</th><td>{judgment['category']}</td></tr>
  <tr><th>PHC Citation</th><td>{judgment['phc_citation']}</td></tr>
  <tr><th>Other Citation</th><td>{judgment['other_citation']}</td></tr>
  <tr><th>SC Status</th><td>{judgment['sc_status']}</td></tr>
  <tr><th>Remarks / Headnotes</th><td>{judgment['remarks']}</td></tr>
  <tr><th>PDF</th><td><a href="{judgment['pdf_url']}">{safe_name}</a></td></tr>
</table>
<hr>
<h3>Original Table Row (from listing page)</h3>
<table border="1">{judgment['_row_html']}</table>
<hr>
<p><small>Scraped from: <a href="{judgment['source_url']}">{judgment['source_url']}</a>
at {scraped_at}</small></p>
</body>
</html>
"""
    with open(html_path, "w", encoding="utf-8") as fh:
        fh.write(content)
    log.info(f"  âœ“ HTML {html_path.name}")


def append_jsonl(meta: dict, jsonl_path: Path) -> None:
    with open(jsonl_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(meta, ensure_ascii=False) + "\n")
    log.info(f"  âœ“ JSONL {jsonl_path.name}")


# â”€â”€ Step 1: fetch judgment listing for one year â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def fetch_judgments_for_year(year: int) -> list:
    """POST to search endpoint for *year*. Returns list of judgment dicts."""
    log.info(f"Fetching judgment list for year {year}â€¦")
    try:
        resp = session.post(
            SEARCH_URL,
            data={
                "year":               str(year),
                "judge":              "0",
                "category":           "0",
                "txtsearchbyremarks": "",
                "submit":             "search",
            },
            timeout=60,
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"  Failed to fetch year {year}: {e}")
        return []

    soup  = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", {"id": "employee_list"})
    if not table:
        log.warning(f"  No results table for year {year}")
        return []

    tbody = table.find("tbody")
    rows  = tbody.find_all("tr") if tbody else []
    judgments = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 9:
            continue

        sno            = cells[0].get_text(strip=True)
        case_raw       = cells[1].get_text(strip=True)
        remarks        = cells[2].get_text(strip=True)
        other_citation = cells[3].get_text(strip=True)
        phc_citation   = cells[4].get_text(strip=True)
        decision_date  = cells[5].get_text(strip=True)
        sc_status      = cells[6].get_text(strip=True)
        category       = cells[7].get_text(strip=True)
        row_html       = str(row)   # raw HTML of this table row

        # --- PDF URL ---
        pdf_link = cells[8].find("a")
        if not pdf_link:
            continue
        pdf_href = pdf_link.get("href", "").replace("PHCCMS//", "PHCCMS/")
        if not pdf_href.startswith("http"):
            pdf_href = urljoin(BASE_PDF_URL, pdf_href)
        pdf_filename = unquote(pdf_href.split("/")[-1])

        # --- split parties ---
        parts = re.split(r'\bVs?\b', case_raw, maxsplit=1, flags=re.IGNORECASE)
        petitioner = parts[0].strip() if parts else case_raw
        respondent = parts[1].strip() if len(parts) == 2 else ""

        # --- rough judge extraction from remarks/citation ---
        judge_hits = re.findall(
            r'(?:Justice|J\.)\s+[A-Z][a-zA-Z .\'-]+',
            remarks + " " + phc_citation,
        )
        judges = list(dict.fromkeys(j.strip() for j in judge_hits))

        # --- determine storage year ---
        doc_year = extract_year_from_date(decision_date)
        if doc_year == "unknown":
            m = re.search(r'\b(20\d{2})\b', phc_citation)
            doc_year = m.group(1) if m else str(year)

        judgments.append({
            "sno":            sno,
            "case_number":    case_raw,
            "petitioner":     petitioner,
            "respondent":     respondent,
            "judges":         judges,
            "bench":          category,
            "category":       category,
            "phc_citation":   phc_citation,
            "other_citation": other_citation,
            "decision_date":  decision_date,
            "year":           doc_year,
            "search_year":    str(year),
            "sc_status":      sc_status,
            "remarks":        remarks,
            "pdf_url":        pdf_href,
            "pdf_filename":   pdf_filename,
            "court":          "Peshawar High Court",
            "source_url":     SEARCH_URL,
            "_row_html":      row_html,   # internal only â€“ not stored in index
        })

    log.info(f"  Found {len(judgments)} judgments for year {year}")
    return judgments


# â”€â”€ Step 2: save one case in all 4 formats â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def save_case(judgment: dict, idx: int, total: int, dl_count: int) -> str:
    """
    Download PDF and write JSON + HTML + JSONL.
    Returns: 'new' | 'skip' | 'fail'
    """
    year      = judgment["year"]
    year_dir  = OUTPUT_DIR / year
    html_dir  = year_dir / "original_html"
    year_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)

    safe_name  = make_safe_name(judgment)
    base_stem  = Path(safe_name).stem

    pdf_path   = year_dir / safe_name
    json_path  = year_dir / f"{base_stem}.json"
    html_path  = html_dir / f"{base_stem}.html"
    jsonl_path = year_dir / f"{year}_PHC.jsonl"

    scraped_at = now_utc()
    meta       = build_meta(judgment, safe_name, scraped_at)

    # â”€â”€ always write JSON / HTML / JSONL (idempotent) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not json_path.exists():
        write_json(meta, json_path)
    if not html_path.exists():
        write_html(judgment, safe_name, scraped_at, html_path)
    # JSONL: only append if this case isn't already in the file
    already_in_jsonl = False
    if jsonl_path.exists():
        search_key = judgment["pdf_url"]
        with open(jsonl_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if search_key in line:
                    already_in_jsonl = True
                    break
    if not already_in_jsonl:
        append_jsonl(meta, jsonl_path)

    # â”€â”€ PDF: skip if already on disk â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if pdf_path.exists():
        log.info(f"  [SKIP] {safe_name} (PDF exists)")
        return "skip"

    log.info(f"[{idx}/{total}] dl#{dl_count+1}  {judgment['case_number'][:80]}")

    try:
        resp = session.get(judgment["pdf_url"], timeout=30, stream=True)
        if resp.status_code == 404:
            log.warning(f"  404: {judgment['pdf_url']}")
            return "fail"
        resp.raise_for_status()
        with open(pdf_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)
        log.info(f"  âœ“ PDF  {safe_name} ({pdf_path.stat().st_size:,} bytes)")
        return "new"
    except requests.RequestException as e:
        log.error(f"  PDF download failed: {e}")
        if pdf_path.exists():
            pdf_path.unlink()
        return "fail"


# â”€â”€ Main â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    log.info("=" * 70)
    log.info("PHC Judgment Scraper  â€”  4-format output  (PDF + JSON + HTML + JSONL)")
    log.info(f"Output  : {OUTPUT_DIR}")
    log.info(f"Max DL  : {MAX_DOWNLOADS}")
    log.info("=" * 70)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # â”€â”€ Phase 1: collect full index (all years) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    all_judgments: list = []
    for year in YEARS:
        all_judgments.extend(fetch_judgments_for_year(year))
        time.sleep(DELAY_SECONDS)

    total = len(all_judgments)
    log.info(f"\nTotal judgments catalogued: {total}")

    # Save lean master index (no _row_html)
    index_path = OUTPUT_DIR / "phc_index.json"
    lean = [{k: v for k, v in j.items() if k != "_row_html"} for j in all_judgments]
    with open(index_path, "w", encoding="utf-8") as fh:
        json.dump(lean, fh, ensure_ascii=False, indent=2)
    log.info(f"Master index â†’ {index_path}  ({total} entries)")

    # â”€â”€ Phase 2: process every case â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    downloaded = skipped = failed = 0

    for idx, judgment in enumerate(all_judgments, 1):
        if downloaded >= MAX_DOWNLOADS:
            log.info(f"Reached MAX_DOWNLOADS ({MAX_DOWNLOADS}). Stopping.")
            break

        result = save_case(judgment, idx, total, downloaded)

        if result == "new":
            downloaded += 1
            time.sleep(DELAY_SECONDS)   # polite delay only after real HTTP download
        elif result == "skip":
            skipped += 1
            # no delay â€” already on disk
        else:
            failed += 1
            time.sleep(1)               # brief pause after errors

    log.info("\n" + "=" * 70)
    log.info("SCRAPE COMPLETE")
    log.info(f"  Total catalogued : {total}")
    log.info(f"  New downloads    : {downloaded}")
    log.info(f"  Already existed  : {skipped}")
    log.info(f"  Failed           : {failed}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
