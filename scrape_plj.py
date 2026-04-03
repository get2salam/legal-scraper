#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PLJ Case Scraper for LawGPT API
Fetches PLJ (Pakistan Law Journal) cases from LawGPT's public search API.
Saves in 4 formats: JSON, readable HTML, original HTML, JSONL.

Strategy:
- API returns ~1 PLJ case per 10 results (cases have multiple reporters)
- Use diverse queries (year + court + topic combos) to get varied result sets
- Track seen doc_ids globally to deduplicate
- Extract only cases where Section contains 'PLJ'
- Max 100 cases per run (safety cap)
"""

import os
import sys
import json
import re
import time
import random
import logging
from datetime import datetime
from pathlib import Path

import requests

# ── Config ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data_v2" / "PLJ"
PROGRESS_FILE = DATA_DIR / "progress.json"
LOG_FILE = BASE_DIR.parent.parent.parent / "AppData" / "Roaming" / "npm" / "node_modules"
# Use workspace memory dir for logs
LOG_DIR = Path("C:/Users/gempo/.openclaw/workspace/memory")

API_URL = "https://prod-search-engine.azurewebsites.net/api/search/lawcases"
MAX_CASES_PER_RUN = 100
REQUEST_TIMEOUT = 45

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("plj_scraper")

# ── User Agents (rotate for stealth) ────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
]

# ── Query Strategy ────────────────────────────────────────────────────────────
# Wide variety of queries to maximise unique case discovery
# Each query fetches 10 results, ~1 PLJ per 10 -> need many queries

COURTS = [
    "Lahore High Court",
    "Supreme Court",
    "Sindh High Court",
    "Islamabad High Court",
    "Peshawar High Court",
    "Balochistan High Court",
    "Federal Shariat Court",
]

TOPICS = [
    "contract", "property", "criminal", "bail", "appeal", "divorce", "custody",
    "murder", "fraud", "tax", "service", "constitutional", "election", "land",
    "rent", "arbitration", "injunction", "revision", "succession", "insurance",
    "banking", "corruption", "negligence", "writ", "habeas corpus",
    "maintenance", "boundary", "registration", "inheritance", "civil",
    "sentence", "acquittal", "conviction", "evidence", "witnesses",
    "cheque", "promissory note", "mortgage", "specific performance",
]

def build_query_list():
    """Build a large diverse list of queries to cycle through."""
    queries = []
    
    # Year-based queries (primary strategy)
    for year in list(range(2026, 1999, -1)) + list(range(1999, 1969, -1)):
        queries.append(str(year))
        # Year + court
        for court in COURTS:
            queries.append(f"{year} {court}")
    
    # Topic queries
    for topic in TOPICS:
        for year in range(2024, 2018, -1):
            queries.append(f"{topic} {year}")
    
    # Court-only queries
    for court in COURTS:
        queries.append(court)
    
    # PLJ citation style queries
    for year in range(2025, 1999, -1):
        queries.append(f"PLJ {year} Lahore")
        queries.append(f"PLJ {year} Karachi")
        queries.append(f"PLJ {year} Islamabad")
        queries.append(f"PLJ {year} Peshawar")
        queries.append(f"PLJ {year} Quetta")
    
    return queries


# ── Helper Functions ──────────────────────────────────────────────────────────

def extract_year_from_date(date_str):
    """Extract year from ISO date string like '2024-10-01T00:00:00.000Z'."""
    if date_str and len(date_str) >= 4:
        return date_str[:4]
    return None


def extract_year_from_citation(citation_list):
    """Extract year from Citation_Name list like ['PLJ 2023 Lahore 719']."""
    if not citation_list:
        return None
    for cit in citation_list:
        if "PLJ" in cit:
            m = re.search(r"PLJ\s+(\d{4})", cit)
            if m:
                return m.group(1)
    # fallback: any 4-digit year
    for cit in citation_list:
        m = re.search(r"\b(19\d{2}|20\d{2})\b", cit)
        if m:
            return m.group(1)
    return None


def make_citation_slug(title, doc_id):
    """Create a filesystem-safe slug from title + doc_id."""
    # Sanitize title
    slug = re.sub(r"[^\w\s-]", "", title[:50]).strip()
    slug = re.sub(r"\s+", "_", slug)
    slug = re.sub(r"_+", "_", slug)
    # Use last 8 chars of doc_id
    doc_suffix = re.sub(r"[^a-zA-Z0-9]", "", doc_id)[-8:]
    return f"{slug}_{doc_suffix}"


def extract_court(title, api_court=None):
    """Extract court name from title or API Court field."""
    if api_court:
        # Map API court names to our standard
        court_map = {
            "Lahore High Court": "Lahore High Court",
            "Supreme Court of Pakistan": "Supreme Court",
            "Sindh High Court": "Sindh High Court",
            "Islamabad High Court": "Islamabad High Court",
            "Peshawar High Court": "Peshawar High Court",
            "Balochistan High Court": "Balochistan High Court",
            "Federal Shariat Court": "Federal Shariat Court",
        }
        for k, v in court_map.items():
            if k.lower() in api_court.lower():
                return v
        return api_court

    # Fallback: parse from title
    title_lower = (title or "").lower()
    courts = [
        ("supreme court", "Supreme Court"),
        ("lahore high court", "Lahore High Court"),
        ("sindh high court", "Sindh High Court"),
        ("islamabad high court", "Islamabad High Court"),
        ("peshawar high court", "Peshawar High Court"),
        ("balochistan high court", "Balochistan High Court"),
        ("federal shariat court", "Federal Shariat Court"),
    ]
    for key, name in courts:
        if key in title_lower:
            return name
    return ""


def extract_judges(api_item):
    """Extract judge names from API response."""
    judge_name = api_item.get("Judge_Name") or ""
    if isinstance(judge_name, list):
        return ", ".join(judge_name)
    return str(judge_name).strip()


def get_plj_citation(citation_list):
    """Extract the PLJ citation string from Citation_Name list."""
    if not citation_list:
        return ""
    for cit in citation_list:
        if "PLJ" in str(cit):
            return str(cit).strip()
    return ""


def make_readable_html(case_data):
    """Generate clean readable HTML for a case."""
    judgment_html = (case_data.get("judgment") or "").replace("\n", "<br>")
    # Escape < and > that aren't part of HTML tags
    title_safe = (case_data.get("title") or "").replace("<", "&lt;").replace(">", "&gt;")
    citation_safe = (case_data.get("citation") or "")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{citation_safe}</title>
  <style>
    body {{ font-family: Georgia, serif; max-width: 900px; margin: 2em auto; padding: 1em; line-height: 1.6; color: #333; }}
    h1 {{ font-size: 1.4em; color: #1a1a6e; border-bottom: 2px solid #1a1a6e; padding-bottom: 0.3em; }}
    h2 {{ font-size: 1.1em; color: #333; }}
    .meta {{ background: #f5f5f5; padding: 1em; border-radius: 4px; margin: 1em 0; }}
    .judgment {{ margin-top: 1.5em; text-align: justify; }}
    .result {{ font-weight: bold; color: #2e7d32; }}
  </style>
</head>
<body>
  <h1>{citation_safe}</h1>
  <h2>{title_safe}</h2>
  <div class="meta">
    <p><strong>Court:</strong> {case_data.get("court", "")}</p>
    <p><strong>Date:</strong> {case_data.get("date", "")}</p>
    <p><strong>Judges:</strong> {case_data.get("judges", "")}</p>
    <p><strong>Result:</strong> <span class="result">{case_data.get("result", "")}</span></p>
    <p><strong>Source:</strong> {case_data.get("source", "")} | <strong>Case ID:</strong> {case_data.get("case_id", "")}</p>
  </div>
  <hr>
  <div class="judgment">
    {judgment_html}
  </div>
</body>
</html>"""


def make_original_html(api_item):
    """Save original raw API response as HTML-like document."""
    title = api_item.get("Title", "")
    doc_id = api_item.get("doc_id", "")
    raw_json = json.dumps(api_item, ensure_ascii=False, indent=2)
    raw_html = api_item.get("Case_Description_HTML") or api_item.get("Case_Description") or ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Raw: {title[:80]}</title>
</head>
<body>
<h1>Original API Response</h1>
<h2>doc_id: {doc_id}</h2>
<h3>Title: {title[:100]}</h3>
<h3>Raw Content:</h3>
<div id="raw-html">
{raw_html}
</div>
<hr>
<h3>Full API JSON:</h3>
<pre id="raw-json" style="white-space:pre-wrap;font-size:0.8em;background:#f0f0f0;padding:1em">
{raw_json}
</pre>
</body>
</html>"""


# ── Progress Tracking ─────────────────────────────────────────────────────────

def load_progress():
    """Load scraping progress from disk."""
    if PROGRESS_FILE.exists():
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"Could not load progress: {e}")
    return {
        "seen_doc_ids": [],
        "query_index": 0,
        "total_saved": 0,
        "runs": [],
        "created_at": datetime.now().isoformat(),
    }


def save_progress(progress):
    """Save progress to disk."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def is_already_saved(citation_slug, year):
    """Check if a case JSON file already exists."""
    json_path = DATA_DIR / str(year) / f"{citation_slug}.json"
    return json_path.exists()


# ── Save Functions ────────────────────────────────────────────────────────────

def save_case(case_data, api_item, citation_slug, year):
    """Save case in all 4 formats."""
    year_dir = DATA_DIR / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)

    # 1. JSON (full metadata + text)
    json_path = year_dir / f"{citation_slug}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(case_data, f, ensure_ascii=False, indent=2)

    # 2. Readable HTML
    readable_path = year_dir / f"{citation_slug}_readable.html"
    with open(readable_path, "w", encoding="utf-8") as f:
        f.write(make_readable_html(case_data))

    # 3. Original HTML (raw API response)
    original_path = year_dir / f"{citation_slug}_original.html"
    with open(original_path, "w", encoding="utf-8") as f:
        f.write(make_original_html(api_item))

    # 4. Append to JSONL
    jsonl_path = DATA_DIR / str(year) / f"PLJ_{year}.jsonl"
    with open(jsonl_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(case_data, ensure_ascii=False) + "\n")

    log.info(f"  ✓ Saved: {citation_slug} ({year})")
    return True


# ── API Call ──────────────────────────────────────────────────────────────────

def search_cases(query, page=1, page_size=10):
    """Call the LawGPT search API."""
    headers = {
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://lawgpt.pk",
        "Referer": "https://lawgpt.pk/",
    }
    payload = {
        "query": query,
        "mode": "keyword",
        "page": page,
        "pageSize": page_size,
    }
    try:
        r = requests.post(API_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        return data.get("value", [])
    except requests.exceptions.RequestException as e:
        log.error(f"  API error for query '{query}': {e}")
        return []
    except Exception as e:
        log.error(f"  Unexpected error for query '{query}': {e}")
        return []


# ── Main Scraper ──────────────────────────────────────────────────────────────

def process_case(api_item, seen_doc_ids):
    """
    Process a single API result item.
    Returns (case_data, citation_slug, year) or None if skip.
    """
    # Check if PLJ case
    sections = api_item.get("Section") or []
    if not isinstance(sections, list):
        sections = [sections] if sections else []
    if "PLJ" not in sections:
        return None

    doc_id = api_item.get("doc_id", "")
    if not doc_id:
        return None

    # Dedup check
    if doc_id in seen_doc_ids:
        return None

    title = api_item.get("Title") or ""
    citation_list = api_item.get("Citation_Name") or []
    date_str = api_item.get("Date_Of_Judgement") or ""
    result = api_item.get("Result") or ""
    case_description = api_item.get("Case_Description") or ""
    api_court = api_item.get("Court") or ""
    api_year = api_item.get("Year")

    # Determine year
    year = extract_year_from_citation(citation_list)
    if not year:
        year = extract_year_from_date(date_str)
    if not year and api_year:
        year = str(api_year)
    if not year:
        year = "unknown"

    # Build citation
    plj_citation = get_plj_citation(citation_list)
    if not plj_citation:
        plj_citation = f"PLJ {year}"
    citation_str = plj_citation

    # Build slug
    citation_slug = make_citation_slug(title, doc_id)

    # Skip if already saved
    if is_already_saved(citation_slug, year):
        seen_doc_ids.add(doc_id)  # still mark as seen
        return None

    # Build case_data
    case_data = {
        "citation": citation_str,
        "reporter": "PLJ",
        "year": year,
        "title": title,
        "judgment": case_description,
        "judgment_raw": case_description,
        "result": result,
        "date": date_str,
        "court": extract_court(title, api_court),
        "judges": extract_judges(api_item),
        "case_id": doc_id,
        "source": "lawgpt_api",
        "scraped_at": datetime.now().isoformat(),
        # Extra fields from API
        "all_citations": citation_list,
        "all_sections": sections,
        "short_summary": api_item.get("Short_Summary") or "",
        "lawyer_name": api_item.get("Lawyer_Name") or "",
    }

    return case_data, citation_slug, year


def run_scraper():
    """Main scraper loop."""
    log.info("=" * 60)
    log.info("PLJ Scraper starting...")
    log.info(f"Max cases per run: {MAX_CASES_PER_RUN}")

    # Load progress
    progress = load_progress()
    seen_doc_ids = set(progress.get("seen_doc_ids", []))
    query_index = progress.get("query_index", 0)
    total_saved_ever = progress.get("total_saved", 0)

    log.info(f"Resuming from query index {query_index} | Total saved so far: {total_saved_ever}")

    # Build query list
    all_queries = build_query_list()
    log.info(f"Total queries in list: {len(all_queries)}")

    saved_this_run = 0
    queries_tried = 0

    while saved_this_run < MAX_CASES_PER_RUN:
        if query_index >= len(all_queries):
            log.info("All queries exhausted. Restarting from beginning.")
            query_index = 0

        query = all_queries[query_index]
        query_index += 1
        queries_tried += 1

        # Random page size for stealth
        page_size = random.randint(5, 15)

        log.info(f"[{queries_tried}] Query: '{query}' | pageSize={page_size}")

        results = search_cases(query, page=1, page_size=page_size)

        if not results:
            log.warning(f"  No results for: '{query}'")
        else:
            for api_item in results:
                processed = process_case(api_item, seen_doc_ids)
                if processed is None:
                    continue

                case_data, citation_slug, year = processed
                doc_id = api_item.get("doc_id", "")

                # Save it
                try:
                    save_case(case_data, api_item, citation_slug, year)
                    seen_doc_ids.add(doc_id)
                    saved_this_run += 1
                    total_saved_ever += 1

                    if saved_this_run >= MAX_CASES_PER_RUN:
                        log.info(f"Reached max {MAX_CASES_PER_RUN} cases for this run.")
                        break
                except Exception as e:
                    log.error(f"  Error saving {citation_slug}: {e}")

        # Save progress after each query
        progress["seen_doc_ids"] = list(seen_doc_ids)
        progress["query_index"] = query_index
        progress["total_saved"] = total_saved_ever
        save_progress(progress)

        if saved_this_run >= MAX_CASES_PER_RUN:
            break

        # Stealth delay: 3-8 seconds
        delay = random.uniform(3.0, 8.0)
        log.info(f"  Sleeping {delay:.1f}s...")
        time.sleep(delay)

    # Final progress save with run record
    progress["runs"].append({
        "run_at": datetime.now().isoformat(),
        "saved_this_run": saved_this_run,
        "queries_tried": queries_tried,
        "total_saved": total_saved_ever,
    })
    save_progress(progress)

    log.info("=" * 60)
    log.info(f"Run complete! Saved: {saved_this_run} | Total ever: {total_saved_ever}")
    log.info(f"Unique doc_ids seen: {len(seen_doc_ids)}")
    return saved_this_run


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    run_scraper()
