"""
PLJ Scraper v2 — Citation-based strategy
Instead of generic searches, query specific PLJ citations directly.
PLJ uses format: YEAR PLJ COURT PAGE
e.g. "2024 PLJ 1", "2024 PLJ Lahore 1", "2023 PLJ SC 100"

Strategy: iterate page numbers per year per court prefix.
"""
import sys, os, json, time, re, random, logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env.lawgpt'))
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

from curl_cffi import requests as r

SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'
DATA_DIR = Path('data_v2/PLJ')
MAX_PER_RUN = 200
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
]

def make_session():
    s = r.Session()
    s.impersonate = 'chrome'
    return s

def load_progress():
    prog_file = DATA_DIR / 'progress_v2.json'
    if prog_file.exists():
        return json.loads(prog_file.read_text(encoding='utf-8'))
    return {'saved_ids': [], 'total_saved': 0, 'queries_done': [], 'last_run': None}

def save_progress(prog):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / 'progress_v2.json').write_text(
        json.dumps(prog, ensure_ascii=False, indent=2), encoding='utf-8')

def extract_court(title):
    courts = [
        ('Supreme Court', ['Supreme Court', 'SC', 'SCR']),
        ('Lahore High Court', ['Lahore', 'LHC']),
        ('Sindh High Court', ['Sindh', 'SHC', 'Karachi']),
        ('Islamabad High Court', ['Islamabad', 'IHC']),
        ('Peshawar High Court', ['Peshawar', 'PHC']),
        ('Balochistan High Court', ['Balochistan', 'BHC', 'Quetta']),
        ('Federal Shariat Court', ['Federal Shariat', 'FSC']),
    ]
    for court_name, keywords in courts:
        for kw in keywords:
            if kw.lower() in title.lower():
                return court_name
    return ''

def save_case(case_data, prog):
    year = str(case_data.get('year', 'unknown'))
    doc_id = case_data.get('case_id', '')
    
    if doc_id in prog['saved_ids']:
        return False
    
    year_dir = DATA_DIR / year
    year_dir.mkdir(parents=True, exist_ok=True)
    
    # Slug from title + id
    title = case_data.get('title', 'unknown')[:40]
    slug = re.sub(r'[^\w]', '_', title).strip('_')
    slug = f"{slug}_{doc_id[-8:]}"
    
    # 1. JSON
    json_path = year_dir / f"{slug}.json"
    json_path.write_text(json.dumps(case_data, ensure_ascii=False, indent=2), encoding='utf-8')
    
    # 2. Readable HTML
    judgment = case_data.get('judgment', '').replace('\n', '<br>')
    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>{case_data.get('citation', '')}</title></head>
<body>
<h1>{case_data.get('citation', '')}</h1>
<h2>{case_data.get('title', '')}</h2>
<p><strong>Court:</strong> {case_data.get('court', '')}</p>
<p><strong>Date:</strong> {case_data.get('date', '')}</p>
<p><strong>Result:</strong> {case_data.get('result', '')}</p>
<hr><div>{judgment}</div>
</body></html>"""
    (year_dir / f"{slug}_readable.html").write_text(html, encoding='utf-8')
    
    # 3. Original HTML (same as readable for API source)
    (year_dir / f"{slug}_original.html").write_text(html, encoding='utf-8')
    
    # 4. JSONL
    jsonl_path = year_dir / f"PLJ_{year}.jsonl"
    with open(jsonl_path, 'a', encoding='utf-8') as f:
        f.write(json.dumps(case_data, ensure_ascii=False) + '\n')
    
    prog['saved_ids'].append(doc_id)
    prog['total_saved'] += 1
    return True

def search_citation(s_session, query):
    """Search for a specific citation and return PLJ matches."""
    try:
        resp = s_session.post(SEARCH,
            json={'query': query, 'mode': 'keyword', 'page': 1, 'pageSize': 10},
            headers={'Content-Type': 'application/json',
                     'User-Agent': random.choice(USER_AGENTS)},
            timeout=20)
        if resp.status_code != 200:
            return []
        data = resp.json()
        results = []
        for item in data.get('value', []):
            if 'PLJ' not in item.get('Section', []):
                continue
            title = item.get('Title', '')
            date = item.get('Date_Of_Judgement', '')
            year = date[:4] if date else ''
            if not year:
                # Try to extract from title or description
                m = re.search(r'\b(19|20)\d{2}\b', title + item.get('Case_Description','')[:100])
                year = m.group(0) if m else 'unknown'
            case_data = {
                'citation': f"{year} PLJ {item.get('Appeal', '') or title[:30]}",
                'reporter': 'PLJ',
                'year': year,
                'title': title,
                'judgment': item.get('Case_Description', ''),
                'judgment_raw': item.get('Case_Description', ''),
                'result': item.get('Result', ''),
                'date': date,
                'court': extract_court(title),
                'case_id': item.get('doc_id', ''),
                'source': 'lawgpt_api',
                'scraped_at': datetime.now().isoformat(),
            }
            results.append(case_data)
        return results
    except Exception as e:
        log.error(f"Search error for '{query}': {e}")
        return []

def generate_queries():
    """Generate targeted PLJ citation queries."""
    queries = []
    
    # PLJ uses citation format: YEAR PLJ COURT_SHORT PAGE
    # Court prefixes used in PLJ
    court_prefixes = ['', 'SC', 'Lahore', 'Karachi', 'Peshawar', 'Quetta', 'Islamabad', 
                      'FSC', 'Lahore High Court', 'Supreme Court']
    
    # Year by year, recent first
    for year in range(2026, 1969, -1):
        # Direct year queries
        queries.append(f"PLJ {year}")
        queries.append(f"{year} PLJ")
        # Court-specific
        for court in court_prefixes[:4]:
            if court:
                queries.append(f"{year} PLJ {court}")
        # Page number fishing (PLJ page numbers are small, 1-100 typically)
        for pg in [1, 10, 50, 100, 200, 500]:
            queries.append(f"{year} PLJ {pg}")
    
    return queries

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    prog = load_progress()
    s_session = make_session()
    
    saved_this_run = 0
    queries = generate_queries()
    done_queries = set(prog.get('queries_done', []))
    
    log.info(f"Starting PLJ v2 scraper. Already saved: {prog['total_saved']}. Queries available: {len(queries)}")
    
    for query in queries:
        if saved_this_run >= MAX_PER_RUN:
            log.info(f"Reached cap of {MAX_PER_RUN} cases for this run.")
            break
        if query in done_queries:
            continue
        
        delay = random.uniform(2, 5)
        time.sleep(delay)
        
        results = search_citation(s_session, query)
        
        new_saves = 0
        for case in results:
            if save_case(case, prog):
                new_saves += 1
                saved_this_run += 1
                log.info(f"SAVED [{prog['total_saved']}]: {case['title'][:50]} ({case['year']})")
        
        if results:
            log.info(f"Query '{query}': {len(results)} PLJ hits, {new_saves} new saves")
        
        prog['queries_done'].append(query)
        save_progress(prog)
    
    prog['last_run'] = datetime.now().isoformat()
    save_progress(prog)
    log.info(f"Run complete. Saved this run: {saved_this_run}. Total: {prog['total_saved']}")

if __name__ == '__main__':
    main()
