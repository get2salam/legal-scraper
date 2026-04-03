"""Debug what the LawGPT API actually returns and what Sections are present."""
import sys, json, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from curl_cffi import requests as r

s = r.Session()
s.impersonate = 'chrome'
SEARCH = 'https://prod-search-engine.azurewebsites.net/api/search/lawcases'

KNOWN = {'SCMR','PLD','PCrLJ','MLD','CLC','YLR','PTD','PLC','CLD','GBLR',
         'PLCCS','PCRLJN','YLRN','PLCCSN','CLCN'}

# Query 5 different things and see ALL sections returned
queries = ['LHC 2024', 'SHC 2023', 'unreported judgment', 'Lahore High Court 2024', 'writ petition 2024']

all_sections = {}
for q in queries:
    resp = s.post(SEARCH, json={'query': q, 'mode': 'keyword', 'page': 1, 'pageSize': 10},
                  headers={'Content-Type': 'application/json'}, timeout=15)
    if resp.status_code == 200:
        data = resp.json()
        vals = data.get('value', [])
        print(f"\nQuery: '{q}' -> {len(vals)} results")
        for v in vals:
            sec = v.get('Section', [])
            if isinstance(sec, list):
                for s_ in sec:
                    all_sections[s_] = all_sections.get(s_, 0) + 1
            elif isinstance(sec, str):
                all_sections[sec] = all_sections.get(sec, 0) + 1
            print(f"  Section={sec} | Title={v.get('Title','')[:50]}")
    time.sleep(2)

print(f"\n\nAll sections seen: {dict(sorted(all_sections.items(), key=lambda x: -x[1]))}")
unknown = {k:v for k,v in all_sections.items() if k not in KNOWN}
print(f"Unknown (unreported) sections: {unknown}")
