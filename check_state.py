import sqlite3, json

# Check scraped_cases.db
try:
    conn = sqlite3.connect('scraped_cases.db')
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    print('scraped_cases.db tables:', tables)
    for t in tables:
        tname = t[0]
        cur.execute(f"SELECT COUNT(*) FROM {tname}")
        print(f"  {tname}: {cur.fetchone()[0]} rows")
        cur.execute(f"PRAGMA table_info({tname})")
        cols = [c[1] for c in cur.fetchall()]
        print(f"  Columns: {cols}")
    conn.close()
except Exception as e:
    print('scraped_cases.db error:', e)

# Check cases.db
try:
    conn2 = sqlite3.connect('cases.db')
    cur2 = conn2.cursor()
    cur2.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables2 = cur2.fetchall()
    print('\ncases.db tables:', tables2)
    for t in tables2:
        tname = t[0]
        cur2.execute(f"SELECT COUNT(*) FROM {tname}")
        print(f"  {tname}: {cur2.fetchone()[0]} rows")
    conn2.close()
except Exception as e:
    print('cases.db error:', e)

# Check the scraper state file if any
import os
for fname in ['scraper_state.json', 'state.json', 'completed.json']:
    if os.path.exists(fname):
        with open(fname) as f:
            d = json.load(f)
        print(f'\n{fname}:', list(d.keys())[:20])
