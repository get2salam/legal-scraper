import sqlite3
import os

# Find the right db
for f in os.listdir('.'):
    if f.endswith('.db'):
        print('DB found:', f)

conn = sqlite3.connect('scraped_cases.db')
cur = conn.cursor()

# List tables
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('Tables:', cur.fetchall())

conn.close()
