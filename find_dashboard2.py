import os
# Search broader - the dashboard might be in workspace root or analytics
search_dirs = [
    r"C:\Users\gempo\.openclaw\workspace",
    r"C:\Users\gempo\.openclaw\workspace\projects",
]
for search in search_dirs:
    for item in os.listdir(search):
        fp = os.path.join(search, item)
        if os.path.isfile(fp) and 'dashboard' in item.lower():
            size_mb = os.path.getsize(fp) / 1024 / 1024
            print(f"{fp} ({size_mb:.1f} MB)")

# Also check update_dashboard.py for output path
ud = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\analytics\update_dashboard.py"
if os.path.exists(ud):
    with open(ud) as f:
        for line in f:
            if 'output' in line.lower() or 'dashboard' in line.lower() or 'html' in line.lower():
                print(f"[update_dashboard.py] {line.strip()}")
