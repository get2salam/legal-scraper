import os
base = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper"
for root, dirs, files in os.walk(base):
    # skip data_v2, node_modules, __pycache__, .git
    dirs[:] = [d for d in dirs if d not in ('data_v2', 'node_modules', '__pycache__', '.git', 'original', 'html')]
    for f in files:
        if 'dashboard' in f.lower() and (f.endswith('.html') or f.endswith('.htm')):
            fp = os.path.join(root, f)
            size_mb = os.path.getsize(fp) / 1024 / 1024
            print(f"{fp} ({size_mb:.1f} MB)")
