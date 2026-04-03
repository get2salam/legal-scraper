"""Fix progress.json — remove B statute names that don't have files on disk."""
import json, re
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data_v2" / "legislation"
PROGRESS_FILE = DATA_DIR / "progress.json"

progress = json.load(open(PROGRESS_FILE, encoding='utf-8'))

old_count = len(progress["statutes_scraped"])

# Build set of names that have files on disk
has_file = set()
for letter_dir in DATA_DIR.iterdir():
    if letter_dir.is_dir() and len(letter_dir.name) == 1:
        for f in letter_dir.glob("*.json"):
            # Reverse the safe_name to approximate original name
            has_file.add(f.stem)

# For each name in statutes_scraped, check if its file exists
cleaned = []
removed = 0
for name in progress["statutes_scraped"]:
    safe_name = re.sub(r'[^\w\-]', '_', name)[:100]
    letter = name[0].upper() if name else "?"
    file_path = DATA_DIR / letter / f"{safe_name}.json"
    
    if file_path.exists():
        cleaned.append(name)
    else:
        removed += 1

progress["statutes_scraped"] = cleaned

# Fix completed_alphabets — only A is truly complete
progress["completed_alphabets"] = ["A"]
progress["current_alphabet"] = None

with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
    json.dump(progress, f, indent=2, ensure_ascii=False)

print(f"Before: {old_count} names in statutes_scraped")
print(f"Removed: {removed} names (no file on disk)")
print(f"After: {len(cleaned)} names")
print(f"Completed alphabets: {progress['completed_alphabets']}")

# Show breakdown
from collections import Counter
letters = Counter(n[0].upper() for n in cleaned if n)
for l in sorted(letters):
    print(f"  {l}: {letters[l]} with files")
