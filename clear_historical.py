"""Clear false 'completed' flags for pre-2018 years so scraper can re-run them."""
import json
from pathlib import Path

progress_file = Path("data_v2/progress.json")
p = json.loads(progress_file.read_text(encoding="utf-8"))

searches = p.get("completed_searches", [])
before = len(searches)

# Keep 2018-2026, remove anything before 2018
kept = [s for s in searches if int(s.split("-")[0]) >= 2018]
removed = [s for s in searches if int(s.split("-")[0]) < 2018]

p["completed_searches"] = kept
progress_file.write_text(json.dumps(p, indent=2, ensure_ascii=False), encoding="utf-8")

print(f"Before: {before} completed searches")
print(f"Removed: {len(removed)} (pre-2018)")
print(f"Kept: {len(kept)} (2018-2026)")
print(f"Years cleared: {sorted(set(s.split('-')[0] for s in removed))}")
