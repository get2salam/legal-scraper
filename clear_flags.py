import json
from pathlib import Path
p = json.loads(Path("data_v2/progress.json").read_text(encoding="utf-8"))
cleared = [s for s in p["completed_searches"] if int(s.split("-")[0]) <= 2018]
p["completed_searches"] = [s for s in p["completed_searches"] if int(s.split("-")[0]) > 2018]
Path("data_v2/progress.json").write_text(json.dumps(p, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Cleared {len(cleared)} flags (2010-2018)")
print(f"Kept: {len(p['completed_searches'])} (2019-2025)")
