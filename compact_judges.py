import json, sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
d = json.load(open('data_v2/analytics/judge_stats.json', encoding='utf-8'))
compact = {"meta": d["meta"], "judges": []}
for j in d["judges"]:
    compact["judges"].append({
        "name": j["name"],
        "total": j["total"],
        "top_reporter": j.get("top_reporter", ""),
        "top_court": j.get("top_court", ""),
        "most_active_years": j.get("most_active_years", ""),
        "reporters": j.get("by_reporter", {}),
        "years": j.get("by_year", {}),
    })
# Overwrite the original with compact version
with open("data_v2/analytics/judge_stats.json", "w", encoding="utf-8") as f:
    json.dump(compact, f, ensure_ascii=False)
size = os.path.getsize("data_v2/analytics/judge_stats.json")
print(f"Done: {len(compact['judges'])} judges, {size // 1024} KB")
