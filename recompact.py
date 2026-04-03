import json, os
d = json.load(open("data_v2/analytics/judge_stats_full.json", encoding="utf-8"))
print(f"Total judges: {len(d['judges'])}")
print(f"Meta: {d['meta']}")
with open("data_v2/analytics/judge_stats.json", "w", encoding="utf-8") as f:
    json.dump(d, f, ensure_ascii=False)
size = os.path.getsize("data_v2/analytics/judge_stats.json")
print(f"Compact size: {size // (1024*1024)} MB")
