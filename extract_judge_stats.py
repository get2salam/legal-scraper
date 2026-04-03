"""
extract_judge_stats.py - Extract per-judge analytics from data_v2/ JSON files.

Scans all reporter directories (SCMR, PLD, PCrLJ, MLD, CLC, YLR, PTD, PLC, CLD, GBLR, PLCCS, PCRLJN)
Outputs: data_v2/analytics/judge_stats.json
"""

import json
import os
import sys
from collections import defaultdict
from pathlib import Path

BASE_DIR = Path(__file__).parent / "data_v2"
OUTPUT_DIR = BASE_DIR / "analytics"
OUTPUT_FILE = OUTPUT_DIR / "judge_stats.json"

REPORTERS = [
    "SCMR", "PLD", "PCrLJ", "MLD", "CLC", "YLR",
    "PTD", "PLC", "CLD", "GBLR", "PLCCS", "PCRLJN"
]


def normalise_judge(name):
    if not name:
        return ""
    name = str(name).strip()
    for prefix in ["Mr. ", "Mr ", "Mrs. ", "Mrs ", "Ms. ", "Ms ", "Dr. ", "Dr "]:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    return name


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # judge_name -> stats dict
    judges = defaultdict(lambda: {
        "total": 0,
        "by_reporter": defaultdict(int),
        "by_year": defaultdict(int),
        "courts": defaultdict(int),
    })

    total_files = 0
    total_cases = 0
    errors = 0

    for reporter in REPORTERS:
        reporter_dir = BASE_DIR / reporter
        if not reporter_dir.exists():
            print(f"  [skip] {reporter} - not found", flush=True)
            continue

        count = 0
        file_count = 0

        # Use os.walk for speed (no need to glob all files up front)
        for dirpath, dirnames, filenames in os.walk(str(reporter_dir)):
            for fname in filenames:
                if not fname.endswith(".json"):
                    continue
                filepath = os.path.join(dirpath, fname)
                file_count += 1
                total_files += 1

                try:
                    with open(filepath, encoding="utf-8", errors="replace") as f:
                        data = json.load(f)

                    raw_judges = data.get("judges", [])
                    if not raw_judges:
                        continue

                    if isinstance(raw_judges, str):
                        raw_judges = [raw_judges]
                    elif not isinstance(raw_judges, list):
                        raw_judges = [str(raw_judges)]

                    year = data.get("year")
                    court = (data.get("court") or "").strip() or "Unknown"
                    rep = (data.get("reporter") or reporter).strip().upper()

                    has_judges = False
                    for judge_name in raw_judges:
                        if not judge_name:
                            continue
                        name = normalise_judge(judge_name)
                        if not name or len(name) < 3:
                            continue
                        j = judges[name]
                        j["total"] += 1
                        j["by_reporter"][rep] += 1
                        if court:
                            j["courts"][court] += 1
                        if year:
                            j["by_year"][str(year)] += 1
                        has_judges = True

                    if has_judges:
                        total_cases += 1
                        count += 1

                except (json.JSONDecodeError, ValueError):
                    errors += 1
                except Exception:
                    errors += 1

        print(f"  [{reporter}] {file_count} files, {count} cases with judges", flush=True)

    print(f"\nTotal files scanned: {total_files}", flush=True)
    print(f"Cases with judge data: {total_cases}", flush=True)
    print(f"Unique judges found: {len(judges)}", flush=True)
    print(f"Errors: {errors}", flush=True)

    # Build output list
    output = []
    for name, stats in judges.items():
        by_reporter = dict(sorted(stats["by_reporter"].items(), key=lambda x: -x[1]))
        by_year = dict(sorted(stats["by_year"].items()))
        courts = dict(sorted(stats["courts"].items(), key=lambda x: -x[1]))

        top_reporter = max(by_reporter, key=by_reporter.get) if by_reporter else ""
        top_court = max(courts, key=courts.get) if courts else "Unknown"

        most_active_years = sorted(by_year.items(), key=lambda x: -x[1])[:3]
        most_active_years_str = ", ".join(y for y, _ in most_active_years)

        output.append({
            "name": name,
            "total": stats["total"],
            "by_reporter": by_reporter,
            "by_year": by_year,
            "courts": courts,
            "top_reporter": top_reporter,
            "top_court": top_court,
            "most_active_years": most_active_years_str,
        })

    output.sort(key=lambda x: -x["total"])

    total_judges = len(output)
    avg_cases = round(total_cases / total_judges, 1) if total_judges else 0
    most_prolific = output[0]["name"] if output else ""

    result = {
        "meta": {
            "total_judges": total_judges,
            "total_cases_with_judges": total_cases,
            "avg_cases_per_judge": avg_cases,
            "most_prolific": most_prolific,
        },
        "judges": output,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\nSaved to {OUTPUT_FILE}", flush=True)
    print(f"  {total_judges} judges, {total_cases} cases, avg {avg_cases}/judge", flush=True)
    print(f"  Most prolific: {most_prolific} ({output[0]['total'] if output else 0} cases)", flush=True)


if __name__ == "__main__":
    print("Extracting judge statistics from data_v2/ ...", flush=True)
    main()
    print("DONE", flush=True)
