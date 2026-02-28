"""
Statistical analysis of legal case data.
"""

import json
from collections import Counter
from pathlib import Path


def generate_stats(
    cases_dir: str | Path = "data/cases",
    cases: list[dict] | None = None,
) -> dict:
    """
    Generate statistics from case data.

    Args:
        cases_dir: Directory containing case JSON files
        cases: Or provide list of case dicts directly

    Returns:
        Statistics dict
    """
    # Load cases if not provided
    if cases is None:
        cases_dir = Path(cases_dir)
        cases = []
        for json_file in cases_dir.glob("*.json"):
            with open(json_file, encoding="utf-8") as f:
                cases.append(json.load(f))

    if not cases:
        return {"error": "No cases found"}

    # Basic counts
    total = len(cases)

    # Court distribution
    courts = Counter(c.get("court", "Unknown") for c in cases)

    # Year distribution
    years = Counter(c.get("year", "Unknown") for c in cases)

    # Text length stats
    text_lengths = [len(c.get("text", "")) for c in cases if c.get("text")]

    avg_length = sum(text_lengths) / max(len(text_lengths), 1)
    max_length = max(text_lengths) if text_lengths else 0
    min_length = min(text_lengths) if text_lengths else 0

    # Judge frequency (if available)
    all_judges = []
    for c in cases:
        judges = c.get("judges", [])
        if isinstance(judges, list):
            all_judges.extend(judges)
    judge_counts = Counter(all_judges)

    return {
        "total_cases": total,
        "courts": dict(courts.most_common(10)),
        "years": dict(sorted(years.items())),
        "text_stats": {
            "avg_length": round(avg_length),
            "max_length": max_length,
            "min_length": min_length,
            "cases_with_text": len(text_lengths),
        },
        "top_judges": dict(judge_counts.most_common(10)),
    }


def compare_periods(
    cases: list[dict],
    year_field: str = "year",
    period1: tuple[int, int] | None = None,
    period2: tuple[int, int] | None = None,
) -> dict:
    """
    Compare statistics between two time periods.

    Args:
        cases: List of case dicts
        year_field: Key containing year
        period1: (start_year, end_year) for first period
        period2: (start_year, end_year) for second period

    Returns:
        Comparison stats
    """
    if not period1 or not period2:
        return {"error": "Must specify both periods"}

    def in_period(case, period):
        year = case.get(year_field)
        if year is None:
            return False
        return period[0] <= year <= period[1]

    cases1 = [c for c in cases if in_period(c, period1)]
    cases2 = [c for c in cases if in_period(c, period2)]

    stats1 = generate_stats(cases=cases1)
    stats2 = generate_stats(cases=cases2)

    return {
        f"period_{period1[0]}_{period1[1]}": stats1,
        f"period_{period2[0]}_{period2[1]}": stats2,
        "comparison": {
            "case_count_change": stats2["total_cases"] - stats1["total_cases"],
            "avg_length_change": (
                stats2["text_stats"]["avg_length"] - stats1["text_stats"]["avg_length"]
            ),
        },
    }
