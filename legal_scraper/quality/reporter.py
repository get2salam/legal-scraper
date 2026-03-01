"""
Quality reporting and metrics aggregation.

Generates comprehensive quality reports for scraped datasets,
tracking validation pass rates, completeness trends, and data issues.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .validator import CaseValidator, Severity, ValidationResult


@dataclass
class QualityReport:
    """Aggregated quality report for a dataset."""

    generated_at: str
    total_cases: int
    valid_cases: int
    invalid_cases: int
    avg_completeness: float
    field_completeness: dict[str, float]
    severity_counts: dict[str, int]
    top_issues: list[dict]
    completeness_distribution: dict[str, int]

    @property
    def pass_rate(self) -> float:
        """Fraction of cases passing validation."""
        return self.valid_cases / self.total_cases if self.total_cases else 0.0

    def to_dict(self) -> dict:
        return {
            "generated_at": self.generated_at,
            "summary": {
                "total_cases": self.total_cases,
                "valid_cases": self.valid_cases,
                "invalid_cases": self.invalid_cases,
                "pass_rate": round(self.pass_rate, 4),
                "avg_completeness": round(self.avg_completeness, 4),
            },
            "field_completeness": {k: round(v, 4) for k, v in self.field_completeness.items()},
            "severity_counts": self.severity_counts,
            "top_issues": self.top_issues,
            "completeness_distribution": self.completeness_distribution,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def save(self, path: str | Path):
        """Save report to JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(self.to_json())

    def summary_text(self) -> str:
        """Human-readable summary."""
        lines = [
            f"Quality Report — {self.generated_at}",
            f"{'=' * 50}",
            f"Total cases:     {self.total_cases:,}",
            f"Valid:           {self.valid_cases:,} ({self.pass_rate:.1%})",
            f"Invalid:         {self.invalid_cases:,}",
            f"Avg completeness: {self.avg_completeness:.1%}",
            "",
            "Field Completeness:",
        ]
        for field_name, score in sorted(
            self.field_completeness.items(),
            key=lambda x: x[1],
            reverse=True,
        ):
            bar = "█" * int(score * 20)
            lines.append(f"  {field_name:20s} {score:5.1%} {bar}")

        if self.top_issues:
            lines.append("")
            lines.append("Top Issues:")
            for issue in self.top_issues[:10]:
                lines.append(
                    f"  [{issue['severity']}] {issue['field']}: "
                    f"{issue['message']} (x{issue['count']})"
                )

        return "\n".join(lines)


class QualityReporter:
    """
    Generate quality reports from validation results.

    Aggregates individual case validation results into dataset-level
    metrics and quality trends.

    Usage:
        reporter = QualityReporter()
        report = reporter.analyze(cases)
        print(report.summary_text())
        report.save("reports/quality_2024.json")
    """

    def __init__(
        self,
        validator: CaseValidator | None = None,
    ):
        """
        Args:
            validator: CaseValidator instance (creates default if None)
        """
        self.validator = validator or CaseValidator()

    def analyze(self, cases: list[dict]) -> QualityReport:
        """
        Run validation on all cases and generate a report.

        Args:
            cases: List of case dicts

        Returns:
            QualityReport with aggregated metrics
        """
        results = self.validator.validate_batch(cases)
        return self._aggregate(results)

    def analyze_results(self, results: list[ValidationResult]) -> QualityReport:
        """
        Generate a report from pre-computed validation results.

        Args:
            results: List of ValidationResult objects

        Returns:
            QualityReport
        """
        return self._aggregate(results)

    def _aggregate(self, results: list[ValidationResult]) -> QualityReport:
        """Aggregate validation results into a report."""
        total = len(results)
        if total == 0:
            return QualityReport(
                generated_at=datetime.now().isoformat(),
                total_cases=0,
                valid_cases=0,
                invalid_cases=0,
                avg_completeness=0.0,
                field_completeness={},
                severity_counts={},
                top_issues=[],
                completeness_distribution={},
            )

        valid_count = sum(1 for r in results if r.valid)

        # Average completeness
        avg_completeness = sum(r.completeness_score for r in results) / total

        # Per-field completeness (average across all cases)
        field_totals: dict[str, float] = defaultdict(float)
        field_counts: dict[str, int] = defaultdict(int)
        for r in results:
            for field_name, score in r.field_scores.items():
                field_totals[field_name] += score
                field_counts[field_name] += 1

        field_completeness = {
            name: field_totals[name] / field_counts[name] for name in field_totals
        }

        # Severity counts
        severity_counts: Counter = Counter()
        for r in results:
            for issue in r.issues:
                severity_counts[issue.severity.value] += 1

        # Top issues (grouped by field + message)
        issue_groups: Counter = Counter()
        issue_details: dict[tuple, dict] = {}
        for r in results:
            for issue in r.issues:
                if issue.severity == Severity.INFO:
                    continue
                key = (issue.field, issue.message, issue.severity.value)
                issue_groups[key] += 1
                if key not in issue_details:
                    issue_details[key] = {
                        "field": issue.field,
                        "message": issue.message,
                        "severity": issue.severity.value,
                    }

        top_issues = [
            {**issue_details[key], "count": count} for key, count in issue_groups.most_common(20)
        ]

        # Completeness distribution (buckets)
        buckets = {"0-20%": 0, "20-40%": 0, "40-60%": 0, "60-80%": 0, "80-100%": 0}
        for r in results:
            pct = r.completeness_score * 100
            if pct < 20:
                buckets["0-20%"] += 1
            elif pct < 40:
                buckets["20-40%"] += 1
            elif pct < 60:
                buckets["40-60%"] += 1
            elif pct < 80:
                buckets["60-80%"] += 1
            else:
                buckets["80-100%"] += 1

        return QualityReport(
            generated_at=datetime.now().isoformat(),
            total_cases=total,
            valid_cases=valid_count,
            invalid_cases=total - valid_count,
            avg_completeness=avg_completeness,
            field_completeness=field_completeness,
            severity_counts=dict(severity_counts),
            top_issues=top_issues,
            completeness_distribution=buckets,
        )

    def compare_reports(
        self,
        before: QualityReport,
        after: QualityReport,
    ) -> dict:
        """
        Compare two quality reports to track improvement.

        Args:
            before: Earlier report
            after: Later report

        Returns:
            Comparison metrics
        """
        return {
            "period": {
                "before": before.generated_at,
                "after": after.generated_at,
            },
            "cases": {
                "before": before.total_cases,
                "after": after.total_cases,
                "delta": after.total_cases - before.total_cases,
            },
            "pass_rate": {
                "before": round(before.pass_rate, 4),
                "after": round(after.pass_rate, 4),
                "delta": round(after.pass_rate - before.pass_rate, 4),
            },
            "completeness": {
                "before": round(before.avg_completeness, 4),
                "after": round(after.avg_completeness, 4),
                "delta": round(after.avg_completeness - before.avg_completeness, 4),
            },
            "field_changes": {
                field: {
                    "before": round(before.field_completeness.get(field, 0), 4),
                    "after": round(score, 4),
                    "delta": round(score - before.field_completeness.get(field, 0), 4),
                }
                for field, score in after.field_completeness.items()
            },
        }


def generate_quality_report(
    cases_dir: str | Path,
    output: str | Path | None = None,
) -> QualityReport:
    """
    Convenience function: load cases from directory and generate report.

    Args:
        cases_dir: Directory containing case JSON files
        output: Optional path to save the report

    Returns:
        QualityReport
    """
    cases_dir = Path(cases_dir)
    cases = []
    for json_file in cases_dir.glob("*.json"):
        try:
            with open(json_file, encoding="utf-8") as f:
                cases.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            continue

    reporter = QualityReporter()
    report = reporter.analyze(cases)

    if output:
        report.save(output)

    return report
