"""
Schema-based validation for scraped legal data.

Validates field presence, types, value constraints, and computes
completeness scores for individual cases and entire datasets.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class Severity(str, Enum):
    """Validation issue severity."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """A single validation issue found in a case."""

    field: str
    severity: Severity
    message: str
    value: Any = None

    def __str__(self) -> str:
        return f"[{self.severity.value.upper()}] {self.field}: {self.message}"


@dataclass
class ValidationResult:
    """Result of validating a single case."""

    case_id: str
    valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)
    completeness_score: float = 0.0
    field_scores: dict[str, float] = field(default_factory=dict)

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    def to_dict(self) -> dict:
        return {
            "case_id": self.case_id,
            "valid": self.valid,
            "completeness_score": round(self.completeness_score, 3),
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "issues": [
                {
                    "field": i.field,
                    "severity": i.severity.value,
                    "message": i.message,
                }
                for i in self.issues
            ],
            "field_scores": {k: round(v, 3) for k, v in self.field_scores.items()},
        }


@dataclass
class FieldSpec:
    """Specification for a case field."""

    name: str
    required: bool = False
    field_type: type | tuple[type, ...] = str
    min_length: int | None = None
    max_length: int | None = None
    pattern: str | None = None
    custom_check: Callable[[Any], str | None] | None = None
    weight: float = 1.0  # Weight for completeness scoring


# Default field specs for a legal case
DEFAULT_CASE_SCHEMA: list[FieldSpec] = [
    FieldSpec(
        name="id",
        required=True,
        field_type=str,
        min_length=1,
        weight=1.0,
    ),
    FieldSpec(
        name="title",
        required=True,
        field_type=str,
        min_length=5,
        weight=1.0,
    ),
    FieldSpec(
        name="citation",
        required=False,
        field_type=str,
        min_length=3,
        pattern=r".+\d+.*",  # Should contain at least one digit
        weight=0.9,
    ),
    FieldSpec(
        name="court",
        required=False,
        field_type=str,
        min_length=2,
        weight=0.8,
    ),
    FieldSpec(
        name="date",
        required=False,
        field_type=str,
        pattern=r"\d{4}[-/]\d{1,2}[-/]\d{1,2}",
        weight=0.7,
    ),
    FieldSpec(
        name="year",
        required=False,
        field_type=(int, str),
        weight=0.7,
    ),
    FieldSpec(
        name="judges",
        required=False,
        field_type=(list, str),
        weight=0.5,
    ),
    FieldSpec(
        name="text",
        required=True,
        field_type=str,
        min_length=100,
        weight=1.0,
    ),
    FieldSpec(
        name="headnotes",
        required=False,
        field_type=(str, list),
        weight=0.4,
    ),
    FieldSpec(
        name="statutes_cited",
        required=False,
        field_type=(list, str),
        weight=0.3,
    ),
]


class CaseValidator:
    """
    Validates scraped legal case data against a schema.

    Checks field presence, types, constraints, and computes
    a weighted completeness score.

    Usage:
        validator = CaseValidator()
        result = validator.validate(case_dict)
        print(result.completeness_score)
        print(result.errors)
    """

    def __init__(
        self,
        schema: list[FieldSpec] | None = None,
        strict: bool = False,
    ):
        """
        Args:
            schema: Field specifications (defaults to standard case schema)
            strict: If True, warnings become errors
        """
        self.schema = list(schema) if schema is not None else list(DEFAULT_CASE_SCHEMA)
        self.strict = strict
        self._field_map = {spec.name: spec for spec in self.schema}

    def validate(self, case: dict) -> ValidationResult:
        """
        Validate a single case.

        Args:
            case: Case data dict

        Returns:
            ValidationResult with issues and completeness score
        """
        case_id = case.get("id", "<unknown>")
        issues: list[ValidationIssue] = []
        field_scores: dict[str, float] = {}
        total_weight = sum(spec.weight for spec in self.schema)

        for spec in self.schema:
            score, field_issues = self._validate_field(case, spec)
            field_scores[spec.name] = score
            issues.extend(field_issues)

        # Compute weighted completeness
        weighted_sum = sum(field_scores.get(spec.name, 0.0) * spec.weight for spec in self.schema)
        completeness = weighted_sum / total_weight if total_weight > 0 else 0.0

        # Check for unknown fields (info-level)
        known_fields = {spec.name for spec in self.schema}
        known_fields.add("_scraped_at")  # Internal metadata
        for key in case:
            if key not in known_fields and not key.startswith("_"):
                issues.append(
                    ValidationIssue(
                        field=key,
                        severity=Severity.INFO,
                        message="Unknown field not in schema",
                    )
                )

        has_errors = any(i.severity == Severity.ERROR for i in issues)

        return ValidationResult(
            case_id=case_id,
            valid=not has_errors,
            issues=issues,
            completeness_score=completeness,
            field_scores=field_scores,
        )

    def validate_batch(
        self,
        cases: list[dict],
    ) -> list[ValidationResult]:
        """Validate multiple cases."""
        return [self.validate(case) for case in cases]

    def _validate_field(self, case: dict, spec: FieldSpec) -> tuple[float, list[ValidationIssue]]:
        """
        Validate a single field against its spec.

        Returns:
            (score 0.0-1.0, list of issues)
        """
        issues: list[ValidationIssue] = []
        value = case.get(spec.name)

        # Missing field
        if value is None:
            if spec.required:
                issues.append(
                    ValidationIssue(
                        field=spec.name,
                        severity=Severity.ERROR,
                        message="Required field is missing",
                    )
                )
            return 0.0, issues

        # Type check
        if not isinstance(value, spec.field_type):
            severity = Severity.ERROR if spec.required else Severity.WARNING
            if self.strict:
                severity = Severity.ERROR
            issues.append(
                ValidationIssue(
                    field=spec.name,
                    severity=severity,
                    message=(f"Expected type {spec.field_type}, got {type(value).__name__}"),
                    value=type(value).__name__,
                )
            )
            return 0.2, issues

        # Length checks (for strings)
        if isinstance(value, str):
            if spec.min_length and len(value) < spec.min_length:
                issues.append(
                    ValidationIssue(
                        field=spec.name,
                        severity=(Severity.ERROR if spec.required else Severity.WARNING),
                        message=(f"Too short: {len(value)} chars (min: {spec.min_length})"),
                        value=len(value),
                    )
                )
                # Partial score based on how close to min
                return min(len(value) / spec.min_length, 0.8), issues

            if spec.max_length and len(value) > spec.max_length:
                issues.append(
                    ValidationIssue(
                        field=spec.name,
                        severity=Severity.WARNING,
                        message=(f"Too long: {len(value)} chars (max: {spec.max_length})"),
                        value=len(value),
                    )
                )
                return 0.9, issues

        # Pattern check
        if spec.pattern and isinstance(value, str) and not re.search(spec.pattern, value):
            issues.append(
                ValidationIssue(
                    field=spec.name,
                    severity=Severity.WARNING,
                    message="Does not match expected pattern",
                    value=value[:100],
                )
            )
            return 0.7, issues

        # Custom check
        if spec.custom_check:
            error_msg = spec.custom_check(value)
            if error_msg:
                issues.append(
                    ValidationIssue(
                        field=spec.name,
                        severity=Severity.WARNING,
                        message=error_msg,
                        value=value,
                    )
                )
                return 0.8, issues

        # Length-based bonus for text fields
        if isinstance(value, str) and spec.min_length:
            ratio = min(len(value) / (spec.min_length * 10), 1.0)
            return max(ratio, 0.9), issues

        return 1.0, issues

    def add_field(self, spec: FieldSpec):
        """Add a field spec to the schema."""
        self.schema.append(spec)
        self._field_map[spec.name] = spec

    def remove_field(self, name: str):
        """Remove a field spec from the schema."""
        self.schema = [s for s in self.schema if s.name != name]
        self._field_map.pop(name, None)


def validate_case(case: dict, strict: bool = False) -> ValidationResult:
    """
    Convenience function to validate a single case.

    Args:
        case: Case dict
        strict: Treat warnings as errors

    Returns:
        ValidationResult
    """
    validator = CaseValidator(strict=strict)
    return validator.validate(case)


def check_text_quality(text: str) -> dict:
    """
    Analyze text quality metrics for a legal document.

    Args:
        text: Document text

    Returns:
        Quality metrics dict
    """
    if not text:
        return {"quality": "empty", "score": 0.0}

    words = text.split()
    word_count = len(words)
    sentences = re.split(r"[.!?]+", text)
    sentence_count = len([s for s in sentences if s.strip()])

    # Check for common scraping artifacts
    artifacts = {
        "excessive_whitespace": bool(re.search(r"\s{10,}", text)),
        "html_tags": bool(re.search(r"<[a-zA-Z][^>]*>", text)),
        "encoding_errors": bool(re.search(r"[ï¿½â€™â€œ]", text)),
        "truncated": text.rstrip().endswith("...") or word_count < 50,
        "boilerplate_heavy": _boilerplate_ratio(text) > 0.3,
    }

    artifact_count = sum(1 for v in artifacts.values() if v)

    # Quality score (0-1)
    score = 1.0
    if word_count < 50:
        score -= 0.4
    elif word_count < 200:
        score -= 0.1
    if artifact_count > 0:
        score -= artifact_count * 0.15
    score = max(score, 0.0)

    return {
        "word_count": word_count,
        "sentence_count": sentence_count,
        "avg_sentence_length": (round(word_count / sentence_count, 1) if sentence_count else 0),
        "artifacts": artifacts,
        "artifact_count": artifact_count,
        "quality_score": round(score, 3),
    }


def _boilerplate_ratio(text: str) -> float:
    """Estimate fraction of text that is boilerplate."""
    boilerplate_patterns = [
        r"all rights reserved",
        r"disclaimer",
        r"terms of use",
        r"copyright \d{4}",
        r"click here",
        r"page \d+ of \d+",
        r"login required",
    ]
    boilerplate_chars = 0
    for pattern in boilerplate_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            boilerplate_chars += len(match.group())
    return boilerplate_chars / len(text) if text else 0.0
