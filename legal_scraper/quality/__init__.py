"""
Data quality and validation for scraped legal data.

Provides schema validation, completeness scoring, near-duplicate detection,
and quality reporting for scraped case law and legislation.
"""

from .validator import CaseValidator, ValidationResult, validate_case
from .dedup import ContentFingerprinter, DuplicateDetector
from .reporter import QualityReporter, QualityReport

__all__ = [
    "CaseValidator",
    "ValidationResult",
    "validate_case",
    "ContentFingerprinter",
    "DuplicateDetector",
    "QualityReporter",
    "QualityReport",
]
