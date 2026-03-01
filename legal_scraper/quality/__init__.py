"""
Data quality and validation for scraped legal data.

Provides schema validation, completeness scoring, near-duplicate detection,
and quality reporting for scraped case law and legislation.
"""

from .dedup import ContentFingerprinter, DuplicateDetector
from .reporter import QualityReport, QualityReporter
from .validator import CaseValidator, ValidationResult, validate_case

__all__ = [
    "CaseValidator",
    "ContentFingerprinter",
    "DuplicateDetector",
    "QualityReport",
    "QualityReporter",
    "ValidationResult",
    "validate_case",
]
