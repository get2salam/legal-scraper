"""Analytics tools for legal data."""

from .citations import extract_citations, CitationExtractor
from .stats import generate_stats

__all__ = ["extract_citations", "CitationExtractor", "generate_stats"]
