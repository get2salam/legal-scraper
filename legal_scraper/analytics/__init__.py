"""Analytics tools for legal data."""

from .citations import CitationExtractor, extract_citations
from .stats import generate_stats

__all__ = ["CitationExtractor", "extract_citations", "generate_stats"]
