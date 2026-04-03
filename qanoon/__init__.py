"""
Qanoon - Pakistan Legal Research Platform
==========================================

A comprehensive package for scraping, processing, and serving Pakistani case law
and legislation data from Pakistan Law Site (pakistanlawsite.com).

Modules:
    scrapers: Web scrapers for cases, legislation, and linked cases
    data: Data cleaning, verification, and integrity checking
    db: Database imports for PostgreSQL, ChromaDB, and pgvector
    features: AI/ML features (judge intel, timelines, citations)
    api: FastAPI endpoints for the legal research platform

Usage:
    from qanoon.scrapers import CaseScraper, LegislationScraper
    from qanoon.data import DataCleaner, DataVerifier
    from qanoon.db import PostgresImporter, ChromaDBImporter

"""

__version__ = "2.0.0"
__author__ = "Qanoon Legal Research"

# Convenient imports
from qanoon.scrapers import BaseScraper, CaseScraper, LegislationScraper, LinkedCasesScraper

__all__ = [
    "BaseScraper",
    "CaseScraper",
    "LegislationScraper",
    "LinkedCasesScraper",
    "__version__",
]
