"""
Qanoon Scrapers Module
======================

Web scrapers for Pakistan Law Site using curl_cffi with Chrome TLS fingerprinting.

Classes:
    BaseScraper: Base class with session management, delays, and operating hours
    CaseScraper: Scrapes case law by reporter and year
    LegislationScraper: Scrapes statutes/legislation alphabetically
    LinkedCasesScraper: Scrapes cases referenced in statute links

Example:
    from qanoon.scrapers import CaseScraper
    
    scraper = CaseScraper()
    if scraper.login():
        scraper.scrape_reporter_year("SCMR", 2024)
"""

from .base import BaseScraper
from .cases import CaseScraper
from .legislation import LegislationScraper
from .linked_cases import LinkedCasesScraper

__all__ = [
    "BaseScraper",
    "CaseScraper",
    "LegislationScraper",
    "LinkedCasesScraper",
]
