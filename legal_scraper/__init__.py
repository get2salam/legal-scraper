"""
Legal Scraper - A modular framework for scraping legal databases.
"""

from .adapters.base import BaseAdapter
from .core.scraper import Scraper

__version__ = "0.1.0"
__all__ = ["BaseAdapter", "Scraper"]
