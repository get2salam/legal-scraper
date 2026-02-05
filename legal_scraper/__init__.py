"""
Legal Scraper - A modular framework for scraping legal databases.
"""

from .core.scraper import Scraper
from .adapters.base import BaseAdapter

__version__ = "0.1.0"
__all__ = ["Scraper", "BaseAdapter"]
