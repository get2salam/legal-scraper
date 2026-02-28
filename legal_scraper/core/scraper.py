"""
Main scraper engine.

Orchestrates adapters, timing, and storage for legal data scraping.
"""

import logging
import os

from ..adapters.base import BaseAdapter
from ..adapters.example import ExampleAdapter
from .storage import Storage
from .timing import HumanTiming

logger = logging.getLogger(__name__)

# Registry of available adapters
ADAPTERS = {
    "example": ExampleAdapter,
}


class Scraper:
    """
    Main scraper engine.

    Coordinates adapter, timing, and storage to scrape legal databases.

    Usage:
        scraper = Scraper(adapter="example")
        cases = scraper.search("constitutional petition")

        for case_id in scraper.enumerate(year=2024):
            scraper.fetch_and_save(case_id)
    """

    def __init__(
        self,
        adapter: str | BaseAdapter = "example",
        data_dir: str | None = None,
        daily_limit: int | None = None,
    ):
        """
        Initialize scraper.

        Args:
            adapter: Adapter name or instance
            data_dir: Directory for storing data
            daily_limit: Maximum requests per day
        """
        # Set up adapter
        if isinstance(adapter, str):
            if adapter not in ADAPTERS:
                raise ValueError(f"Unknown adapter: {adapter}. Available: {list(ADAPTERS.keys())}")
            self.adapter = ADAPTERS[adapter]()
        else:
            self.adapter = adapter

        # Set up components
        self.timing = HumanTiming()
        self.storage = Storage(data_dir)
        self.daily_limit = daily_limit or int(os.environ.get("DAILY_REQUEST_LIMIT", 500))
        self.request_count = 0

    def authenticate(self) -> bool:
        """Authenticate with the data source."""
        logger.info(f"Authenticating with {self.adapter.NAME}...")
        return self.adapter.authenticate()

    def search(self, query: str, **kwargs) -> list[dict]:
        """
        Search for cases.

        Args:
            query: Search terms
            **kwargs: Additional search parameters

        Returns:
            List of case metadata
        """
        self._check_limit()
        self.timing.wait()

        results = self.adapter.search(query, **kwargs)
        self.request_count += 1

        logger.info(f"Found {len(results)} cases for query: {query}")
        return results

    def fetch(self, case_id: str) -> dict | None:
        """
        Fetch a single case.

        Args:
            case_id: Case identifier

        Returns:
            Case dict or None if not found
        """
        self._check_limit()
        self.timing.wait()

        case = self.adapter.fetch_case(case_id)
        self.request_count += 1

        if case:
            logger.info(f"Fetched case: {case_id}")
        else:
            logger.warning(f"Case not found: {case_id}")

        return case

    def fetch_and_save(self, case_id: str, skip_existing: bool = True) -> bool:
        """
        Fetch a case and save to storage.

        Args:
            case_id: Case identifier
            skip_existing: Skip if already fetched

        Returns:
            True if case was fetched and saved
        """
        if skip_existing and self.storage.is_fetched(case_id):
            logger.debug(f"Skipping existing case: {case_id}")
            return False

        case = self.fetch(case_id)
        if case:
            return self.storage.save_case(case)
        return False

    def enumerate(self, year: int | None = None, **kwargs) -> list[str]:
        """
        Enumerate available cases.

        Args:
            year: Year to enumerate (if supported)
            **kwargs: Additional parameters

        Returns:
            List of case IDs
        """
        self._check_limit()
        self.timing.wait()

        if year:
            ids = self.adapter.enumerate_by_year(year, **kwargs)
        else:
            raise ValueError("Must specify year for enumeration")

        self.request_count += 1
        logger.info(f"Enumerated {len(ids)} cases")
        return ids

    def batch_fetch(
        self,
        case_ids: list[str],
        limit: int | None = None,
        skip_existing: bool = True,
    ) -> int:
        """
        Fetch multiple cases.

        Args:
            case_ids: List of case IDs to fetch
            limit: Maximum cases to fetch (None = all)
            skip_existing: Skip already fetched cases

        Returns:
            Number of cases fetched
        """
        fetched = 0
        for case_id in case_ids:
            if limit and fetched >= limit:
                break

            if self.fetch_and_save(case_id, skip_existing):
                fetched += 1

        logger.info(f"Batch complete: {fetched} cases fetched")
        return fetched

    def _check_limit(self):
        """Check if daily limit reached."""
        if self.request_count >= self.daily_limit:
            raise RuntimeError(
                f"Daily limit reached ({self.daily_limit}). "
                "Try again tomorrow or increase DAILY_REQUEST_LIMIT."
            )

    def status(self) -> dict:
        """Get current scraper status."""
        storage_stats = self.storage.get_stats()
        return {
            "adapter": self.adapter.NAME,
            "authenticated": self.adapter.authenticated,
            "requests_today": self.request_count,
            "daily_limit": self.daily_limit,
            "remaining": self.daily_limit - self.request_count,
            **storage_stats,
        }

    def close(self):
        """Clean up resources."""
        self.adapter.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
