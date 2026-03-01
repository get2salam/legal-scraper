"""
Base adapter for legal database scrapers.

All adapters must inherit from BaseAdapter and implement the required methods.
"""

from abc import ABC, abstractmethod

import requests


class BaseAdapter(ABC):
    """Abstract base class for legal database adapters."""

    # Override in subclass
    NAME = "base"
    BASE_URL = ""

    def __init__(self):
        self.session = requests.Session()
        self.authenticated = False

    @abstractmethod
    def authenticate(self) -> bool:
        """
        Authenticate with the legal database.

        Returns:
            True if authentication successful, False otherwise.
        """
        pass

    @abstractmethod
    def search(self, query: str, **kwargs) -> list[dict]:
        """
        Search for cases matching the query.

        Args:
            query: Search terms
            **kwargs: Additional search parameters (court, year, etc.)

        Returns:
            List of case metadata dicts with at least 'id' and 'title' keys.
        """
        pass

    @abstractmethod
    def fetch_case(self, case_id: str) -> dict | None:
        """
        Fetch full case details by ID.

        Args:
            case_id: Unique identifier for the case

        Returns:
            Dict with case details including 'text', 'citation', 'court', 'date', etc.
            Returns None if case not found.
        """
        pass

    def enumerate_by_year(self, year: int, **kwargs) -> list[str]:
        """
        List all case IDs for a given year.

        Args:
            year: Year to enumerate
            **kwargs: Additional parameters (court, journal, etc.)

        Returns:
            List of case IDs.
        """
        raise NotImplementedError("This adapter doesn't support enumeration by year")

    def enumerate_by_citation(self, citation_prefix: str) -> list[str]:
        """
        List all case IDs matching a citation pattern.

        Args:
            citation_prefix: Citation prefix (e.g., "2024 SC")

        Returns:
            List of case IDs.
        """
        raise NotImplementedError("This adapter doesn't support citation enumeration")

    def get_courts(self) -> list[dict]:
        """
        Get list of available courts.

        Returns:
            List of court dicts with 'id' and 'name' keys.
        """
        return []

    def get_journals(self) -> list[dict]:
        """
        Get list of available journals/reporters.

        Returns:
            List of journal dicts with 'id' and 'name' keys.
        """
        return []

    def close(self):
        """Clean up resources."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
