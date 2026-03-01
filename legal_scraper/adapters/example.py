"""
Example adapter - Template for creating new adapters.

Copy this file and modify for your target legal database.
"""

import os

from .base import BaseAdapter


class ExampleAdapter(BaseAdapter):
    """
    Example adapter demonstrating the adapter pattern.

    Replace with your actual legal database implementation.
    """

    NAME = "example"
    BASE_URL = "https://example-legal-db.com"

    def __init__(self):
        super().__init__()
        self.username = os.environ.get("ADAPTER_USER", "")
        self.password = os.environ.get("ADAPTER_PASS", "")

    def authenticate(self) -> bool:
        """
        Authenticate with the database.

        Example implementation - replace with actual login logic.
        """
        if not self.username or not self.password:
            print("Warning: No credentials provided")
            return False

        # Example: POST to login endpoint
        # response = self.session.post(
        #     f"{self.BASE_URL}/login",
        #     data={"username": self.username, "password": self.password}
        # )
        # return response.status_code == 200

        self.authenticated = True
        return True

    def search(self, query: str, **kwargs) -> list[dict]:
        """
        Search for cases.

        Example implementation - replace with actual search logic.
        """
        # Example: GET search endpoint
        # response = self.session.get(
        #     f"{self.BASE_URL}/search",
        #     params={"q": query, **kwargs}
        # )
        # return response.json().get("results", [])

        # Return mock data for demonstration
        return [
            {"id": "case_001", "title": f"Example Case - {query}", "year": 2024},
            {"id": "case_002", "title": f"Another Case - {query}", "year": 2024},
        ]

    def fetch_case(self, case_id: str) -> dict | None:
        """
        Fetch full case details.

        Example implementation - replace with actual fetch logic.
        """
        # Example: GET case endpoint
        # response = self.session.get(f"{self.BASE_URL}/cases/{case_id}")
        # if response.status_code == 404:
        #     return None
        # return response.json()

        # Return mock data for demonstration
        return {
            "id": case_id,
            "title": f"Example Case {case_id}",
            "citation": f"2024 EX {case_id[-3:]}",
            "court": "Example High Court",
            "date": "2024-01-15",
            "judges": ["Justice A", "Justice B"],
            "text": "This is the full text of the judgment...",
            "headnote": "Brief summary of the case...",
        }

    def enumerate_by_year(self, year: int, **kwargs) -> list[str]:
        """
        List all cases for a year.

        Example implementation - replace with actual enumeration logic.
        """
        # Example: GET year listing
        # response = self.session.get(
        #     f"{self.BASE_URL}/cases/year/{year}",
        #     params=kwargs
        # )
        # return [c["id"] for c in response.json().get("cases", [])]

        # Return mock data
        return [f"case_{year}_{i:03d}" for i in range(1, 11)]

    def get_courts(self) -> list[dict]:
        """Get available courts."""
        return [
            {"id": "supreme", "name": "Supreme Court"},
            {"id": "high", "name": "High Court"},
            {"id": "district", "name": "District Court"},
        ]

    def get_journals(self) -> list[dict]:
        """Get available journals/reporters."""
        return [
            {"id": "EX", "name": "Example Reports"},
            {"id": "ELR", "name": "Example Law Review"},
        ]
