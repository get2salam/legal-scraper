"""
Shared test fixtures for legal_scraper tests.

Provides sample data, mock adapters, and temporary directories.
"""

import pytest

from legal_scraper.adapters.base import BaseAdapter

# ---------------------------------------------------------------------------
# Sample case data
# ---------------------------------------------------------------------------

SAMPLE_CASE_MINIMAL = {
    "id": "case_001",
    "title": "Smith v. Jones",
    "year": 2024,
}

SAMPLE_CASE_FULL = {
    "id": "case_002",
    "title": "Doe v. State",
    "citation": "2024 EX 123",
    "court": "High Court",
    "date": "2024-03-15",
    "year": 2024,
    "judges": ["Justice Adams", "Justice Baker"],
    "text": (
        "This is the full text of the judgment. "
        "The court considered Article 14 and Section 302 of the Penal Code. "
        "Reference was made to 2023 SC 445 and 2022 HC 112. "
        "Order III Rule 5 was also discussed."
    ),
    "headnote": "A case involving fundamental rights and criminal law.",
}

SAMPLE_CASE_NO_TEXT = {
    "id": "case_003",
    "title": "Corp v. Agency",
    "citation": "2024 EX 456",
    "court": "Supreme Court",
    "date": "2024-06-01",
    "year": 2024,
    "judges": ["Justice Adams"],
}

SAMPLE_CASES = [SAMPLE_CASE_MINIMAL, SAMPLE_CASE_FULL, SAMPLE_CASE_NO_TEXT]

SAMPLE_SEARCH_RESULTS = [
    {"id": "case_010", "title": "Alpha v. Beta", "year": 2024},
    {"id": "case_011", "title": "Gamma v. Delta", "year": 2024},
    {"id": "case_012", "title": "Epsilon v. Zeta", "year": 2023},
]

SAMPLE_CASE_IDS = ["case_2024_001", "case_2024_002", "case_2024_003"]


# ---------------------------------------------------------------------------
# Sample HTML responses (for adapter-level tests)
# ---------------------------------------------------------------------------

SAMPLE_HTML_SEARCH = """
<html>
<body>
<div class="results">
    <div class="result" data-id="case_010">
        <h3>Alpha v. Beta</h3>
        <span class="year">2024</span>
    </div>
    <div class="result" data-id="case_011">
        <h3>Gamma v. Delta</h3>
        <span class="year">2024</span>
    </div>
</div>
</body>
</html>
"""

SAMPLE_HTML_CASE = """
<html>
<body>
<div class="case-detail">
    <h1>Doe v. State</h1>
    <div class="citation">2024 EX 123</div>
    <div class="court">High Court</div>
    <div class="date">2024-03-15</div>
    <div class="judges">Justice Adams, Justice Baker</div>
    <div class="text">
        This is the full text of the judgment.
        The court considered Article 14 and Section 302 of the Penal Code.
    </div>
</div>
</body>
</html>
"""

SAMPLE_HTML_EMPTY = """
<html>
<body>
<div class="results">
    <p>No results found.</p>
</div>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class MockAdapter(BaseAdapter):
    """A mock adapter for testing the scraper engine."""

    NAME = "mock"
    BASE_URL = "https://mock-legal-db.test"

    def __init__(self):
        super().__init__()
        self._search_results = list(SAMPLE_SEARCH_RESULTS)
        self._cases = {c["id"]: dict(c) for c in SAMPLE_CASES}
        self._enumerated_ids = list(SAMPLE_CASE_IDS)

    def authenticate(self) -> bool:
        self.authenticated = True
        return True

    def search(self, query: str, **kwargs) -> list[dict]:
        return self._search_results

    def fetch_case(self, case_id: str):
        return self._cases.get(case_id)

    def enumerate_by_year(self, year: int, **kwargs) -> list[str]:
        return self._enumerated_ids


class FailingAdapter(BaseAdapter):
    """An adapter that always fails authentication."""

    NAME = "failing"
    BASE_URL = "https://failing-db.test"

    def authenticate(self) -> bool:
        self.authenticated = False
        return False

    def search(self, query: str, **kwargs) -> list[dict]:
        raise ConnectionError("Mock connection error")

    def fetch_case(self, case_id: str):
        raise ConnectionError("Mock connection error")


@pytest.fixture
def mock_adapter():
    """Return a fresh MockAdapter instance."""
    return MockAdapter()


@pytest.fixture
def failing_adapter():
    """Return a FailingAdapter instance."""
    return FailingAdapter()


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Provide a temporary data directory."""
    return str(tmp_path / "data")


@pytest.fixture
def sample_case():
    """Return a copy of a full sample case."""
    return dict(SAMPLE_CASE_FULL)


@pytest.fixture
def sample_cases():
    """Return copies of all sample cases."""
    return [dict(c) for c in SAMPLE_CASES]


@pytest.fixture
def populated_storage(tmp_data_dir):
    """Return a Storage instance pre-loaded with sample cases."""
    from legal_scraper.core.storage import Storage

    storage = Storage(tmp_data_dir)
    for case in SAMPLE_CASES:
        storage.save_case(dict(case))
    return storage


@pytest.fixture(autouse=True)
def _fast_timing(monkeypatch):
    """
    Patch time.sleep globally so timing tests don't actually wait.

    Individual test files can override this where needed.
    """
    monkeypatch.setattr("time.sleep", lambda _: None)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure environment variables don't leak between tests."""
    env_keys = [
        "DATA_DIR",
        "DAILY_REQUEST_LIMIT",
        "MIN_DELAY_SECONDS",
        "MAX_DELAY_SECONDS",
        "READING_PAUSE_CHANCE",
        "READING_PAUSE_MIN",
        "READING_PAUSE_MAX",
        "BREAK_AFTER_REQUESTS_MIN",
        "BREAK_AFTER_REQUESTS_MAX",
        "BREAK_DURATION_MIN",
        "BREAK_DURATION_MAX",
        "ADAPTER_USER",
        "ADAPTER_PASS",
    ]
    for key in env_keys:
        monkeypatch.delenv(key, raising=False)
