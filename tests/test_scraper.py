"""
Unit tests for the core Scraper engine.

Tests cover initialisation, search, fetch, enumeration, batching,
daily limits, and context-manager behaviour — all with mocked adapters
so no real HTTP requests are made.
"""

import pytest

from legal_scraper.core.scraper import Scraper, ADAPTERS
from tests.conftest import MockAdapter, FailingAdapter, SAMPLE_SEARCH_RESULTS


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestScraperInit:
    """Scraper construction and adapter resolution."""

    def test_init_with_adapter_name(self, tmp_data_dir):
        """Built-in adapter names should resolve correctly."""
        scraper = Scraper(adapter="example", data_dir=tmp_data_dir)
        assert scraper.adapter.NAME == "example"

    def test_init_with_adapter_instance(self, mock_adapter, tmp_data_dir):
        """Passing an adapter instance should be used directly."""
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        assert scraper.adapter is mock_adapter

    def test_init_unknown_adapter_raises(self, tmp_data_dir):
        """Unknown adapter name should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown adapter"):
            Scraper(adapter="nonexistent", data_dir=tmp_data_dir)

    def test_default_daily_limit(self, mock_adapter, tmp_data_dir):
        """Default daily limit should be 500."""
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        assert scraper.daily_limit == 500

    def test_custom_daily_limit(self, mock_adapter, tmp_data_dir):
        """Explicit daily_limit kwarg should override default."""
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir, daily_limit=10)
        assert scraper.daily_limit == 10

    def test_daily_limit_from_env(self, mock_adapter, tmp_data_dir, monkeypatch):
        """DAILY_REQUEST_LIMIT env var should be respected."""
        monkeypatch.setenv("DAILY_REQUEST_LIMIT", "42")
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        assert scraper.daily_limit == 42


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class TestAuthentication:
    def test_authenticate_success(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        assert scraper.authenticate() is True
        assert scraper.adapter.authenticated is True

    def test_authenticate_failure(self, failing_adapter, tmp_data_dir):
        scraper = Scraper(adapter=failing_adapter, data_dir=tmp_data_dir)
        assert scraper.authenticate() is False


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


class TestSearch:
    def test_search_returns_results(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        results = scraper.search("test query")
        assert len(results) == len(SAMPLE_SEARCH_RESULTS)
        assert results[0]["id"] == "case_010"

    def test_search_increments_request_count(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        assert scraper.request_count == 0
        scraper.search("query")
        assert scraper.request_count == 1


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


class TestFetch:
    def test_fetch_existing_case(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        case = scraper.fetch("case_001")
        assert case is not None
        assert case["id"] == "case_001"

    def test_fetch_nonexistent_case(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        case = scraper.fetch("nonexistent_999")
        assert case is None

    def test_fetch_increments_request_count(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        scraper.fetch("case_001")
        assert scraper.request_count == 1


# ---------------------------------------------------------------------------
# Fetch & save
# ---------------------------------------------------------------------------


class TestFetchAndSave:
    def test_fetch_and_save_new_case(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        result = scraper.fetch_and_save("case_002")
        assert result is True
        assert scraper.storage.is_fetched("case_002")

    def test_fetch_and_save_skips_existing(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        scraper.fetch_and_save("case_002")
        initial_count = scraper.request_count
        result = scraper.fetch_and_save("case_002", skip_existing=True)
        assert result is False
        # Should not have made another request
        assert scraper.request_count == initial_count

    def test_fetch_and_save_does_not_skip_when_disabled(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        scraper.fetch_and_save("case_002")
        result = scraper.fetch_and_save("case_002", skip_existing=False)
        assert result is True

    def test_fetch_and_save_nonexistent(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        result = scraper.fetch_and_save("nonexistent_999")
        assert result is False


# ---------------------------------------------------------------------------
# Enumeration
# ---------------------------------------------------------------------------


class TestEnumerate:
    def test_enumerate_by_year(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        ids = scraper.enumerate(year=2024)
        assert isinstance(ids, list)
        assert len(ids) > 0

    def test_enumerate_without_year_raises(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        with pytest.raises(ValueError, match="Must specify year"):
            scraper.enumerate()


# ---------------------------------------------------------------------------
# Batch fetch
# ---------------------------------------------------------------------------


class TestBatchFetch:
    def test_batch_fetch_all(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        # Add the case IDs to the mock adapter's case dict
        for cid in ["id_a", "id_b", "id_c"]:
            mock_adapter._cases[cid] = {"id": cid, "title": f"Case {cid}"}
        fetched = scraper.batch_fetch(["id_a", "id_b", "id_c"])
        assert fetched == 3

    def test_batch_fetch_with_limit(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        for cid in ["id_a", "id_b", "id_c"]:
            mock_adapter._cases[cid] = {"id": cid, "title": f"Case {cid}"}
        fetched = scraper.batch_fetch(["id_a", "id_b", "id_c"], limit=2)
        assert fetched == 2

    def test_batch_fetch_skips_existing(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        mock_adapter._cases["id_a"] = {"id": "id_a", "title": "Case A"}
        scraper.fetch_and_save("id_a")
        fetched = scraper.batch_fetch(["id_a"], skip_existing=True)
        assert fetched == 0


# ---------------------------------------------------------------------------
# Daily limit
# ---------------------------------------------------------------------------


class TestDailyLimit:
    def test_limit_raises_runtime_error(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir, daily_limit=2)
        scraper.search("q1")
        scraper.search("q2")
        with pytest.raises(RuntimeError, match="Daily limit reached"):
            scraper.search("q3")

    def test_limit_blocks_fetch(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir, daily_limit=1)
        scraper.fetch("case_001")
        with pytest.raises(RuntimeError, match="Daily limit reached"):
            scraper.fetch("case_002")


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_dict(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir, daily_limit=100)
        scraper.search("q")
        status = scraper.status()
        assert status["adapter"] == "mock"
        assert status["requests_today"] == 1
        assert status["daily_limit"] == 100
        assert status["remaining"] == 99

    def test_status_includes_storage_stats(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        status = scraper.status()
        assert "total_cases" in status
        assert "data_dir" in status


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------


class TestContextManager:
    def test_context_manager(self, mock_adapter, tmp_data_dir):
        with Scraper(adapter=mock_adapter, data_dir=tmp_data_dir) as scraper:
            scraper.search("hello")
        # After exiting, session should be closed (no exception)

    def test_close_called(self, mock_adapter, tmp_data_dir):
        scraper = Scraper(adapter=mock_adapter, data_dir=tmp_data_dir)
        scraper.close()  # Should not raise
