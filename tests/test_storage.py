"""
Unit tests for the Storage module.

Covers save/load, dual-format output, progress tracking,
resume support, and edge cases.
"""

import json
from pathlib import Path

import pytest

from legal_scraper.core.storage import Storage


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestStorageInit:
    def test_creates_directories(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        assert Path(storage.cases_dir).exists()
        assert Path(storage.jsonl_dir).exists()

    def test_default_progress(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        assert storage.progress["total_count"] == 0
        assert storage.progress["fetched_ids"] == []
        assert "created" in storage.progress

    def test_loads_existing_progress(self, tmp_data_dir):
        # Create storage, save a case, then re-open
        s1 = Storage(tmp_data_dir)
        s1.save_case({"id": "test_001", "title": "Test"})
        s2 = Storage(tmp_data_dir)
        assert "test_001" in s2.progress["fetched_ids"]
        assert s2.progress["total_count"] == 1


# ---------------------------------------------------------------------------
# Save
# ---------------------------------------------------------------------------


class TestSaveCase:
    def test_save_creates_json_file(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        result = storage.save_case(sample_case)
        assert result is True
        json_path = storage.cases_dir / f"{sample_case['id']}.json"
        assert json_path.exists()

    def test_save_creates_jsonl_entry(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case)
        year = sample_case.get("year", "unknown")
        jsonl_path = storage.jsonl_dir / f"cases_{year}.jsonl"
        assert jsonl_path.exists()
        lines = jsonl_path.read_text(encoding="utf-8").strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["id"] == sample_case["id"]

    def test_save_json_only(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case, format="json")
        json_path = storage.cases_dir / f"{sample_case['id']}.json"
        assert json_path.exists()
        # JSONL should not exist (no prior writes)
        jsonl_files = list(storage.jsonl_dir.glob("*.jsonl"))
        assert len(jsonl_files) == 0

    def test_save_jsonl_only(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case, format="jsonl")
        json_path = storage.cases_dir / f"{sample_case['id']}.json"
        assert not json_path.exists()
        jsonl_files = list(storage.jsonl_dir.glob("*.jsonl"))
        assert len(jsonl_files) == 1

    def test_save_adds_scraped_timestamp(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case)
        loaded = storage.load_case(sample_case["id"])
        assert "_scraped_at" in loaded

    def test_save_missing_id_returns_false(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        result = storage.save_case({"title": "No ID"})
        assert result is False

    def test_save_updates_progress(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case)
        assert sample_case["id"] in storage.progress["fetched_ids"]
        assert storage.progress["total_count"] == 1

    def test_save_duplicate_does_not_double_count(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case)
        storage.save_case(sample_case)
        assert storage.progress["fetched_ids"].count(sample_case["id"]) == 1
        assert storage.progress["total_count"] == 1

    def test_save_case_without_year_uses_unknown(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        storage.save_case({"id": "no_year", "title": "No Year"})
        jsonl_path = storage.jsonl_dir / "cases_unknown.jsonl"
        assert jsonl_path.exists()

    def test_save_preserves_unicode(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        case = {"id": "uni_001", "title": "Ünïcödé Tëst — «quotes»", "year": 2024}
        storage.save_case(case)
        loaded = storage.load_case("uni_001")
        assert loaded["title"] == case["title"]


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------


class TestLoadCase:
    def test_load_existing_case(self, populated_storage):
        case = populated_storage.load_case("case_002")
        assert case is not None
        assert case["id"] == "case_002"

    def test_load_nonexistent_case(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        assert storage.load_case("nonexistent") is None


# ---------------------------------------------------------------------------
# Progress / resume
# ---------------------------------------------------------------------------


class TestProgress:
    def test_is_fetched(self, populated_storage):
        assert populated_storage.is_fetched("case_001") is True
        assert populated_storage.is_fetched("case_999") is False

    def test_get_all_ids(self, populated_storage):
        ids = populated_storage.get_all_ids()
        assert "case_001" in ids
        assert "case_002" in ids
        assert "case_003" in ids

    def test_progress_persists_to_disk(self, tmp_data_dir, sample_case):
        storage = Storage(tmp_data_dir)
        storage.save_case(sample_case)
        # Read the progress file directly
        progress_path = Path(tmp_data_dir) / "progress.json"
        assert progress_path.exists()
        raw = json.loads(progress_path.read_text())
        assert sample_case["id"] in raw["fetched_ids"]
        assert "updated" in raw


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


class TestStats:
    def test_get_stats_empty(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        stats = storage.get_stats()
        assert stats["total_cases"] == 0
        assert stats["jsonl_files"] == 0

    def test_get_stats_populated(self, populated_storage):
        stats = populated_storage.get_stats()
        assert stats["total_cases"] == 3
        assert stats["jsonl_files"] >= 1
        assert stats["progress_tracked"] == 3

    def test_data_dir_in_stats(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        stats = storage.get_stats()
        assert "data_dir" in stats


# ---------------------------------------------------------------------------
# JSONL grouping
# ---------------------------------------------------------------------------


class TestJsonlGrouping:
    def test_cases_grouped_by_year(self, tmp_data_dir):
        storage = Storage(tmp_data_dir)
        storage.save_case({"id": "a", "year": 2023, "title": "A"})
        storage.save_case({"id": "b", "year": 2024, "title": "B"})
        storage.save_case({"id": "c", "year": 2024, "title": "C"})

        assert (storage.jsonl_dir / "cases_2023.jsonl").exists()
        assert (storage.jsonl_dir / "cases_2024.jsonl").exists()

        lines_2024 = (storage.jsonl_dir / "cases_2024.jsonl").read_text().strip().split("\n")
        assert len(lines_2024) == 2
