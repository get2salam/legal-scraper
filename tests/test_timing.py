"""
Unit tests for the HumanTiming module.

All tests patch time.sleep (via conftest) so they run instantly.
"""

import time
from unittest.mock import patch, call

import pytest

from legal_scraper.core.timing import HumanTiming


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


class TestTimingInit:
    def test_default_values(self):
        t = HumanTiming()
        assert t.min_delay == 5
        assert t.max_delay == 20
        assert t.reading_pause_chance == pytest.approx(0.12)
        assert t.request_count == 0

    def test_env_override(self, monkeypatch):
        monkeypatch.setenv("MIN_DELAY_SECONDS", "1")
        monkeypatch.setenv("MAX_DELAY_SECONDS", "3")
        monkeypatch.setenv("READING_PAUSE_CHANCE", "0.5")
        monkeypatch.setenv("READING_PAUSE_MIN", "10")
        monkeypatch.setenv("READING_PAUSE_MAX", "20")
        monkeypatch.setenv("BREAK_AFTER_REQUESTS_MIN", "5")
        monkeypatch.setenv("BREAK_AFTER_REQUESTS_MAX", "10")
        monkeypatch.setenv("BREAK_DURATION_MIN", "30")
        monkeypatch.setenv("BREAK_DURATION_MAX", "60")

        t = HumanTiming()
        assert t.min_delay == 1.0
        assert t.max_delay == 3.0
        assert t.reading_pause_chance == pytest.approx(0.5)
        assert t.reading_pause_min == 10.0
        assert t.reading_pause_max == 20.0
        assert t.break_min == 5
        assert t.break_max == 10
        assert t.break_duration_min == 30.0
        assert t.break_duration_max == 60.0


# ---------------------------------------------------------------------------
# Delay
# ---------------------------------------------------------------------------


class TestDelay:
    def test_delay_calls_sleep(self):
        """delay() should call time.sleep with a value in the expected range."""
        t = HumanTiming()
        sleep_values = []

        with patch("time.sleep", side_effect=lambda s: sleep_values.append(s)):
            # Force no reading pause by mocking random
            with patch("random.random", return_value=1.0):
                t.delay()

        assert len(sleep_values) == 1
        assert t.min_delay <= sleep_values[0] <= t.max_delay

    def test_reading_pause_triggered(self):
        """When random triggers reading pause, delay should be longer."""
        t = HumanTiming()
        sleep_values = []

        with patch("time.sleep", side_effect=lambda s: sleep_values.append(s)):
            # Force reading pause (random < reading_pause_chance)
            with patch("random.random", return_value=0.01):
                with patch("random.uniform", return_value=45.0):
                    t.delay()

        assert len(sleep_values) == 1
        assert sleep_values[0] == 45.0


# ---------------------------------------------------------------------------
# Break logic
# ---------------------------------------------------------------------------


class TestBreaks:
    def test_break_triggers_at_threshold(self):
        """Break should trigger when request_count reaches threshold."""
        t = HumanTiming()
        t._next_break_at = 3  # Force low threshold
        sleep_calls = []

        with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            t.maybe_break()  # count=1 → no break
            t.maybe_break()  # count=2 → no break
            t.maybe_break()  # count=3 → break!

        assert t.request_count == 3
        # The third call should have triggered a sleep for break
        assert len(sleep_calls) == 1
        assert sleep_calls[0] >= t.break_duration_min
        assert sleep_calls[0] <= t.break_duration_max

    def test_no_break_before_threshold(self):
        """No break should occur before the threshold."""
        t = HumanTiming()
        t._next_break_at = 100  # Very high threshold
        sleep_calls = []

        with patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)):
            for _ in range(5):
                t.maybe_break()

        assert len(sleep_calls) == 0

    def test_break_resets_next_threshold(self):
        """After a break, the next threshold should be recalculated."""
        t = HumanTiming()
        t._next_break_at = 1

        with patch("time.sleep"):
            t.maybe_break()  # triggers break

        # _next_break_at should now be > request_count
        assert t._next_break_at > t.request_count


# ---------------------------------------------------------------------------
# Combined wait
# ---------------------------------------------------------------------------


class TestWait:
    def test_wait_calls_delay_and_maybe_break(self):
        """wait() should call both delay() and maybe_break()."""
        t = HumanTiming()

        with patch.object(t, "delay") as mock_delay, \
             patch.object(t, "maybe_break") as mock_break:
            t.wait()
            mock_delay.assert_called_once()
            mock_break.assert_called_once()


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------


class TestReset:
    def test_reset_clears_count(self):
        t = HumanTiming()
        t.request_count = 50
        t.reset()
        assert t.request_count == 0

    def test_reset_sets_new_threshold(self):
        t = HumanTiming()
        old_threshold = t._next_break_at
        t.request_count = 100
        t.reset()
        # New threshold should be within break_min..break_max
        assert t.break_min <= t._next_break_at <= t.break_max


# ---------------------------------------------------------------------------
# Random break threshold
# ---------------------------------------------------------------------------


class TestRandomBreakThreshold:
    def test_threshold_in_range(self):
        t = HumanTiming()
        for _ in range(50):
            val = t._random_break_threshold()
            assert t.break_min <= val <= t.break_max
