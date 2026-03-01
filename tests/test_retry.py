"""
Unit tests for legal_scraper.core.retry.

Covers RetryPolicy, CircuitBreaker, and with_retry() decorator.
All time.sleep / time.monotonic calls are patched so tests run instantly.
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, call, patch

import pytest

from legal_scraper.core.retry import (
    AGGRESSIVE_POLICY,
    CONSERVATIVE_POLICY,
    DEFAULT_CIRCUIT_BREAKER,
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    RetryExhausted,
    RetryPolicy,
    with_retry,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TransientError(Exception):
    """Simulates a transient network error."""


class PermanentError(Exception):
    """Simulates a non-retryable error."""


def make_flaky(failures: int, *, exc_type: type = TransientError):
    """Return a callable that raises *exc_type* for the first *failures* calls."""
    call_count = 0

    def flaky(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        if call_count <= failures:
            raise exc_type(f"Failure {call_count}")
        return f"ok-{call_count}"

    return flaky


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------


class TestRetryPolicyValidation:
    """Constructor validation."""

    def test_defaults(self):
        p = RetryPolicy()
        assert p.max_attempts == 3
        assert p.base_delay == 1.0
        assert p.max_delay == 60.0
        assert p.backoff_multiplier == 2.0
        assert p.jitter is True

    def test_max_attempts_zero_raises(self):
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_negative_base_delay_raises(self):
        with pytest.raises(ValueError, match="base_delay"):
            RetryPolicy(base_delay=-1.0)

    def test_max_delay_less_than_base_raises(self):
        with pytest.raises(ValueError, match="max_delay"):
            RetryPolicy(base_delay=10.0, max_delay=5.0)

    def test_backoff_multiplier_below_one_raises(self):
        with pytest.raises(ValueError, match="backoff_multiplier"):
            RetryPolicy(backoff_multiplier=0.5)


class TestRetryPolicyDelays:
    """delay_for() calculation."""

    def test_initial_attempt_no_delay(self):
        p = RetryPolicy(base_delay=2.0, jitter=False)
        assert p.delay_for(0) == 0.0

    def test_first_retry_base_delay(self):
        p = RetryPolicy(base_delay=2.0, backoff_multiplier=2.0, jitter=False)
        assert p.delay_for(1) == pytest.approx(2.0)

    def test_second_retry_doubles(self):
        p = RetryPolicy(base_delay=2.0, backoff_multiplier=2.0, jitter=False)
        assert p.delay_for(2) == pytest.approx(4.0)

    def test_third_retry_quadruples(self):
        p = RetryPolicy(base_delay=2.0, backoff_multiplier=2.0, jitter=False)
        assert p.delay_for(3) == pytest.approx(8.0)

    def test_delay_capped_at_max(self):
        p = RetryPolicy(
            base_delay=10.0,
            max_delay=15.0,
            backoff_multiplier=10.0,
            jitter=False,
        )
        # base * 10^4 = 100_000 — should be capped at 15
        assert p.delay_for(5) == pytest.approx(15.0)

    def test_jitter_adds_random_noise(self):
        """Jitter should cause slight variation in returned delays."""
        p = RetryPolicy(base_delay=1.0, jitter=True, jitter_max=0.5)
        delays = {p.delay_for(1) for _ in range(20)}
        # With jitter, repeated calls should produce different values
        assert len(delays) > 1

    def test_jitter_stays_within_bounds(self):
        p = RetryPolicy(base_delay=1.0, max_delay=10.0, jitter=True, jitter_max=0.5)
        for _ in range(50):
            d = p.delay_for(1)
            assert 1.0 <= d <= 1.5 + 1e-9

    def test_custom_multiplier(self):
        p = RetryPolicy(base_delay=1.0, backoff_multiplier=3.0, jitter=False)
        assert p.delay_for(1) == pytest.approx(1.0)
        assert p.delay_for(2) == pytest.approx(3.0)
        assert p.delay_for(3) == pytest.approx(9.0)


class TestRetryPolicyIsRetryable:
    def test_default_retries_any_exception(self):
        p = RetryPolicy()
        assert p.is_retryable(ValueError("x"))
        assert p.is_retryable(RuntimeError("x"))
        assert p.is_retryable(TransientError("x"))

    def test_specific_exception_filter(self):
        p = RetryPolicy(retryable_exceptions=(TransientError,))
        assert p.is_retryable(TransientError("x"))
        assert not p.is_retryable(PermanentError("x"))
        assert not p.is_retryable(ValueError("x"))

    def test_multiple_exception_types(self):
        p = RetryPolicy(retryable_exceptions=(TransientError, ConnectionError))
        assert p.is_retryable(TransientError("x"))
        assert p.is_retryable(ConnectionError("x"))
        assert not p.is_retryable(ValueError("x"))


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


class TestCircuitBreakerValidation:
    def test_invalid_failure_threshold_raises(self):
        with pytest.raises(ValueError, match="failure_threshold"):
            CircuitBreaker(failure_threshold=0)

    def test_invalid_recovery_timeout_raises(self):
        with pytest.raises(ValueError, match="recovery_timeout"):
            CircuitBreaker(recovery_timeout=-1.0)

    def test_defaults(self):
        cb = CircuitBreaker()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0


class TestCircuitBreakerStateMachine:
    """Core state transitions."""

    def test_starts_closed(self):
        cb = CircuitBreaker(failure_threshold=3)
        assert cb.state == CircuitState.CLOSED

    def test_stays_closed_on_success(self):
        cb = CircuitBreaker(failure_threshold=3)
        cb.call(lambda: "ok")
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_increments_failure_count(self):
        cb = CircuitBreaker(failure_threshold=5)
        for _i in range(3):
            with pytest.raises(TransientError):
                cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.failure_count == 3
        assert cb.state == CircuitState.CLOSED

    def test_trips_open_at_threshold(self):
        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            with pytest.raises(TransientError):
                cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.state == CircuitState.OPEN

    def test_open_blocks_calls(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=9999.0)
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.state == CircuitState.OPEN
        with pytest.raises(CircuitOpenError):
            cb.call(lambda: "should not be called")

    def test_transitions_to_half_open_after_timeout(self, monkeypatch):
        """After recovery_timeout elapses, state should transition to HALF_OPEN."""
        now = time.monotonic()
        monkeypatch.setattr("time.monotonic", lambda: now)

        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0)
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))

        assert cb.state == CircuitState.OPEN

        # Fast-forward past recovery_timeout
        monkeypatch.setattr("time.monotonic", lambda: now + 31.0)
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self, monkeypatch):
        """A successful probe call should close the circuit."""
        now = time.monotonic()
        monkeypatch.setattr("time.monotonic", lambda: now)

        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0)
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))

        monkeypatch.setattr("time.monotonic", lambda: now + 11.0)
        cb.call(lambda: "probe-ok")
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_half_open_failure_reopens(self, monkeypatch):
        """A failing probe call should re-open the circuit."""
        now = time.monotonic()
        monkeypatch.setattr("time.monotonic", lambda: now)

        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0)
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))

        monkeypatch.setattr("time.monotonic", lambda: now + 11.0)
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.state == CircuitState.OPEN

    def test_success_resets_failure_count(self):
        cb = CircuitBreaker(failure_threshold=5)
        for _ in range(3):
            with pytest.raises(TransientError):
                cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.failure_count == 3
        cb.call(lambda: "ok")
        assert cb.failure_count == 0

    def test_manual_reset(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=9999.0)
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0

    def test_manual_trip(self):
        cb = CircuitBreaker(failure_threshold=100)
        assert cb.state == CircuitState.CLOSED
        cb.trip()
        assert cb.state == CircuitState.OPEN

    def test_unexpected_exception_not_counted(self):
        """Exceptions not in expected_exceptions should propagate but not count."""
        cb = CircuitBreaker(failure_threshold=2, expected_exceptions=(TransientError,))
        with pytest.raises(PermanentError):
            cb.call(lambda: (_ for _ in ()).throw(PermanentError("e")))
        # PermanentError is not in expected_exceptions → failure_count stays 0
        assert cb.failure_count == 0
        assert cb.state == CircuitState.CLOSED

    def test_repr(self):
        cb = CircuitBreaker(name="test-cb", failure_threshold=5)
        r = repr(cb)
        assert "test-cb" in r
        assert "closed" in r
        assert "0/5" in r


# ---------------------------------------------------------------------------
# with_retry decorator
# ---------------------------------------------------------------------------


class TestWithRetryBasic:
    """Basic decorator behaviour."""

    def test_success_on_first_attempt(self):
        policy = RetryPolicy(max_attempts=3, jitter=False)
        mock_sleep = MagicMock()
        with patch("time.sleep", mock_sleep):
            decorated = with_retry(policy=policy)(lambda: "hello")
            result = decorated()
        assert result == "hello"
        mock_sleep.assert_not_called()

    def test_retries_on_transient_failure(self):
        policy = RetryPolicy(max_attempts=3, base_delay=0, jitter=False)
        flaky = make_flaky(2)
        decorated = with_retry(policy=policy)(flaky)
        with patch("time.sleep"):
            result = decorated()
        assert result == "ok-3"

    def test_exhaust_raises_retry_exhausted(self):
        policy = RetryPolicy(max_attempts=3, base_delay=0, jitter=False)
        always_fail = make_flaky(999)
        decorated = with_retry(policy=policy)(always_fail)
        with patch("time.sleep"), pytest.raises(RetryExhausted) as exc_info:
            decorated()
        assert exc_info.value.attempts == 3
        assert isinstance(exc_info.value.last_error, TransientError)

    def test_non_retryable_exception_propagates_immediately(self):
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0,
            jitter=False,
            retryable_exceptions=(TransientError,),
        )
        fn = MagicMock(side_effect=PermanentError("no retry"))
        decorated = with_retry(policy=policy)(fn)
        with patch("time.sleep"), pytest.raises(PermanentError):
            decorated()
        # Should only be called once (no retry)
        fn.assert_called_once()

    def test_reraise_false_returns_none(self):
        policy = RetryPolicy(max_attempts=2, base_delay=0, jitter=False, reraise_on_exhaust=False)
        always_fail = make_flaky(999)
        decorated = with_retry(policy=policy)(always_fail)
        with patch("time.sleep"):
            result = decorated()
        assert result is None

    def test_sleep_called_with_correct_delays(self):
        """with_retry should call time.sleep with the policy delays."""
        policy = RetryPolicy(
            max_attempts=3,
            base_delay=2.0,
            backoff_multiplier=2.0,
            jitter=False,
        )
        always_fail = make_flaky(999)
        decorated = with_retry(policy=policy)(always_fail)
        mock_sleep = MagicMock()
        with patch("time.sleep", mock_sleep), pytest.raises(RetryExhausted):
            decorated()
        # attempt 0 → no sleep; attempt 1 → 2.0s; attempt 2 → 4.0s
        assert mock_sleep.call_count == 2
        assert mock_sleep.call_args_list[0] == call(pytest.approx(2.0))
        assert mock_sleep.call_args_list[1] == call(pytest.approx(4.0))

    def test_wraps_preserves_function_metadata(self):
        def my_func():
            """My docstring."""

        decorated = with_retry()(my_func)
        assert decorated.__name__ == "my_func"
        assert decorated.__doc__ == "My docstring."

    def test_default_policy_applied_when_none(self):
        """Calling with_retry() without arguments should use RetryPolicy()."""
        fn = MagicMock(return_value="ok")
        decorated = with_retry()(fn)
        with patch("time.sleep"):
            result = decorated()
        assert result == "ok"

    def test_passes_args_and_kwargs(self):
        def add(a, b, *, factor=1):
            return (a + b) * factor

        decorated = with_retry()(add)
        with patch("time.sleep"):
            assert decorated(2, 3, factor=4) == 20


class TestWithRetryAndCircuitBreaker:
    """Interaction between retry policy and circuit breaker."""

    def test_circuit_open_propagates_without_retry(self):
        """When circuit is open, CircuitOpenError should bypass retry logic."""
        policy = RetryPolicy(max_attempts=5, base_delay=0, jitter=False)
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=9999.0)

        # Trip the breaker
        with pytest.raises(TransientError):
            cb.call(lambda: (_ for _ in ()).throw(TransientError("e")))
        assert cb.state == CircuitState.OPEN

        call_count = 0

        @with_retry(policy=policy, circuit_breaker=cb)
        def fetch():
            nonlocal call_count
            call_count += 1
            return "data"

        with patch("time.sleep"), pytest.raises(CircuitOpenError):
            fetch()

        # Should have attempted exactly zero user-function calls (breaker blocked)
        assert call_count == 0

    def test_retry_trips_circuit_breaker(self):
        """Repeated failures should both trigger retries AND trip the breaker."""
        policy = RetryPolicy(max_attempts=5, base_delay=0, jitter=False)
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=9999.0)

        @with_retry(policy=policy, circuit_breaker=cb)
        def fetch():
            raise TransientError("oops")

        with patch("time.sleep"), pytest.raises((RetryExhausted, CircuitOpenError)):
            fetch()

        # After enough failures the breaker should be open
        assert cb.state == CircuitState.OPEN

    def test_successful_call_closes_circuit(self, monkeypatch):
        """A probe success after timeout should close the breaker."""
        now = time.monotonic()
        monkeypatch.setattr("time.monotonic", lambda: now)

        policy = RetryPolicy(max_attempts=2, base_delay=0, jitter=False)
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0)

        @with_retry(policy=policy, circuit_breaker=cb)
        def fetch():
            raise TransientError("fail")

        with patch("time.sleep"), pytest.raises((RetryExhausted, CircuitOpenError)):
            fetch()

        assert cb.state == CircuitState.OPEN

        # Fast-forward; next call should be a probe
        monkeypatch.setattr("time.monotonic", lambda: now + 11.0)
        assert cb.state == CircuitState.HALF_OPEN

        @with_retry(policy=policy, circuit_breaker=cb)
        def fetch_ok():
            return "ok"

        with patch("time.sleep"):
            result = fetch_ok()
        assert result == "ok"
        assert cb.state == CircuitState.CLOSED


# ---------------------------------------------------------------------------
# Preset objects
# ---------------------------------------------------------------------------


class TestPresets:
    def test_conservative_policy_valid(self):
        p = CONSERVATIVE_POLICY
        assert p.max_attempts == 3
        assert p.base_delay >= 5.0
        assert p.backoff_multiplier >= 2.0

    def test_aggressive_policy_valid(self):
        p = AGGRESSIVE_POLICY
        assert p.max_attempts >= 4
        assert p.base_delay < 2.0

    def test_default_circuit_breaker_valid(self):
        cb = DEFAULT_CIRCUIT_BREAKER
        assert cb.name == "legal-scraper"
        assert cb.failure_threshold >= 3
        assert cb.recovery_timeout >= 30.0


# ---------------------------------------------------------------------------
# RetryExhausted and CircuitOpenError
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_retry_exhausted_message(self):
        exc = RetryExhausted(3, ValueError("root cause"))
        assert "3" in str(exc)
        assert "root cause" in str(exc)
        assert exc.attempts == 3

    def test_circuit_open_error_message(self):
        exc = CircuitOpenError("my-circuit", 42.5)
        assert "my-circuit" in str(exc)
        assert "42.5" in str(exc)
        assert exc.name == "my-circuit"
        assert exc.retry_after == pytest.approx(42.5)
