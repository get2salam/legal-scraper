"""
Retry policy and circuit breaker for resilient HTTP scraping.

Provides three composable primitives:

    RetryPolicy   — configures exponential-backoff retry behaviour
    CircuitBreaker — tracks consecutive failures; trips open to halt
                     a failing service until it recovers
    with_retry()  — wraps any callable with retry + optional circuit-breaker

Typical usage::

    from legal_scraper.core.retry import RetryPolicy, CircuitBreaker, with_retry

    policy  = RetryPolicy(max_attempts=4, base_delay=1.0, max_delay=30.0)
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0)

    # Decorate a function
    @with_retry(policy=policy, circuit_breaker=breaker)
    def fetch_page(url):
        ...

    # Or wrap an existing callable
    safe_fetch = with_retry(policy=policy)(requests.get)
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class RetryExhausted(RuntimeError):  # noqa: N818
    """Raised when all retry attempts have been consumed."""

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"All {attempts} retry attempt(s) exhausted. Last error: {last_error!r}")


class CircuitOpenError(RuntimeError):
    """Raised when the circuit breaker is open and calls are blocked."""

    def __init__(self, name: str, retry_after: float) -> None:
        self.name = name
        self.retry_after = retry_after
        super().__init__(f"Circuit '{name}' is OPEN. Retry after {retry_after:.1f}s.")


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------


@dataclass
class RetryPolicy:
    """
    Configures exponential-backoff retry behaviour.

    Args:
        max_attempts: Total number of attempts (first try + retries).
                      E.g. max_attempts=3 means 1 initial call + 2 retries.
        base_delay:   Seconds to wait after the first failure.
        max_delay:    Upper cap on inter-attempt delay (seconds).
        backoff_multiplier: Factor applied to delay on each successive retry.
        jitter:       If True, adds uniform random noise up to *jitter_max*
                      seconds to avoid thundering-herd problems.
        jitter_max:   Maximum extra seconds added when *jitter* is True.
        retryable_exceptions: Exception types that trigger a retry.
                      Defaults to (Exception,), meaning all exceptions.
        reraise_on_exhaust: If True (default), raise RetryExhausted after
                      all attempts fail. If False, return None.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    jitter_max: float = 0.5
    retryable_exceptions: tuple[type[BaseException], ...] = field(
        default_factory=lambda: (Exception,)
    )
    reraise_on_exhaust: bool = True

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be >= base_delay")
        if self.backoff_multiplier < 1.0:
            raise ValueError("backoff_multiplier must be >= 1.0")

    def delay_for(self, attempt: int) -> float:
        """
        Compute sleep duration before *attempt* (0-indexed).

        Attempt 0 is the initial call — no pre-delay.
        Attempt 1 is the first retry — delay = base_delay * backoff^0.
        Attempt k is the k-th retry — delay = base_delay * backoff^(k-1).

        Args:
            attempt: 0-based attempt index.

        Returns:
            Seconds to sleep before this attempt.
        """
        if attempt == 0:
            return 0.0
        exponent = attempt - 1
        delay = min(self.base_delay * (self.backoff_multiplier**exponent), self.max_delay)
        if self.jitter:
            delay += random.uniform(0, self.jitter_max)
        return delay

    def is_retryable(self, exc: BaseException) -> bool:
        """Return True if *exc* should trigger a retry attempt."""
        return isinstance(exc, self.retryable_exceptions)


# ---------------------------------------------------------------------------
# CircuitBreaker
# ---------------------------------------------------------------------------


class CircuitState(str, Enum):
    """States of a circuit breaker."""

    CLOSED = "closed"  # Normal operation; failures are counted.
    OPEN = "open"  # Failing; all calls are blocked immediately.
    HALF_OPEN = "half_open"  # One probe call allowed to test recovery.


class CircuitBreaker:
    """
    Prevents cascading failures by halting calls to a consistently failing service.

    State machine::

        CLOSED ──(failure_threshold exceeded)──► OPEN
        OPEN   ──(recovery_timeout elapsed)───► HALF_OPEN
        HALF_OPEN ──(success)──────────────────► CLOSED
        HALF_OPEN ──(failure)──────────────────► OPEN

    Args:
        name:              Human-readable name for logging.
        failure_threshold: Consecutive failures required to trip the breaker.
        recovery_timeout:  Seconds to wait in OPEN state before probing.
        expected_exceptions: Exceptions that count as failures.
                             Defaults to (Exception,).
    """

    def __init__(
        self,
        name: str = "default",
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exceptions: Sequence[type[BaseException]] = (Exception,),
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be > 0")

        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions: tuple[type[BaseException], ...] = tuple(expected_exceptions)

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None
        self._success_count = 0  # lifetime successes (informational)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def state(self) -> CircuitState:
        """Current circuit state (may auto-transition OPEN → HALF_OPEN)."""
        self._maybe_attempt_recovery()
        return self._state

    @property
    def failure_count(self) -> int:
        """Consecutive failure count (resets on success or recovery)."""
        return self._failure_count

    def call(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """
        Invoke *fn* through the circuit breaker.

        Raises:
            CircuitOpenError: If the circuit is open and not yet recoverable.
        """
        self._maybe_attempt_recovery()

        if self._state == CircuitState.OPEN:
            assert self._opened_at is not None
            retry_after = self._opened_at + self.recovery_timeout - time.monotonic()
            raise CircuitOpenError(self.name, max(retry_after, 0.0))

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except BaseException as exc:
            if isinstance(exc, tuple(self.expected_exceptions)):
                self._on_failure()
            raise

    def reset(self) -> None:
        """Manually reset the breaker to CLOSED state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = None
        logger.info("Circuit '%s' manually reset to CLOSED.", self.name)

    def trip(self) -> None:
        """Manually force the breaker to OPEN state."""
        self._trip()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _on_success(self) -> None:
        """Record a successful call."""
        self._success_count += 1
        if self._state in (CircuitState.HALF_OPEN, CircuitState.OPEN):
            logger.info("Circuit '%s' → CLOSED after successful probe.", self.name)
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at = None

    def _on_failure(self) -> None:
        """Record a failed call and trip the breaker if threshold is hit."""
        self._failure_count += 1
        if self._state == CircuitState.HALF_OPEN:
            logger.warning("Circuit '%s': probe failed → re-opening.", self.name)
            self._trip()
        elif self._state == CircuitState.CLOSED and self._failure_count >= self.failure_threshold:
            logger.warning(
                "Circuit '%s': %d consecutive failures → OPEN.",
                self.name,
                self._failure_count,
            )
            self._trip()

    def _trip(self) -> None:
        """Transition to OPEN state."""
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()

    def _maybe_attempt_recovery(self) -> None:
        """Auto-transition OPEN → HALF_OPEN when recovery_timeout has elapsed."""
        if (
            self._state == CircuitState.OPEN
            and self._opened_at is not None
            and time.monotonic() - self._opened_at >= self.recovery_timeout
        ):
            logger.info(
                "Circuit '%s' → HALF_OPEN (probing after %.1fs).",
                self.name,
                self.recovery_timeout,
            )
            self._state = CircuitState.HALF_OPEN

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(name={self.name!r}, state={self._state.value}, "
            f"failures={self._failure_count}/{self.failure_threshold})"
        )


# ---------------------------------------------------------------------------
# with_retry decorator / wrapper
# ---------------------------------------------------------------------------


def with_retry(
    policy: RetryPolicy | None = None,
    circuit_breaker: CircuitBreaker | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator factory: wrap a callable with retry and optional circuit-breaker.

    Args:
        policy:          RetryPolicy instance (defaults to RetryPolicy()).
        circuit_breaker: Optional CircuitBreaker instance.

    Returns:
        A decorator that wraps the target function.

    Example::

        policy = RetryPolicy(max_attempts=3, base_delay=0.5)

        @with_retry(policy=policy)
        def fetch(url):
            return requests.get(url)

        # Or wrap after the fact:
        safe_get = with_retry(policy=policy)(requests.get)
    """
    if policy is None:
        policy = RetryPolicy()

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None

            for attempt in range(policy.max_attempts):
                delay = policy.delay_for(attempt)
                if delay > 0:
                    logger.debug(
                        "%s: attempt %d/%d — sleeping %.2fs",
                        fn.__name__,
                        attempt + 1,
                        policy.max_attempts,
                        delay,
                    )
                    time.sleep(delay)

                try:
                    if circuit_breaker is not None:
                        return circuit_breaker.call(fn, *args, **kwargs)
                    return fn(*args, **kwargs)

                except CircuitOpenError:
                    # Don't retry when the circuit is open; propagate immediately.
                    raise

                except BaseException as exc:
                    if not policy.is_retryable(exc):
                        raise
                    last_exc = exc
                    remaining = policy.max_attempts - attempt - 1
                    logger.warning(
                        "%s: attempt %d/%d failed (%s). %d remaining.",
                        fn.__name__,
                        attempt + 1,
                        policy.max_attempts,
                        type(exc).__name__,
                        remaining,
                    )

            # All attempts exhausted
            assert last_exc is not None
            if policy.reraise_on_exhaust:
                raise RetryExhausted(policy.max_attempts, last_exc) from last_exc
            return None

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Convenience presets
# ---------------------------------------------------------------------------

#: Conservative policy — suitable for rate-limited sources.
CONSERVATIVE_POLICY = RetryPolicy(
    max_attempts=3,
    base_delay=5.0,
    max_delay=120.0,
    backoff_multiplier=3.0,
    jitter=True,
)

#: Aggressive policy — fast retries for short-lived transient errors.
AGGRESSIVE_POLICY = RetryPolicy(
    max_attempts=5,
    base_delay=0.5,
    max_delay=10.0,
    backoff_multiplier=2.0,
    jitter=True,
)

#: Standard circuit breaker — trips after 5 failures, recovers in 60s.
DEFAULT_CIRCUIT_BREAKER = CircuitBreaker(
    name="legal-scraper",
    failure_threshold=5,
    recovery_timeout=60.0,
)
