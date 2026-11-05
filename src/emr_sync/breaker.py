"""Per-partner circuit breaker.

A partner whose messages keep failing stops being queued until a cooldown
passes. Other partners are unaffected. The raw object is still stored, so a
paused partner does not lose messages.
"""

from __future__ import annotations

import time
from collections.abc import Callable


class CircuitBreaker:
    def __init__(
        self,
        threshold: int = 5,
        cooldown_seconds: float = 30.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.threshold = threshold
        self.cooldown_seconds = cooldown_seconds
        self._clock = clock or time.monotonic
        self.failures: dict[str, int] = {}
        self.opened_at: dict[str, float] = {}

    def allow(self, partner_id: str) -> bool:
        opened = self.opened_at.get(partner_id)
        if opened is None:
            return True
        if self._clock() - opened >= self.cooldown_seconds:
            self.opened_at.pop(partner_id, None)
            self.failures[partner_id] = max(self.threshold - 1, 0)
            return True
        return False

    def record_success(self, partner_id: str) -> None:
        self.failures[partner_id] = 0
        self.opened_at.pop(partner_id, None)

    def record_failure(self, partner_id: str) -> None:
        count = self.failures.get(partner_id, 0) + 1
        self.failures[partner_id] = count
        if count >= self.threshold:
            self.opened_at[partner_id] = self._clock()
