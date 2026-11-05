"""Stable rejection reasons for poison messages."""

from __future__ import annotations


class MessageRejected(Exception):
    """The payload cannot be applied. `reason` is a stable code, never a patient value."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class TransientFailure(Exception):
    """A dependency failed in a way that is worth retrying."""
