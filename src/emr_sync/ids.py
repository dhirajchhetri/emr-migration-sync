"""Deterministic resource ids so a replay upserts the same record."""

from __future__ import annotations

import hashlib


def resource_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
    return f"{prefix}-{digest}"


def group_key(*parts: str) -> str:
    """Ordering key for a patient. The raw identifier is not placed on the queue."""
    return "p-" + hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
