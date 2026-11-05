"""Redaction for operational logs.

Logs may carry partner ids, message control ids, and reason codes. Names, raw
payloads, and common identifiers in free text are dropped or masked. The raw
object store is the place to read a payload, under a separate role.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_PHONE = re.compile(r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]*)\d{3}[-.\s]*\d{4}\b")
_PID_LINE = re.compile(r"(?im)^(PID\|).*$")

# Structured fields that must never be written to a log line.
UNSAFE_KEYS = frozenset(
    {
        "family_name",
        "given_name",
        "middle_name",
        "birth_date",
        "dob",
        "phone",
        "address",
        "address_line",
        "city",
        "state",
        "postal_code",
        "ssn",
        "raw",
        "payload",
        "sig",
        "free_text",
        "drug_description",
        "patient_name",
        "name",
        "mrn",
    }
)


def redact_text(text: str) -> str:
    text = _PID_LINE.sub(r"\1[redacted]", text)
    text = _SSN.sub("[redacted]", text)
    text = _EMAIL.sub("[redacted]", text)
    text = _PHONE.sub("[redacted]", text)
    return text


def safe_fields(fields: Mapping[str, object]) -> dict[str, object]:
    cleaned: dict[str, object] = {}
    for key, value in fields.items():
        if key.lower() in UNSAFE_KEYS:
            cleaned[key] = "[redacted]"
            continue
        if isinstance(value, str):
            cleaned[key] = redact_text(value)
        else:
            cleaned[key] = value
    return cleaned


class PhiLogger:
    """Append-only logger that runs every record through `safe_fields`."""

    def __init__(self, sink) -> None:
        self._sink = sink

    def info(self, action: str, **fields: object) -> None:
        safe = safe_fields(fields)
        parts = [action]
        for key in sorted(safe):
            parts.append(f"{key}={safe[key]}")
        self._sink(" ".join(parts))
