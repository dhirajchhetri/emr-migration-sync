"""Date parsing shared by HL7 timestamps, SCRIPT dates, and CSV extracts."""

from __future__ import annotations

import re
from datetime import date, datetime

_ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?)?")


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO-8601 date or an HL7 TS value. Return None when it is not a real date."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    iso = _ISO.match(text)
    if iso:
        year, month, day, hour, minute, second = iso.groups()
        try:
            return datetime(
                int(year),
                int(month),
                int(day),
                int(hour or 0),
                int(minute or 0),
                int(second or 0),
            )
        except ValueError:
            return None
    digits = re.sub(r"\D", "", text)
    if len(digits) < 8:
        return None
    try:
        if len(digits) >= 14:
            return datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
        if len(digits) >= 12:
            return datetime.strptime(digits[:12], "%Y%m%d%H%M")
        return datetime.strptime(digits[:8], "%Y%m%d")
    except ValueError:
        return None


def parse_date(value: str | None) -> date | None:
    parsed = parse_timestamp(value)
    if parsed is None:
        return None
    return parsed.date()


def to_fhir_date(value: date) -> str:
    return value.isoformat()


def to_fhir_datetime(value: datetime) -> str:
    """Render a naive timestamp as UTC. Partner offsets are not modeled."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")
