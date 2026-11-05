"""Append-only audit events.

An event records what happened to a message or a resource. It does not record
the payload, a name, a date of birth, or an address. Operators who need the
payload read the raw zone with the integration role.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class AuditEvent:
    event_id: str
    recorded_at: str
    action: str
    partner_id: str
    source_system: str
    message_control_id: str | None
    outcome: str
    reason_code: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)


class AuditLog:
    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    def record(self, event: AuditEvent) -> None:
        self.events.append(event)

    def as_dicts(self) -> list[dict[str, str | None]]:
        return [event.as_dict() for event in self.events]
