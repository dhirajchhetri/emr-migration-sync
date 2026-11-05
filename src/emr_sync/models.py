"""Parsed partner messages. These stay inside the integration boundary."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True)
class Person:
    mrn: str
    family_name: str
    given_name: str
    sex: str
    birth_date: date | None = None
    phone: str | None = None
    address_line: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    middle_name: str | None = None


@dataclass(frozen=True)
class Visit:
    patient_class: str
    visit_number: str
    location: str | None = None
    attending: str | None = None
    admit_at: datetime | None = None
    discharge_at: datetime | None = None


@dataclass(frozen=True)
class Observation:
    set_id: str
    value_type: str
    code: str
    code_text: str
    code_system: str
    value: str
    units: str | None = None
    status: str | None = None


@dataclass(frozen=True)
class Order:
    set_id: str
    service_code: str
    service_text: str
    service_system: str
    observations: tuple[Observation, ...] = ()


@dataclass(frozen=True)
class Hl7Message:
    message_type: str
    event: str
    control_id: str
    source_app: str
    source_facility: str
    recorded_at: datetime | None
    person: Person
    visit: Visit | None = None
    orders: tuple[Order, ...] = ()
    version: str = "2.5"


@dataclass(frozen=True)
class Medication:
    description: str
    ndc: str | None = None
    rxnorm: str | None = None
    sig: str | None = None
    quantity: str | None = None
    order_number: str | None = None


@dataclass(frozen=True)
class ScriptMessage:
    kind: str
    message_id: str
    sent_at: datetime | None
    person: Person
    medication: Medication | None = None
    medications: tuple[Medication, ...] = ()
    relates_to: str | None = None
