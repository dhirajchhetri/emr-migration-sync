"""A small HL7 v2 parser for the ADT and ORU messages this pipeline accepts.

Supported types are ADT^A01, ADT^A03, ADT^A08, and ORU^R01. Field positions
follow the v2 chapter 2 rules: for MSH, the field separator itself is MSH-1,
so MSH-n lives at index n-1 after the split.
"""

from __future__ import annotations

import re

from emr_sync.dates import parse_timestamp
from emr_sync.errors import MessageRejected
from emr_sync.models import Hl7Message, Observation, Order, Person, Visit

_SUPPORTED = {
    ("ADT", "A01"),
    ("ADT", "A03"),
    ("ADT", "A08"),
    ("ORU", "R01"),
}
_SEX = {"M": "M", "F": "F", "O": "O", "A": "O", "U": "U"}


def parse_hl7(raw: str) -> Hl7Message:
    segments = _segments(raw)
    if not segments or not segments[0].startswith("MSH"):
        raise MessageRejected("MISSING_MSH")
    header = _split(segments[0])
    if len(header) < 12:
        raise MessageRejected("MALFORMED_HL7")
    version = header[11].strip()
    if not version.startswith("2."):
        raise MessageRejected("UNSUPPORTED_VERSION")
    type_field = header[8].split("^")
    message_type = type_field[0].strip() if type_field else ""
    event = type_field[1].strip() if len(type_field) > 1 else ""
    if (message_type, event) not in _SUPPORTED:
        raise MessageRejected("UNSUPPORTED_MESSAGE_TYPE")
    control_id = header[9].strip()
    if not control_id:
        raise MessageRejected("MISSING_CONTROL_ID")

    grouped = _group(segments[1:])
    person = _person(grouped.get("PID"))
    visit = _visit(grouped.get("PV1"))
    if message_type == "ADT" and event in {"A01", "A03"} and visit is None:
        raise MessageRejected("MISSING_VISIT")
    orders = _orders(grouped["orders"]) if message_type == "ORU" else ()
    if message_type == "ORU" and not orders:
        raise MessageRejected("MISSING_ORDER")
    return Hl7Message(
        message_type=message_type,
        event=event,
        control_id=control_id,
        source_app=header[2].strip(),
        source_facility=header[3].strip(),
        recorded_at=parse_timestamp(header[6]),
        person=person,
        visit=visit,
        orders=orders,
        version=version,
    )


def _segments(raw: str) -> list[str]:
    text = raw.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")
    return [line.strip() for line in text.split("\n") if line.strip()]


def _split(segment: str) -> list[str]:
    return segment.split("|")


def _group(segments: list[str]) -> dict:
    found: dict = {"orders": []}
    current_order: dict | None = None
    for segment in segments:
        name = segment[:3]
        fields = _split(segment)
        if name == "OBR":
            current_order = {"fields": fields, "observations": []}
            found["orders"].append(current_order)
            continue
        if name == "OBX":
            if current_order is None:
                current_order = {"fields": ["OBR", "1"], "observations": []}
                found["orders"].append(current_order)
            current_order["observations"].append(fields)
            continue
        if name in {"PID", "PV1", "EVN"}:
            found[name] = fields
    return found


def _component(field: str, index: int) -> str:
    parts = (field or "").split("^")
    if index < len(parts):
        return parts[index].strip()
    return ""


def _repetition(field: str) -> list[str]:
    if not field:
        return []
    return [part for part in field.split("~") if part != ""]


def _person(fields: list[str] | None) -> Person:
    if not fields:
        raise MessageRejected("MISSING_PID")
    identifier = _preferred_identifier(_field(fields, 3))
    mrn = _component(identifier, 0)
    if not mrn:
        raise MessageRejected("MISSING_PATIENT_ID")
    name = _field(fields, 5)
    family_name = _component(name, 0)
    given_name = _component(name, 1)
    middle_name = _component(name, 2) or None
    if not family_name or not given_name:
        raise MessageRejected("MISSING_PATIENT_NAME")
    dob_text = _field(fields, 7)
    if not dob_text:
        raise MessageRejected("MISSING_DOB")
    birth_date = parse_timestamp(dob_text)
    if birth_date is None:
        raise MessageRejected("INVALID_DOB")
    sex_text = _field(fields, 8).upper()
    if sex_text not in _SEX:
        raise MessageRejected("INVALID_SEX")
    address = _field(fields, 11)
    return Person(
        mrn=mrn,
        family_name=family_name,
        given_name=given_name,
        middle_name=middle_name,
        sex=_SEX[sex_text],
        birth_date=birth_date.date(),
        phone=_phone(_field(fields, 13)),
        address_line=_component(address, 0) or None,
        city=_component(address, 2) or None,
        state=_component(address, 3) or None,
        postal_code=_component(address, 4) or None,
    )


def _preferred_identifier(field: str) -> str:
    repetitions = _repetition(field)
    if not repetitions:
        return ""
    for repetition in repetitions:
        if _component(repetition, 4).upper() == "MR":
            return repetition
    return repetitions[0]


def _phone(field: str) -> str | None:
    repetitions = _repetition(field)
    if not repetitions:
        return None
    digits = re.sub(r"\D", "", _component(repetitions[0], 0))
    if len(digits) < 7:
        return None
    return digits


def _visit(fields: list[str] | None) -> Visit | None:
    if not fields:
        return None
    patient_class = _field(fields, 2).upper() or "U"
    visit_number = _component(_field(fields, 19), 0)
    if not visit_number:
        raise MessageRejected("MISSING_VISIT_NUMBER")
    location = _field(fields, 3)
    point_of_care = _component(location, 0)
    room = _component(location, 1)
    location_text = "^".join(part for part in (point_of_care, room) if part) or None
    attending = _field(fields, 7)
    attending_text = None
    if _component(attending, 1) or _component(attending, 2):
        attending_text = ", ".join(
            part for part in (_component(attending, 1), _component(attending, 2)) if part
        )
    return Visit(
        patient_class=patient_class,
        visit_number=visit_number,
        location=location_text,
        attending=attending_text,
        admit_at=parse_timestamp(_field(fields, 44)),
        discharge_at=parse_timestamp(_field(fields, 45)),
    )


def _orders(raw_orders: list[dict]) -> tuple[Order, ...]:
    orders: list[Order] = []
    for raw in raw_orders:
        fields = raw["fields"]
        service = _field(fields, 4)
        observations = tuple(_observation(item) for item in raw["observations"])
        if not observations:
            raise MessageRejected("MISSING_OBSERVATION")
        orders.append(
            Order(
                set_id=_field(fields, 1) or "1",
                service_code=_component(service, 0),
                service_text=_component(service, 1) or _component(service, 0),
                service_system=_component(service, 2),
                observations=observations,
            )
        )
    return tuple(orders)


def _observation(fields: list[str]) -> Observation:
    identifier = _field(fields, 3)
    value = _field(fields, 5)
    if value == "":
        raise MessageRejected("MISSING_OBSERVATION_VALUE")
    return Observation(
        set_id=_field(fields, 1) or "1",
        value_type=_field(fields, 2).upper() or "ST",
        code=_component(identifier, 0),
        code_text=_component(identifier, 1) or _component(identifier, 0),
        code_system=_component(identifier, 2),
        value=value,
        units=_component(_field(fields, 6), 0) or None,
        status=_field(fields, 11).upper() or None,
    )


def _field(fields: list[str], number: int) -> str:
    """Return the 1-based HL7 field for a normal segment (not the MSH shift)."""
    if number < len(fields):
        return fields[number]
    return ""
