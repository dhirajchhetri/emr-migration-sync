"""Parser for a subset of NCPDP SCRIPT messages.

The subset covers NewRx, CancelRx, RxRenewalRequest, RxRenewalResponse,
RxFill, and RxHistoryResponse. Element lookup uses local names, so a default
namespace does not change the result. This is not a full SCRIPT schema
validator; required clinical fields are checked and everything else is ignored.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET

from emr_sync.dates import parse_timestamp
from emr_sync.errors import MessageRejected
from emr_sync.models import Medication, Person, ScriptMessage

_KINDS = {
    "NewRx",
    "CancelRx",
    "RxRenewalRequest",
    "RxRenewalResponse",
    "RxFill",
    "RxHistoryResponse",
}
_SEX = {"M": "M", "F": "F", "O": "O", "A": "O", "U": "U"}


def parse_script(raw: str) -> ScriptMessage:
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise MessageRejected("MALFORMED_XML") from exc
    if _local(root.tag) != "Message":
        raise MessageRejected("MALFORMED_XML")
    message_id = _text(root, "MessageId")
    if not message_id:
        raise MessageRejected("MISSING_MESSAGE_ID")
    body = _first(root, "Body")
    if body is None:
        raise MessageRejected("MISSING_BODY")
    kind_element = _kind(body)
    if kind_element is None:
        raise MessageRejected("UNSUPPORTED_SCRIPT_TYPE")
    kind = _local(kind_element.tag)
    person = _person(kind_element)
    medications = _medications(kind_element)
    relates_to = _text(root, "RelatesToMessageId") or None
    if kind == "NewRx" and not medications:
        raise MessageRejected("MISSING_MEDICATION")
    if kind == "CancelRx" and not relates_to and not any(item.order_number for item in medications):
        raise MessageRejected("MISSING_PRESCRIPTION_REFERENCE")
    if kind == "RxHistoryResponse" and not medications:
        raise MessageRejected("MISSING_MEDICATION")
    return ScriptMessage(
        kind=kind,
        message_id=message_id,
        sent_at=parse_timestamp(_text(root, "SentTime")),
        person=person,
        medication=medications[0] if medications else None,
        medications=tuple(medications),
        relates_to=relates_to,
    )


def _kind(body: ET.Element) -> ET.Element | None:
    for child in list(body):
        if _local(child.tag) in _KINDS:
            return child
    return None


def _person(scope: ET.Element) -> Person:
    patient = _first(scope, "Patient")
    if patient is None:
        raise MessageRejected("MISSING_PATIENT")
    mrn = _text(patient, "MedicalRecordNumber")
    if not mrn:
        raise MessageRejected("MISSING_PATIENT_ID")
    family_name = _text(patient, "LastName")
    given_name = _text(patient, "FirstName")
    if not family_name or not given_name:
        raise MessageRejected("MISSING_PATIENT_NAME")
    dob_text = _text(patient, "DateOfBirth")
    if not dob_text:
        raise MessageRejected("MISSING_DOB")
    birth_date = parse_timestamp(dob_text)
    if birth_date is None:
        raise MessageRejected("INVALID_DOB")
    sex_text = _text(patient, "Gender").upper()
    if sex_text not in _SEX:
        raise MessageRejected("INVALID_SEX")
    return Person(
        mrn=mrn,
        family_name=family_name,
        given_name=given_name,
        sex=_SEX[sex_text],
        birth_date=birth_date.date(),
        phone=_digits(_text(patient, "Phone")) or None,
    )


def _medications(scope: ET.Element) -> list[Medication]:
    found: list[Medication] = []
    for element in scope.iter():
        if _local(element.tag) != "Medication":
            continue
        description = _text(element, "DrugDescription")
        ndc = _digits(_text(element, "NDC") or _text(element, "Ndc")) or None
        rxnorm = _text(element, "RxNorm") or None
        sig = _text(element, "Sig") or _text(element, "Directions") or None
        quantity = _text(element, "Quantity") or None
        order_number = _text(element, "PrescriberOrderNumber") or None
        if not description and not ndc and not order_number:
            continue
        if not description:
            raise MessageRejected("MISSING_MEDICATION")
        found.append(
            Medication(
                description=description,
                ndc=ndc,
                rxnorm=rxnorm,
                sig=sig,
                quantity=quantity,
                order_number=order_number,
            )
        )
    return found


def _digits(value: str) -> str:
    return "".join(character for character in value if character.isdigit())


def _text(scope: ET.Element, name: str) -> str:
    found = _first(scope, name)
    if found is None or found.text is None:
        return ""
    return found.text.strip()


def _first(scope: ET.Element, name: str) -> ET.Element | None:
    for element in scope.iter():
        if _local(element.tag) == name:
            return element
    return None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]
