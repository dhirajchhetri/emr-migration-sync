"""Map parsed partner data to a small FHIR R4 subset.

Resources are plain dicts. Only elements this pipeline reads and writes are
set: Patient, Encounter, Observation, DiagnosticReport, MedicationRequest,
MedicationStatement, and Condition.
"""

from __future__ import annotations

import copy
from datetime import datetime

from emr_sync.dates import parse_date, to_fhir_date, to_fhir_datetime
from emr_sync.errors import MessageRejected
from emr_sync.ids import resource_id
from emr_sync.models import Medication, Observation, Order, Person, Visit

LOINC = "http://loinc.org"
SNOMED = "http://snomed.info/sct"
ICD10 = "http://hl7.org/fhir/sid/icd-10-cm"
NDC = "http://hl7.org/fhir/sid/ndc"
RXNORM = "http://www.nlm.nih.gov/research/umls/rxnorm"
ACT_CODE = "http://terminology.hl7.org/CodeSystem/v3-ActCode"

_GENDER = {"M": "male", "F": "female", "O": "other", "U": "unknown"}
_SEX = {"male": "M", "female": "F", "other": "O", "unknown": "U"}
_CODE_SYSTEMS = {"LN": LOINC, "L": LOINC, "SCT": SNOMED, "I10": ICD10, "ICD10": ICD10}
_CLASS = {
    "I": ("IMP", "inpatient encounter"),
    "E": ("EMER", "emergency"),
    "O": ("AMB", "ambulatory"),
}


def mrn_system(source_system: str) -> str:
    return f"urn:mrn:{source_system}"


def build_patient(person: Person, source_system: str, partner_id: str) -> dict:
    resource: dict = {
        "resourceType": "Patient",
        "id": resource_id("pat", source_system, person.mrn),
        "active": True,
        "identifier": [{"system": mrn_system(source_system), "value": person.mrn}],
        "name": [
            {
                "family": person.family_name,
                "given": [part for part in (person.given_name, person.middle_name) if part],
            }
        ],
        "gender": _GENDER.get(person.sex, "unknown"),
    }
    if person.birth_date is not None:
        resource["birthDate"] = to_fhir_date(person.birth_date)
    if person.phone:
        resource["telecom"] = [{"system": "phone", "value": person.phone}]
    address = _address(person)
    if address:
        resource["address"] = [address]
    if partner_id:
        resource["managingOrganization"] = {"display": partner_id}
    return resource


def merge_source_identity(existing: dict, person: Person, source_system: str) -> dict:
    """Apply an update from the system that already owns this medical record number."""
    updated = copy.deepcopy(existing)
    partner = (existing.get("managingOrganization") or {}).get("display", "")
    fresh = build_patient(person, source_system, partner)
    for field in ("name", "gender", "birthDate", "telecom", "address"):
        if field in fresh:
            updated[field] = fresh[field]
        elif field in updated and field not in fresh:
            updated.pop(field)
    updated["identifier"] = _with_identifier(
        updated.get("identifier", []), source_system, person.mrn
    )
    updated["active"] = True
    return updated


def attach_identifier(existing: dict, person: Person, source_system: str) -> dict:
    """Link another source's record onto a golden patient without overwriting demographics."""
    updated = copy.deepcopy(existing)
    updated["identifier"] = _with_identifier(
        updated.get("identifier", []), source_system, person.mrn
    )
    if person.phone and not updated.get("telecom"):
        updated["telecom"] = [{"system": "phone", "value": person.phone}]
    return updated


def person_from_patient(resource: dict) -> Person:
    name = (resource.get("name") or [{}])[0]
    given = list(name.get("given") or [])
    phone = None
    for telecom in resource.get("telecom") or []:
        if telecom.get("system") == "phone":
            phone = telecom.get("value")
            break
    address = (resource.get("address") or [{}])[0]
    lines = address.get("line") or []
    return Person(
        mrn="",
        family_name=name.get("family") or "",
        given_name=given[0] if given else "",
        middle_name=given[1] if len(given) > 1 else None,
        sex=_SEX.get(resource.get("gender"), "U"),
        birth_date=parse_date(resource.get("birthDate")),
        phone=phone,
        address_line=lines[0] if lines else None,
        city=address.get("city"),
        state=address.get("state"),
        postal_code=address.get("postalCode"),
    )


def build_encounter(
    *,
    source_system: str,
    patient_id: str,
    visit: Visit,
    event: str,
    recorded_at: datetime | None,
    existing: dict | None = None,
) -> dict:
    code, display = _CLASS.get(visit.patient_class, ("AMB", "ambulatory"))
    status = _encounter_status(event, existing)
    resource: dict = {
        "resourceType": "Encounter",
        "id": resource_id("enc", source_system, visit.visit_number),
        "status": status,
        "class": {"system": ACT_CODE, "code": code, "display": display},
        "subject": {"reference": f"Patient/{patient_id}"},
        "identifier": [{"system": f"urn:visit:{source_system}", "value": visit.visit_number}],
    }
    period = _encounter_period(visit, recorded_at, existing, event)
    if period:
        resource["period"] = period
    if visit.location:
        resource["location"] = [{"location": {"display": visit.location}}]
    if visit.attending:
        resource["participant"] = [
            {"type": [{"text": "attending"}], "individual": {"display": visit.attending}}
        ]
    return resource


def build_observation(
    *,
    source_system: str,
    patient_id: str,
    control_id: str,
    order: Order,
    observation: Observation,
    recorded_at: datetime | None,
) -> dict:
    resource: dict = {
        "resourceType": "Observation",
        "id": resource_id("obs", source_system, control_id, order.set_id, observation.set_id),
        "status": "final" if observation.status in {None, "", "F"} else "preliminary",
        "code": _code(observation.code, observation.code_text, observation.code_system),
        "subject": {"reference": f"Patient/{patient_id}"},
    }
    if observation.value_type == "NM":
        try:
            number = float(observation.value)
        except ValueError as exc:
            raise MessageRejected("INVALID_OBSERVATION_VALUE") from exc
        quantity: dict = {"value": number}
        if observation.units:
            quantity["unit"] = observation.units
        resource["valueQuantity"] = quantity
    else:
        resource["valueString"] = observation.value
    if recorded_at is not None:
        resource["effectiveDateTime"] = to_fhir_datetime(recorded_at)
    return resource


def build_diagnostic_report(
    *,
    source_system: str,
    patient_id: str,
    control_id: str,
    order: Order,
    observation_ids: list[str],
    recorded_at: datetime | None,
) -> dict:
    resource: dict = {
        "resourceType": "DiagnosticReport",
        "id": resource_id("rpt", source_system, control_id, order.set_id),
        "status": "final",
        "code": _code(order.service_code, order.service_text, order.service_system),
        "subject": {"reference": f"Patient/{patient_id}"},
        "result": [{"reference": f"Observation/{item}"} for item in observation_ids],
        "identifier": [{"system": f"urn:hl7-control:{source_system}", "value": control_id}],
    }
    if recorded_at is not None:
        resource["effectiveDateTime"] = to_fhir_datetime(recorded_at)
    return resource


def build_medication_request(
    *,
    source_system: str,
    patient_id: str,
    message_id: str,
    identity_key: str,
    kind: str,
    medication: Medication,
    authored_on: datetime | None,
    prior_id: str | None = None,
) -> dict:
    status, intent = _MEDICATION_INTENT[kind]
    resource: dict = {
        "resourceType": "MedicationRequest",
        "id": resource_id("med", source_system, identity_key),
        "status": status,
        "intent": intent,
        "subject": {"reference": f"Patient/{patient_id}"},
        "medicationCodeableConcept": _medication_code(medication),
        "identifier": _medication_identifiers(source_system, message_id, medication),
    }
    if medication.sig:
        resource["dosageInstruction"] = [{"text": medication.sig}]
    if authored_on is not None:
        resource["authoredOn"] = to_fhir_datetime(authored_on)
    if prior_id:
        resource["priorPrescription"] = {"reference": f"MedicationRequest/{prior_id}"}
    return resource


def build_medication_statement(
    *,
    source_system: str,
    patient_id: str,
    message_id: str,
    medication: Medication,
    effective: datetime | None,
) -> dict:
    identity = medication.ndc or medication.rxnorm or medication.description
    resource: dict = {
        "resourceType": "MedicationStatement",
        "id": resource_id("medstmt", source_system, message_id, identity),
        "status": "active",
        "subject": {"reference": f"Patient/{patient_id}"},
        "medicationCodeableConcept": _medication_code(medication),
        "identifier": [{"system": f"urn:script-message:{source_system}", "value": message_id}],
    }
    if medication.sig:
        resource["dosage"] = [{"text": medication.sig}]
    if effective is not None:
        resource["effectiveDateTime"] = to_fhir_datetime(effective)
    return resource


def build_wound(
    *,
    source_system: str,
    patient_id: str,
    wound_id: str,
    icd10: str,
    site: str,
    acquired_on,
) -> dict:
    resource: dict = {
        "resourceType": "Condition",
        "id": resource_id("wnd", source_system, wound_id),
        "clinicalStatus": {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                    "code": "active",
                }
            ]
        },
        "code": {"coding": [{"system": ICD10, "code": icd10}], "text": site or icd10},
        "subject": {"reference": f"Patient/{patient_id}"},
        "identifier": [{"system": f"urn:wound:{source_system}", "value": wound_id}],
    }
    if site:
        resource["bodySite"] = [{"text": site}]
    if acquired_on is not None:
        resource["onsetDateTime"] = to_fhir_date(acquired_on)
    return resource


def build_wound_length(
    *,
    source_system: str,
    patient_id: str,
    wound_id: str,
    length_cm: float,
    recorded_at: datetime | None,
) -> dict:
    resource: dict = {
        "resourceType": "Observation",
        "id": resource_id("obs", source_system, wound_id, "length"),
        "status": "final",
        "code": _code("39126-2", "Wound length", "LN"),
        "subject": {"reference": f"Patient/{patient_id}"},
        "valueQuantity": {"value": length_cm, "unit": "cm"},
        "focus": [{"reference": f"Condition/{resource_id('wnd', source_system, wound_id)}"}],
    }
    if recorded_at is not None:
        resource["effectiveDateTime"] = to_fhir_datetime(recorded_at)
    return resource


_MEDICATION_INTENT = {
    "NewRx": ("active", "order"),
    "CancelRx": ("cancelled", "order"),
    "RxRenewalRequest": ("active", "proposal"),
    "RxRenewalResponse": ("active", "order"),
    "RxFill": ("completed", "order"),
}


def _encounter_status(event: str, existing: dict | None) -> str:
    if event == "A03":
        return "finished"
    if existing and existing.get("status") == "finished":
        return "finished"
    if existing and existing.get("status"):
        return existing["status"]
    return "in-progress"


def _encounter_period(
    visit: Visit,
    recorded_at: datetime | None,
    existing: dict | None,
    event: str,
) -> dict:
    existing_period = (existing or {}).get("period") or {}
    start = existing_period.get("start")
    if visit.admit_at is not None:
        start = to_fhir_datetime(visit.admit_at)
    elif start is None and recorded_at is not None:
        start = to_fhir_datetime(recorded_at)
    end = existing_period.get("end")
    if event == "A03":
        when = visit.discharge_at or recorded_at
        if when is not None:
            end = to_fhir_datetime(when)
    period = {}
    if start:
        period["start"] = start
    if end:
        period["end"] = end
    return period


def _address(person: Person) -> dict:
    address: dict = {}
    if person.address_line:
        address["line"] = [person.address_line]
    if person.city:
        address["city"] = person.city
    if person.state:
        address["state"] = person.state
    if person.postal_code:
        address["postalCode"] = person.postal_code
    return address


def _with_identifier(identifiers: list[dict], source_system: str, mrn: str) -> list[dict]:
    system = mrn_system(source_system)
    kept = [
        item
        for item in identifiers
        if not (item.get("system") == system and item.get("value") == mrn)
    ]
    kept.append({"system": system, "value": mrn})
    return kept


def _code(code: str, text: str, system: str) -> dict:
    concept: dict = {"text": text or code}
    mapped = _CODE_SYSTEMS.get(system.upper()) if system else None
    if code and mapped:
        concept["coding"] = [{"system": mapped, "code": code, "display": text or code}]
    return concept


def _medication_code(medication: Medication) -> dict:
    concept: dict = {"text": medication.description}
    coding = []
    if medication.ndc:
        coding.append({"system": NDC, "code": medication.ndc})
    if medication.rxnorm:
        coding.append({"system": RXNORM, "code": medication.rxnorm})
    if coding:
        concept["coding"] = coding
    return concept


def _medication_identifiers(
    source_system: str, message_id: str, medication: Medication
) -> list[dict]:
    identifiers = [{"system": f"urn:script-message:{source_system}", "value": message_id}]
    if medication.order_number:
        identifiers.append(
            {"system": f"urn:prescription:{source_system}", "value": medication.order_number}
        )
    return identifiers
