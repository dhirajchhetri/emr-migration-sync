"""Anti-corruption apply step.

Partner messages become FHIR resources here. Source identity (same system and
medical record number) updates that patient. A cross-source auto match only
adds an identifier. An uncertain match creates a provisional patient and a
review row; it does not merge.
"""

from __future__ import annotations

from dataclasses import dataclass

from emr_sync.errors import MessageRejected
from emr_sync.fhir import (
    attach_identifier,
    build_diagnostic_report,
    build_encounter,
    build_medication_request,
    build_medication_statement,
    build_observation,
    build_patient,
    merge_source_identity,
    mrn_system,
    person_from_patient,
)
from emr_sync.ids import resource_id
from emr_sync.models import Hl7Message, ScriptMessage
from emr_sync.mpi import decide


@dataclass(frozen=True)
class ApplyOutcome:
    patient_id: str
    resource_ids: tuple[str, ...]
    match_decision: str
    review_id: str | None = None


def apply_hl7(documents, message: Hl7Message, source_system: str, partner_id: str) -> ApplyOutcome:
    patient_id, decision, review_id = resolve_patient(
        documents, message.person, source_system, partner_id
    )
    ids = [patient_id]
    if message.message_type == "ADT" and message.visit is not None:
        ids.append(_apply_encounter(documents, message, source_system, patient_id))
    if message.message_type == "ORU":
        ids.extend(_apply_orders(documents, message, source_system, patient_id))
    return ApplyOutcome(patient_id, tuple(ids), decision, review_id)


def apply_script(
    documents, message: ScriptMessage, source_system: str, partner_id: str
) -> ApplyOutcome:
    patient_id, decision, review_id = resolve_patient(
        documents, message.person, source_system, partner_id
    )
    ids = [patient_id]
    if message.kind == "RxHistoryResponse":
        for medication in message.medications:
            statement = build_medication_statement(
                source_system=source_system,
                patient_id=patient_id,
                message_id=message.message_id,
                medication=medication,
                effective=message.sent_at,
            )
            documents.put_resource(statement)
            ids.append(statement["id"])
        return ApplyOutcome(patient_id, tuple(ids), decision, review_id)
    if message.medication is None and message.kind != "CancelRx":
        raise MessageRejected("MISSING_MEDICATION")
    ids.append(_apply_prescription(documents, message, source_system, patient_id))
    return ApplyOutcome(patient_id, tuple(ids), decision, review_id)


def resolve_patient(documents, person, source_system: str, partner_id: str):
    existing = find_by_identifier(documents, "Patient", mrn_system(source_system), person.mrn)
    if existing is not None:
        documents.put_resource(merge_source_identity(existing, person, source_system))
        return existing["id"], "source_identity", None
    candidates = [
        (resource["id"], person_from_patient(resource))
        for resource in documents.list_resources("Patient")
        if resource.get("active", True)
    ]
    decision = decide(person, candidates)
    if decision.decision == "auto" and decision.patient_id:
        golden = documents.get_resource("Patient", decision.patient_id)
        documents.put_resource(attach_identifier(golden, person, source_system))
        return golden["id"], "auto", None
    patient = build_patient(person, source_system, partner_id)
    review_id = None
    if decision.decision == "review" and decision.patient_id:
        tags = patient.setdefault("meta", {}).setdefault("tag", [])
        tags.append({"code": "match-review"})
        review_id = resource_id("rev", patient["id"], decision.patient_id)
        documents.put_review(
            {
                "id": review_id,
                "status": "pending",
                "provisional_id": patient["id"],
                "candidate_id": decision.patient_id,
                "score": decision.score,
                "reasons": list(decision.reasons),
            }
        )
    documents.put_resource(patient)
    label = "review" if review_id else "none"
    return patient["id"], label, review_id


def find_by_identifier(documents, resource_type: str, system: str, value: str) -> dict | None:
    if not value:
        return None
    for resource in documents.list_resources(resource_type):
        for identifier in resource.get("identifier") or []:
            if identifier.get("system") == system and identifier.get("value") == value:
                return resource
    return None


def _apply_encounter(documents, message: Hl7Message, source_system: str, patient_id: str) -> str:
    assert message.visit is not None
    existing_id = resource_id("enc", source_system, message.visit.visit_number)
    existing = documents.get_resource("Encounter", existing_id)
    encounter = build_encounter(
        source_system=source_system,
        patient_id=patient_id,
        visit=message.visit,
        event=message.event,
        recorded_at=message.recorded_at,
        existing=existing,
    )
    documents.put_resource(encounter)
    return encounter["id"]


def _apply_orders(documents, message: Hl7Message, source_system: str, patient_id: str) -> list[str]:
    ids: list[str] = []
    for order in message.orders:
        observation_ids = []
        for observation in order.observations:
            resource = build_observation(
                source_system=source_system,
                patient_id=patient_id,
                control_id=message.control_id,
                order=order,
                observation=observation,
                recorded_at=message.recorded_at,
            )
            documents.put_resource(resource)
            observation_ids.append(resource["id"])
            ids.append(resource["id"])
        report = build_diagnostic_report(
            source_system=source_system,
            patient_id=patient_id,
            control_id=message.control_id,
            order=order,
            observation_ids=observation_ids,
            recorded_at=message.recorded_at,
        )
        documents.put_resource(report)
        ids.append(report["id"])
    return ids


def _apply_prescription(
    documents, message: ScriptMessage, source_system: str, patient_id: str
) -> str:
    prior = _find_prescription(documents, source_system, message)
    if message.kind == "CancelRx" and prior is not None:
        prior["status"] = "cancelled"
        documents.put_resource(prior)
        return prior["id"]
    if message.kind == "RxFill" and prior is not None:
        prior["status"] = "completed"
        documents.put_resource(prior)
        return prior["id"]
    medication = message.medication
    if medication is None:
        raise MessageRejected("MISSING_MEDICATION")
    if message.kind in {"RxRenewalRequest", "RxRenewalResponse"}:
        identity_key = message.message_id
    else:
        identity_key = medication.order_number or message.message_id
    request = build_medication_request(
        source_system=source_system,
        patient_id=patient_id,
        message_id=message.message_id,
        identity_key=identity_key,
        kind=message.kind,
        medication=medication,
        authored_on=message.sent_at,
        prior_id=prior["id"] if prior else None,
    )
    documents.put_resource(request)
    return request["id"]


def _find_prescription(documents, source_system: str, message: ScriptMessage) -> dict | None:
    order_number = message.medication.order_number if message.medication else None
    if order_number:
        found = find_by_identifier(
            documents,
            "MedicationRequest",
            f"urn:prescription:{source_system}",
            order_number,
        )
        if found is not None:
            return found
    if message.relates_to:
        return find_by_identifier(
            documents,
            "MedicationRequest",
            f"urn:script-message:{source_system}",
            message.relates_to,
        )
    return None
