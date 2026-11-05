"""Apply ordered change records from a legacy extract.

This is the file-shaped stand-in for a database change stream. A sequence
number is applied once per primary key. A row that fails validation blocks
later changes for that key until a corrected record arrives. Other keys keep
moving.
"""

from __future__ import annotations

from datetime import date

from emr_sync.apply import find_by_identifier, resolve_patient
from emr_sync.fhir import mrn_system
from emr_sync.migration import _person, apply_medication_row, apply_wound_row
from emr_sync.quality import cell, check_row

_TABLE_KIND = {
    "patients": "patients",
    "wounds": "wounds",
    "medications": "medications",
}


def apply_changes(
    documents,
    changes: list[dict],
    *,
    facility_id: str,
    today: date | None = None,
) -> list[dict]:
    today = today or date.today()
    source_system = f"legacy:{facility_id}"
    blocked: set[str] = set()
    results: list[dict] = []
    ordered = sorted(changes, key=lambda item: int(item["sequence"]))
    for change in ordered:
        token = f"{change['table']}#{change['pk']}"
        sequence = int(change["sequence"])
        base = {"table": change["table"], "pk": change["pk"], "sequence": sequence}
        if token in blocked:
            results.append({**base, "status": "blocked"})
            continue
        if sequence <= documents.checkpoints.get(token, 0):
            results.append({**base, "status": "skipped"})
            continue
        if change["table"] not in _TABLE_KIND:
            blocked.add(token)
            results.append({**base, "status": "rejected", "reason": "UNSUPPORTED_TABLE"})
            continue
        if change["op"] == "D":
            _delete(documents, change["table"], change["pk"], source_system)
            documents.checkpoints[token] = sequence
            results.append({**base, "status": "applied"})
            continue
        row = _row_from_change(change)
        violations = check_row(_TABLE_KIND[change["table"]], row, today)
        if violations:
            blocked.add(token)
            results.append({**base, "status": "rejected", "reason": violations[0].rule_id})
            continue
        reason = _apply(documents, change["table"], row, source_system, facility_id)
        if reason:
            blocked.add(token)
            results.append({**base, "status": "rejected", "reason": reason})
            continue
        documents.checkpoints[token] = sequence
        results.append({**base, "status": "applied"})
    return results


def _apply(documents, table: str, row: dict, source_system: str, facility_id: str) -> str | None:
    if table == "patients":
        resolve_patient(documents, _person(row), source_system, facility_id)
        return None
    if table == "wounds":
        return apply_wound_row(documents, row, source_system)
    return apply_medication_row(documents, row, source_system)


def _delete(documents, table: str, primary_key: str, source_system: str) -> None:
    if table == "patients":
        resource = find_by_identifier(documents, "Patient", mrn_system(source_system), primary_key)
        if resource is not None:
            resource["active"] = False
            documents.put_resource(resource)
        return
    if table == "wounds":
        resource = find_by_identifier(
            documents, "Condition", f"urn:wound:{source_system}", primary_key
        )
        if resource is not None:
            resource["verificationStatus"] = {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/condition-ver-status",
                        "code": "entered-in-error",
                    }
                ]
            }
            documents.put_resource(resource)
        return
    resource = find_by_identifier(
        documents, "MedicationRequest", f"urn:prescription:{source_system}", primary_key
    )
    if resource is not None:
        resource["status"] = "entered-in-error"
        documents.put_resource(resource)


def _row_from_change(change: dict) -> dict:
    after = change.get("after") or {}
    row = {key: "" if value is None else str(value) for key, value in after.items()}
    if change["table"] == "patients" and not cell(row, "mrn"):
        row["mrn"] = change["pk"]
    if change["table"] == "wounds" and not cell(row, "wound_id"):
        row["wound_id"] = change["pk"]
    if change["table"] == "medications" and not cell(row, "prescription_id"):
        row["prescription_id"] = change["pk"]
    return row
