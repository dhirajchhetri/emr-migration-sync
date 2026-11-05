"""Load a facility extract through the same apply functions as live sync.

Every source row ends as matched, rejected with a reason, or pending review.
The report is accounted only when nothing is left over.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import uuid
from datetime import date, datetime
from pathlib import Path

from emr_sync.apply import find_by_identifier, resolve_patient
from emr_sync.dates import parse_date
from emr_sync.fhir import build_medication_request, build_wound, build_wound_length, mrn_system
from emr_sync.models import Medication, Person
from emr_sync.quality import cell, check_medication, check_patient, check_wound


class EntityBook:
    def __init__(self, name: str) -> None:
        self.name = name
        self.source_ids: list[str] = []
        self.matched_ids: list[str] = []
        self.pending_ids: list[str] = []
        self.rejections: list[tuple[str, str]] = []

    def source(self, row_id: str) -> None:
        self.source_ids.append(row_id)

    def matched(self, row_id: str) -> None:
        self.matched_ids.append(row_id)

    def pending(self, row_id: str) -> None:
        self.pending_ids.append(row_id)

    def reject(self, row_id: str, reason: str) -> None:
        self.rejections.append((row_id, reason))


def run_facility(
    documents,
    facility_dir: str | Path,
    *,
    facility_id: str,
    today: date | None = None,
) -> dict:
    directory = Path(facility_dir)
    return run_rows(
        documents,
        _read_csv(directory / "patients.csv"),
        _read_csv(directory / "wounds.csv"),
        _read_csv(directory / "medications.csv"),
        facility_id=facility_id,
        today=today,
    )


def run_rows(
    documents,
    patients: list[dict],
    wounds: list[dict],
    medications: list[dict],
    *,
    facility_id: str,
    today: date | None = None,
    run_id: str | None = None,
) -> dict:
    today = today or date.today()
    source_system = f"legacy:{facility_id}"
    patient_book = EntityBook("patients")
    wound_book = EntityBook("wounds")
    medication_book = EntityBook("medications")
    _load_patients(documents, patients, patient_book, source_system, facility_id, today)
    _load_wounds(documents, wounds, wound_book, source_system, today)
    _load_medications(documents, medications, medication_book, source_system)
    return build_report(
        facility_id,
        [patient_book, wound_book, medication_book],
        run_id=run_id,
    )


def build_report(facility_id: str, books: list[EntityBook], run_id: str | None = None) -> dict:
    entities = [_entity_result(book) for book in books]
    status = _rollup(entities)
    return {
        "run_id": run_id or "run-" + uuid.uuid4().hex[:12],
        "facility_id": facility_id,
        "status": status,
        "entities": entities,
    }


def confirm_report(report: dict) -> dict:
    """Recompute the rollup. A gap in any entity keeps the run from being accounted."""
    entities = report.get("entities") or []
    status = _rollup(entities)
    confirmed = dict(report)
    confirmed["status"] = status
    return confirmed


def store_report(objects, bucket: str, report: dict) -> str:
    key = f"reports/{report['facility_id']}/{report['run_id']}.json"
    objects.put(bucket, key, json.dumps(report, indent=2, sort_keys=True))
    return key


def checksum(ids: list[str]) -> str:
    digest = hashlib.sha256()
    for item in ids:
        digest.update(item.encode())
        digest.update(b"\n")
    return digest.hexdigest()


def _entity_result(book: EntityBook) -> dict:
    source = set(book.source_ids)
    matched = set(book.matched_ids)
    pending = set(book.pending_ids)
    rejected = {row_id for row_id, _reason in book.rejections}
    missing = sorted(source - matched - pending - rejected)
    accounted_ids = matched | pending | rejected
    disposition = "accounted" if source and source == accounted_ids else "gap"
    if not source:
        disposition = "accounted"
    return {
        "entity": book.name,
        "source_count": len(source),
        "matched_count": len(matched & source),
        "rejected_count": len(rejected & source),
        "pending_review_count": len(pending & source),
        "missing_count": len(missing),
        "missing_ids": missing,
        "disposition": disposition,
        "source_checksum": checksum(sorted(source)),
        "accounted_checksum": checksum(sorted(accounted_ids)),
        "rejections": [{"id": row_id, "reason": reason} for row_id, reason in book.rejections],
    }


def _rollup(entities: list[dict]) -> str:
    if entities and all(item.get("disposition") == "accounted" for item in entities):
        return "accounted"
    return "gap"


def _load_patients(
    documents,
    rows,
    book: EntityBook,
    source_system: str,
    facility_id: str,
    today: date,
) -> None:
    for index, row in enumerate(rows, start=1):
        row_id = cell(row, "mrn") or f"row-{index}"
        book.source(row_id)
        violations = check_patient(row, today)
        if violations:
            book.reject(row_id, violations[0].rule_id)
            continue
        _patient_id, decision, _review_id = resolve_patient(
            documents, _person(row), source_system, facility_id
        )
        if decision == "review":
            book.pending(row_id)
        else:
            book.matched(row_id)


def _load_wounds(documents, rows, book: EntityBook, source_system: str, today: date) -> None:
    for index, row in enumerate(rows, start=1):
        row_id = cell(row, "wound_id") or f"row-{index}"
        book.source(row_id)
        violations = check_wound(row, today)
        if violations:
            book.reject(row_id, violations[0].rule_id)
            continue
        reason = apply_wound_row(documents, row, source_system)
        if reason:
            book.reject(row_id, reason)
        else:
            book.matched(row_id)


def _load_medications(documents, rows, book: EntityBook, source_system: str) -> None:
    for index, row in enumerate(rows, start=1):
        row_id = cell(row, "prescription_id") or f"row-{index}"
        book.source(row_id)
        violations = check_medication(row)
        if violations:
            book.reject(row_id, violations[0].rule_id)
            continue
        reason = apply_medication_row(documents, row, source_system)
        if reason:
            book.reject(row_id, reason)
        else:
            book.matched(row_id)


def apply_wound_row(documents, row: dict, source_system: str) -> str | None:
    patient = find_by_identifier(documents, "Patient", mrn_system(source_system), cell(row, "mrn"))
    if patient is None or patient.get("active") is False:
        return "UNKNOWN_PATIENT"
    wound = build_wound(
        source_system=source_system,
        patient_id=patient["id"],
        wound_id=cell(row, "wound_id"),
        icd10=cell(row, "icd10").upper(),
        site=cell(row, "site"),
        acquired_on=parse_date(cell(row, "acquired_on")),
    )
    documents.put_resource(wound)
    length = cell(row, "length_cm")
    if length:
        recorded = parse_date(cell(row, "acquired_on"))
        moment = datetime(recorded.year, recorded.month, recorded.day) if recorded else None
        documents.put_resource(
            build_wound_length(
                source_system=source_system,
                patient_id=patient["id"],
                wound_id=cell(row, "wound_id"),
                length_cm=float(length),
                recorded_at=moment,
            )
        )
    return None


def apply_medication_row(documents, row: dict, source_system: str) -> str | None:
    patient = find_by_identifier(documents, "Patient", mrn_system(source_system), cell(row, "mrn"))
    if patient is None or patient.get("active") is False:
        return "UNKNOWN_PATIENT"
    order_number = cell(row, "prescription_id")
    medication = Medication(
        description=cell(row, "drug_description"),
        ndc=re.sub(r"\D", "", cell(row, "ndc")) or None,
        rxnorm=cell(row, "rxnorm") or None,
        sig=cell(row, "sig") or None,
        order_number=order_number,
    )
    request = build_medication_request(
        source_system=source_system,
        patient_id=patient["id"],
        message_id=order_number,
        identity_key=order_number,
        kind="NewRx",
        medication=medication,
        authored_on=None,
    )
    if cell(row, "status").lower() == "cancelled":
        request["status"] = "cancelled"
    documents.put_resource(request)
    return None


def _person(row: dict) -> Person:
    sex = cell(row, "sex").upper()
    if sex == "A":
        sex = "O"
    phone = re.sub(r"\D", "", cell(row, "phone"))
    return Person(
        mrn=cell(row, "mrn"),
        family_name=cell(row, "family_name"),
        given_name=cell(row, "given_name"),
        sex=sex,
        birth_date=parse_date(cell(row, "birth_date")),
        phone=phone or None,
    )


def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))
