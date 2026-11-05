"""Row checks shared by the local migration runner and the Glue job.

A row that fails a check is quarantined with a reason code. The value itself
is not part of the reason, so a quarantine listing can be reviewed without
copying free text into a log.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from emr_sync.dates import parse_date

_ICD10 = re.compile(r"^[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?$")
_NDC = re.compile(r"^\d{10,11}$")
_SEX = {"M", "F", "O", "U", "A"}


@dataclass(frozen=True)
class Violation:
    rule_id: str
    field: str


def check_row(kind: str, row: dict, today: date | None = None) -> list[Violation]:
    if kind == "patients":
        return check_patient(row, today)
    if kind == "wounds":
        return check_wound(row, today)
    if kind == "medications":
        return check_medication(row)
    raise ValueError(f"Unknown extract kind '{kind}'")


def violation_codes(kind: str, row: dict, today: date | None = None) -> str:
    return ",".join(item.rule_id for item in check_row(kind, row, today))


def check_patient(row: dict, today: date | None = None) -> list[Violation]:
    today = today or date.today()
    violations: list[Violation] = []
    if not _cell(row, "mrn"):
        violations.append(Violation("REQ_MRN", "mrn"))
    if not _cell(row, "family_name"):
        violations.append(Violation("REQ_FAMILY_NAME", "family_name"))
    if not _cell(row, "given_name"):
        violations.append(Violation("REQ_GIVEN_NAME", "given_name"))
    dob_text = _cell(row, "birth_date")
    parsed = parse_date(dob_text) if dob_text else None
    if parsed is None:
        violations.append(Violation("DOB_INVALID", "birth_date"))
    elif parsed > today:
        violations.append(Violation("DOB_FUTURE", "birth_date"))
    elif parsed.year < 1900:
        violations.append(Violation("DOB_TOO_OLD", "birth_date"))
    if _cell(row, "sex").upper() not in _SEX:
        violations.append(Violation("SEX_INVALID", "sex"))
    return violations


def check_wound(row: dict, today: date | None = None) -> list[Violation]:
    today = today or date.today()
    violations: list[Violation] = []
    if not _cell(row, "wound_id"):
        violations.append(Violation("REQ_WOUND_ID", "wound_id"))
    if not _cell(row, "mrn"):
        violations.append(Violation("REQ_MRN", "mrn"))
    if not _ICD10.match(_cell(row, "icd10").upper()):
        violations.append(Violation("ICD10_INVALID", "icd10"))
    length = _cell(row, "length_cm")
    if length:
        try:
            value = float(length)
        except ValueError:
            violations.append(Violation("WOUND_LENGTH_INVALID", "length_cm"))
        else:
            if value < 0:
                violations.append(Violation("WOUND_LENGTH_INVALID", "length_cm"))
    acquired = _cell(row, "acquired_on")
    if acquired:
        parsed = parse_date(acquired)
        if parsed is None:
            violations.append(Violation("WOUND_DATE_INVALID", "acquired_on"))
        elif parsed > today:
            violations.append(Violation("WOUND_DATE_FUTURE", "acquired_on"))
    return violations


def check_medication(row: dict) -> list[Violation]:
    violations: list[Violation] = []
    if not _cell(row, "prescription_id"):
        violations.append(Violation("REQ_PRESCRIPTION_ID", "prescription_id"))
    if not _cell(row, "mrn"):
        violations.append(Violation("REQ_MRN", "mrn"))
    if not _cell(row, "drug_description"):
        violations.append(Violation("REQ_DRUG", "drug_description"))
    raw_ndc = _cell(row, "ndc")
    if raw_ndc and not _NDC.match(re.sub(r"\D", "", raw_ndc)):
        violations.append(Violation("NDC_INVALID", "ndc"))
    rxnorm = _cell(row, "rxnorm")
    if rxnorm and not rxnorm.isdigit():
        violations.append(Violation("RXNORM_INVALID", "rxnorm"))
    return violations


def cell(row: dict, name: str) -> str:
    return _cell(row, name)


def _cell(row: dict, name: str) -> str:
    value = row.get(name)
    if value is None:
        return ""
    return str(value).strip()
