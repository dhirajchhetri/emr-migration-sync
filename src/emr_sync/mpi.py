"""Rules-based patient comparison.

The same function is the boundary a trained matcher would sit behind. This
repository ships the rules scorer only. Two hard stops keep a high score from
merging records that disagree on date of birth or on sex: those go to review.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from emr_sync.models import Person

AUTO_THRESHOLD = 85
REVIEW_THRESHOLD = 60

_SOUNDEX_GROUPS = {
    "B": "1",
    "F": "1",
    "P": "1",
    "V": "1",
    "C": "2",
    "G": "2",
    "J": "2",
    "K": "2",
    "Q": "2",
    "S": "2",
    "X": "2",
    "Z": "2",
    "D": "3",
    "T": "3",
    "L": "4",
    "M": "5",
    "N": "5",
    "R": "6",
}


@dataclass(frozen=True)
class Decision:
    decision: str
    score: int
    patient_id: str | None
    reasons: tuple[str, ...]


def decide(person: Person, candidates: list[tuple[str, Person]]) -> Decision:
    best_score = 0
    best_id: str | None = None
    best_reasons: tuple[str, ...] = ()
    hard_stop = False
    for patient_id, other in candidates:
        score, reasons, blocked = compare(person, other)
        if score > best_score or (score == best_score and score > 0 and best_id is None):
            best_score = score
            best_id = patient_id
            best_reasons = tuple(reasons)
            hard_stop = blocked
    if best_id is None or best_score < REVIEW_THRESHOLD:
        return Decision("none", best_score, None, best_reasons)
    if hard_stop or best_score < AUTO_THRESHOLD:
        return Decision("review", best_score, best_id, best_reasons)
    return Decision("auto", best_score, best_id, best_reasons)


def compare(left: Person, right: Person) -> tuple[int, list[str], bool]:
    score = 0
    reasons: list[str] = []
    blocked = False
    if left.birth_date and right.birth_date:
        if left.birth_date == right.birth_date:
            score += 40
            reasons.append("birth_date")
        else:
            blocked = True
            reasons.append("birth_date_differs")
    if left.sex in {"M", "F", "O"} and right.sex in {"M", "F", "O"}:
        if left.sex == right.sex:
            score += 10
            reasons.append("sex")
        else:
            blocked = True
            reasons.append("sex_differs")
    family, family_reason = _family_points(left.family_name, right.family_name)
    if family:
        score += family
        reasons.append(f"family_{family_reason}")
    given, given_reason = _given_points(left.given_name, right.given_name)
    if given:
        score += given
        reasons.append(f"given_{given_reason}")
    if _phone_match(left.phone, right.phone):
        score += 10
        reasons.append("phone")
    return score, reasons, blocked


def soundex(value: str) -> str:
    letters = re.sub(r"[^A-Z]", "", value.upper())
    if not letters:
        return ""
    coded = [letters[0]]
    previous = _SOUNDEX_GROUPS.get(letters[0], "0")
    for character in letters[1:]:
        digit = _SOUNDEX_GROUPS.get(character, "0")
        if digit != "0" and digit != previous:
            coded.append(digit)
        previous = digit
    return ("".join(coded) + "000")[:4]


def _family_points(left: str, right: str) -> tuple[int, str]:
    a, b = _norm(left), _norm(right)
    if not a or not b:
        return 0, ""
    if a == b:
        return 25, "exact"
    if soundex(a) == soundex(b):
        return 12, "soundex"
    return 0, ""


def _given_points(left: str, right: str) -> tuple[int, str]:
    a, b = _norm(left), _norm(right)
    if not a or not b:
        return 0, ""
    if a == b:
        return 15, "exact"
    if len(a) >= 3 and len(b) >= 3 and a[:3] == b[:3]:
        return 8, "prefix"
    return 0, ""


def _phone_match(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    a = re.sub(r"\D", "", left)
    b = re.sub(r"\D", "", right)
    if len(a) >= 10 and len(b) >= 10:
        return a[-10:] == b[-10:]
    if len(a) >= 7 and len(b) >= 7:
        return a[-7:] == b[-7:]
    return False


def _norm(value: str) -> str:
    return re.sub(r"[^A-Z]", "", value.upper())
