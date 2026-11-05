from datetime import date

from emr_sync.models import Person
from emr_sync.mpi import decide, soundex


def _person(**overrides) -> Person:
    data = {
        "mrn": "MRN1",
        "family_name": "DOE",
        "given_name": "JANE",
        "sex": "F",
        "birth_date": date(1980, 3, 15),
        "phone": "5125550100",
    }
    data.update(overrides)
    return Person(**data)


def test_identical_demographics_auto_match():
    incoming = _person(mrn="MRN2")
    decision = decide(incoming, [("pat-1", _person())])
    assert decision.decision == "auto"
    assert decision.score == 100
    assert decision.patient_id == "pat-1"
    assert "birth_date" in decision.reasons
    assert "phone" in decision.reasons


def test_sex_or_birth_date_disagreement_never_auto_merges():
    sex = decide(_person(sex="M"), [("pat-1", _person())])
    assert sex.decision == "review"
    assert "sex_differs" in sex.reasons
    dob = decide(_person(birth_date=date(1981, 3, 15)), [("pat-1", _person())])
    assert dob.decision == "review"
    assert "birth_date_differs" in dob.reasons


def test_unrelated_person_is_not_a_candidate():
    incoming = _person(
        family_name="RIVERA",
        given_name="CARLOS",
        sex="M",
        birth_date=date(1975, 11, 2),
        phone="2105550148",
    )
    decision = decide(incoming, [("pat-1", _person())])
    assert decision.decision == "none"
    assert decision.patient_id is None


def test_soundex_family_name_is_a_review_not_an_auto_match():
    assert soundex("SMITH") == soundex("SMYTH")
    incoming = _person(family_name="SMYTH", phone=None)
    existing = _person(family_name="SMITH", phone=None)
    decision = decide(incoming, [("pat-1", existing)])
    assert decision.decision == "review"
    assert "family_soundex" in decision.reasons
    assert decision.score == 40 + 10 + 12 + 15


def test_empty_candidate_list_creates_no_match():
    decision = decide(_person(), [])
    assert decision.decision == "none"
    assert decision.score == 0
