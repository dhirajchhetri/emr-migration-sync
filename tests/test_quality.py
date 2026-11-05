from datetime import date

from emr_sync.quality import check_medication, check_patient, check_wound, violation_codes


def test_patient_rules_cover_required_fields_dates_and_sex():
    assert violation_codes("patients", {}, today=date(2026, 11, 5)).split(",")[:3] == [
        "REQ_MRN",
        "REQ_FAMILY_NAME",
        "REQ_GIVEN_NAME",
    ]
    future = {
        "mrn": "MRN1",
        "family_name": "DOE",
        "given_name": "JANE",
        "birth_date": "2027-01-01",
        "sex": "F",
    }
    assert check_patient(future, today=date(2026, 11, 5))[0].rule_id == "DOB_FUTURE"
    invalid_sex = dict(future, birth_date="1980-03-15", sex="X")
    assert check_patient(invalid_sex, today=date(2026, 11, 5))[0].rule_id == "SEX_INVALID"
    ancient = dict(invalid_sex, sex="F", birth_date="1899-12-31")
    assert check_patient(ancient, today=date(2026, 11, 5))[0].rule_id == "DOB_TOO_OLD"


def test_wound_and_medication_codes():
    wound = check_wound(
        {
            "wound_id": "W1",
            "mrn": "MRN1",
            "icd10": "not-a-code",
            "length_cm": "-1",
            "acquired_on": "2026-13-01",
        },
        today=date(2026, 11, 5),
    )
    assert [item.rule_id for item in wound] == [
        "ICD10_INVALID",
        "WOUND_LENGTH_INVALID",
        "WOUND_DATE_INVALID",
    ]
    good = check_wound(
        {
            "wound_id": "W1",
            "mrn": "MRN1",
            "icd10": "L89.152",
            "length_cm": "3.2",
            "acquired_on": "2026-01-15",
        },
        today=date(2026, 11, 5),
    )
    assert good == []
    medication = check_medication(
        {"prescription_id": "RX1", "mrn": "MRN1", "drug_description": "Cream", "ndc": "12AB"}
    )
    assert medication[0].rule_id == "NDC_INVALID"
    dashed = check_medication(
        {
            "prescription_id": "RX1",
            "mrn": "MRN1",
            "drug_description": "Cream",
            "ndc": "00456-0123-01",
            "rxnorm": "979334",
        }
    )
    assert dashed == []
