from pathlib import Path

from emr_sync.fhir import (
    ICD10,
    LOINC,
    NDC,
    RXNORM,
    build_diagnostic_report,
    build_encounter,
    build_medication_request,
    build_medication_statement,
    build_observation,
    build_patient,
    build_wound,
    merge_source_identity,
    person_from_patient,
)
from emr_sync.hl7 import parse_hl7
from emr_sync.ncpdp import parse_script

ROOT = Path(__file__).resolve().parents[1]


def _hl7(name: str):
    return parse_hl7((ROOT / "samples" / "hl7" / name).read_text())


def _script(name: str):
    return parse_script((ROOT / "samples" / "ncpdp" / name).read_text())


def test_adt_a01_maps_patient_and_inpatient_encounter():
    message = _hl7("adt_a01.hl7")
    patient = build_patient(message.person, "PARTNER_EMR", "snf01")
    encounter = build_encounter(
        source_system="PARTNER_EMR",
        patient_id=patient["id"],
        visit=message.visit,
        event="A01",
        recorded_at=message.recorded_at,
    )
    assert patient["resourceType"] == "Patient"
    assert patient["gender"] == "female"
    assert patient["birthDate"] == "1980-03-15"
    assert patient["name"][0]["family"] == "DOE"
    assert patient["name"][0]["given"] == ["JANE", "A"]
    assert patient["identifier"][0] == {"system": "urn:mrn:PARTNER_EMR", "value": "MRN1001"}
    assert patient["address"][0]["city"] == "AUSTIN"
    assert encounter["status"] == "in-progress"
    assert encounter["class"]["code"] == "IMP"
    assert encounter["subject"] == {"reference": f"Patient/{patient['id']}"}
    assert encounter["period"]["start"] == "2026-11-02T11:59:00Z"
    assert encounter["location"][0]["location"]["display"] == "WARD1^101"
    again = build_patient(message.person, "PARTNER_EMR", "snf01")
    assert again["id"] == patient["id"]


def test_discharge_finishes_the_same_encounter():
    admitted = _hl7("adt_a01.hl7")
    patient = build_patient(admitted.person, "PARTNER_EMR", "snf01")
    opened = build_encounter(
        source_system="PARTNER_EMR",
        patient_id=patient["id"],
        visit=admitted.visit,
        event="A01",
        recorded_at=admitted.recorded_at,
    )
    discharged = _hl7("adt_a03.hl7")
    closed = build_encounter(
        source_system="PARTNER_EMR",
        patient_id=patient["id"],
        visit=discharged.visit,
        event="A03",
        recorded_at=discharged.recorded_at,
        existing=opened,
    )
    assert closed["id"] == opened["id"]
    assert closed["status"] == "finished"
    assert closed["period"]["end"] == "2026-11-05T10:00:00Z"
    updated = _hl7("adt_a08.hl7")
    kept = build_encounter(
        source_system="PARTNER_EMR",
        patient_id=patient["id"],
        visit=updated.visit,
        event="A08",
        recorded_at=updated.recorded_at,
        existing=closed,
    )
    assert kept["status"] == "finished"
    assert kept["location"][0]["location"]["display"] == "WARD2^210"


def test_source_update_changes_phone_and_round_trips_demographics():
    admitted = _hl7("adt_a01.hl7")
    patient = build_patient(admitted.person, "PARTNER_EMR", "snf01")
    updated = merge_source_identity(patient, _hl7("adt_a08.hl7").person, "PARTNER_EMR")
    assert updated["id"] == patient["id"]
    assert updated["telecom"][0]["value"] == "5125550199"
    assert updated["address"][0]["line"] == ["500 OAK AVE"]
    restored = person_from_patient(updated)
    assert restored.family_name == "DOE"
    assert restored.birth_date.isoformat() == "1980-03-15"
    assert restored.sex == "F"


def test_oru_maps_loinc_observations_and_a_report():
    message = _hl7("oru_r01.hl7")
    patient = build_patient(message.person, "PARTNER_EMR", "snf01")
    order = message.orders[0]
    observations = [
        build_observation(
            source_system="PARTNER_EMR",
            patient_id=patient["id"],
            control_id=message.control_id,
            order=order,
            observation=item,
            recorded_at=message.recorded_at,
        )
        for item in order.observations
    ]
    report = build_diagnostic_report(
        source_system="PARTNER_EMR",
        patient_id=patient["id"],
        control_id=message.control_id,
        order=order,
        observation_ids=[item["id"] for item in observations],
        recorded_at=message.recorded_at,
    )
    assert observations[0]["valueQuantity"] == {"value": 3.2, "unit": "cm"}
    assert observations[0]["code"]["coding"][0]["system"] == LOINC
    assert report["result"][0]["reference"] == f"Observation/{observations[0]['id']}"
    assert report["code"]["coding"][0]["code"] == "39125-4"


def test_prescriptions_map_to_medication_requests_and_statements():
    newrx = _script("newrx.xml")
    patient_id = "pat-test"
    request = build_medication_request(
        source_system="ERX",
        patient_id=patient_id,
        message_id=newrx.message_id,
        identity_key=newrx.medication.order_number,
        kind=newrx.kind,
        medication=newrx.medication,
        authored_on=newrx.sent_at,
    )
    assert request["status"] == "active"
    assert request["intent"] == "order"
    coding = request["medicationCodeableConcept"]["coding"]
    codes = {item["system"]: item["code"] for item in coding}
    assert codes[NDC] == "00456012301"
    assert codes[RXNORM] == "979334"
    cancelled = _script("cancelrx.xml")
    cancel = build_medication_request(
        source_system="ERX",
        patient_id=patient_id,
        message_id=cancelled.message_id,
        identity_key=cancelled.medication.order_number,
        kind="CancelRx",
        medication=cancelled.medication,
        authored_on=cancelled.sent_at,
    )
    assert cancel["id"] == request["id"]
    assert cancel["status"] == "cancelled"
    renewal = _script("rxrenewal.xml")
    renewed = build_medication_request(
        source_system="ERX",
        patient_id=patient_id,
        message_id=renewal.message_id,
        identity_key=renewal.message_id,
        kind="RxRenewalRequest",
        medication=renewal.medication,
        authored_on=renewal.sent_at,
        prior_id=request["id"],
    )
    assert renewed["intent"] == "proposal"
    assert renewed["priorPrescription"]["reference"] == f"MedicationRequest/{request['id']}"
    history = _script("history.xml")
    statement = build_medication_statement(
        source_system="ERX",
        patient_id=patient_id,
        message_id=history.message_id,
        medication=history.medications[0],
        effective=history.sent_at,
    )
    assert statement["resourceType"] == "MedicationStatement"
    assert statement["status"] == "active"


def test_wound_condition_uses_icd10():
    wound = build_wound(
        source_system="legacy:facility_a",
        patient_id="pat-1",
        wound_id="W1",
        icd10="L89.152",
        site="sacrum",
        acquired_on=None,
    )
    assert wound["resourceType"] == "Condition"
    assert wound["code"]["coding"][0]["system"] == ICD10
    assert wound["code"]["coding"][0]["code"] == "L89.152"
    assert wound["bodySite"][0]["text"] == "sacrum"
