import json
from pathlib import Path

from emr_sync.cdc import apply_changes
from emr_sync.pipeline import build_memory_pipeline
from emr_sync.store import MemoryDocuments

SAMPLE = Path(__file__).resolve().parents[1] / "samples" / "cdc" / "facility_a.jsonl"


def _changes() -> list[dict]:
    return [json.loads(line) for line in SAMPLE.read_text().splitlines() if line.strip()]


def test_sample_change_log_is_ordered_and_idempotent():
    documents = MemoryDocuments()
    results = apply_changes(documents, _changes(), facility_id="facility_a")
    assert [item["status"] for item in results] == [
        "applied",
        "applied",
        "skipped",
        "applied",
        "applied",
    ]
    patient = documents.list_resources("Patient")[0]
    assert patient["telecom"][0]["value"] == "3035550199"
    medication = documents.list_resources("MedicationRequest")[0]
    assert medication["status"] == "entered-in-error"
    replay = apply_changes(documents, _changes(), facility_id="facility_a")
    assert {item["status"] for item in replay} == {"skipped"}


def test_invalid_change_blocks_only_that_key():
    documents = MemoryDocuments()
    changes = [
        {
            "op": "I",
            "table": "patients",
            "pk": "MRN1",
            "sequence": 1,
            "after": {
                "mrn": "MRN1",
                "family_name": "DOE",
                "given_name": "JANE",
                "birth_date": "1980-03-15",
                "sex": "X",
            },
        },
        {
            "op": "U",
            "table": "patients",
            "pk": "MRN1",
            "sequence": 2,
            "after": {
                "mrn": "MRN1",
                "family_name": "DOE",
                "given_name": "JANE",
                "birth_date": "1980-03-15",
                "sex": "F",
            },
        },
        {
            "op": "I",
            "table": "patients",
            "pk": "MRN2",
            "sequence": 3,
            "after": {
                "mrn": "MRN2",
                "family_name": "NG",
                "given_name": "MINH",
                "birth_date": "1992-07-19",
                "sex": "F",
            },
        },
    ]
    results = apply_changes(documents, changes, facility_id="facility_a")
    assert [item["status"] for item in results] == ["rejected", "blocked", "applied"]
    assert results[0]["reason"] == "SEX_INVALID"
    mrns = [
        ident["value"]
        for patient in documents.list_resources("Patient")
        for ident in patient["identifier"]
    ]
    assert mrns == ["MRN2"]


def test_pipeline_documents_accept_checkpoints():
    pipeline = build_memory_pipeline()
    apply_changes(pipeline.documents, _changes(), facility_id="facility_a")
    assert pipeline.documents.checkpoints["patients#MRN9001"] == 2
