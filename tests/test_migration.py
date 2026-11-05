from pathlib import Path

from emr_sync.handlers.load import handle_load
from emr_sync.handlers.reconcile import handle_reconcile
from emr_sync.migration import EntityBook, build_report, run_facility
from emr_sync.pipeline import build_memory_pipeline

FACILITY = Path(__file__).resolve().parents[1] / "samples" / "migration" / "facility_a"


def _by_name(report: dict) -> dict:
    return {item["entity"]: item for item in report["entities"]}


def test_facility_extract_accounts_for_every_row():
    pipeline = build_memory_pipeline()
    report = run_facility(pipeline.documents, FACILITY, facility_id="facility_a")
    assert report["status"] == "accounted"
    counts = _by_name(report)
    assert counts["patients"]["source_count"] == 5
    assert counts["patients"]["matched_count"] == 3
    assert counts["patients"]["pending_review_count"] == 1
    assert counts["patients"]["rejected_count"] == 1
    assert counts["patients"]["rejections"][0]["reason"] == "SEX_INVALID"
    assert counts["wounds"]["matched_count"] == 2
    assert {item["reason"] for item in counts["wounds"]["rejections"]} == {
        "ICD10_INVALID",
        "UNKNOWN_PATIENT",
    }
    assert counts["medications"]["matched_count"] == 2
    assert {item["reason"] for item in counts["medications"]["rejections"]} == {
        "UNKNOWN_PATIENT",
        "NDC_INVALID",
    }
    assert counts["patients"]["source_checksum"] == counts["patients"]["accounted_checksum"]
    assert len(pipeline.documents.list_resources("Patient")) == 4
    assert len(pipeline.documents.list_resources("Condition")) == 2
    assert len(pipeline.documents.list_reviews()) == 1
    again = run_facility(pipeline.documents, FACILITY, facility_id="facility_a")
    assert again["status"] == "accounted"
    assert len(pipeline.documents.list_resources("Patient")) == 4


def test_a_missing_row_is_a_gap():
    book = EntityBook("patients")
    book.source("MRN1")
    book.source("MRN2")
    book.matched("MRN1")
    report = build_report("facility_a", [book], run_id="run-gap")
    assert report["status"] == "gap"
    assert report["entities"][0]["missing_ids"] == ["MRN2"]
    assert report["entities"][0]["disposition"] == "gap"


def test_load_and_reconcile_handlers_write_the_report():
    pipeline = build_memory_pipeline()
    loaded = handle_load(
        {"facility_id": "facility_a", "facility_dir": str(FACILITY)},
        pipeline,
    )
    assert loaded["status"] == "accounted"
    confirmed = handle_reconcile({"report": loaded}, pipeline)
    assert confirmed["status"] == "accounted"
    stored = pipeline.objects.get(
        "reports",
        f"reports/facility_a/{loaded['run_id']}.json",
    )
    assert "accounted" in stored
    tampered = dict(loaded)
    tampered["entities"] = [dict(tampered["entities"][0], disposition="gap")]
    assert handle_reconcile({"report": tampered}, pipeline)["status"] == "gap"
