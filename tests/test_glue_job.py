import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FACILITY = ROOT / "samples" / "migration" / "facility_a"


def _job():
    path = ROOT / "jobs" / "normalize_facility_extract.py"
    spec = importlib.util.spec_from_file_location("normalize_facility_extract", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_argument_parser_ignores_glue_runtime_flags():
    job = _job()
    args = job.parse_args(
        [
            "--input_prefix",
            "s3://raw/facility_a",
            "--curated_prefix",
            "s3://curated/facility_a",
            "--quarantine_prefix",
            "s3://quarantine/facility_a",
            "--facility_id",
            "facility_a",
            "--JOB_NAME",
            "emr-sync-normalize-facility",
        ]
    )
    assert args.facility_id == "facility_a"
    assert args.input_prefix == "s3://raw/facility_a"


def test_row_codes_use_the_shared_quality_rules():
    job = _job()
    assert job.patient_codes("MRN1", "DOE", "JANE", "1980-03-15", "F", "", "FAC01") == ""
    invalid_sex = job.patient_codes("MRN5", "QUINN", "ALEX", "1988-01-09", "X", "", "FAC01")
    assert "SEX_INVALID" in invalid_sex
    invalid_code = job.wound_codes("W3", "MRN1", "NOT_A_CODE", "arm", "1", "2026-03-01")
    assert "ICD10_INVALID" in invalid_code
    invalid_ndc = job.medication_codes("RX4", "MRN1", "Cream", "12AB", "1", "Apply", "active")
    assert "NDC_INVALID" in invalid_ndc


def test_job_splits_the_sample_extract(tmp_path):
    pytest.importorskip("pyspark")
    job = _job()
    curated = tmp_path / "curated"
    quarantine = tmp_path / "quarantine"
    code = job.main(
        [
            "--input_prefix",
            str(FACILITY),
            "--curated_prefix",
            str(curated),
            "--quarantine_prefix",
            str(quarantine),
            "--facility_id",
            "facility_a",
        ]
    )
    assert code == 0
    patients = [line for line in (curated / "patients.jsonl").read_text().splitlines() if line]
    rejected = (quarantine / "patients.jsonl").read_text()
    assert len(patients) == 4
    assert "SEX_INVALID" in rejected
    assert (curated / "patients").exists()
    assert "L89.152" in (curated / "wounds.jsonl").read_text()
    assert "NOT_A_CODE" in (quarantine / "wounds.jsonl").read_text()
