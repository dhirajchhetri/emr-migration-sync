import json
import subprocess
from pathlib import Path

from emr_sync.cli import main
from emr_sync.directory import build_directory_pipeline

ROOT = Path(__file__).resolve().parents[1]


def test_demo_prints_counts_without_sample_names(capsys):
    assert main(["demo"]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["migration_status"] == "accounted"
    assert summary["patients"] >= 4
    assert summary["reviews"] >= 1
    assert summary["reject_queue_depth"] >= 1
    assert summary["cdc_applied"] >= 1
    assert summary["cdc_skipped"] >= 1
    assert summary["log_has_sample_name"] is False
    assert summary["migration_entities"]["patients"]["disposition"] == "accounted"


def test_directory_backend_keeps_a_patient_across_processes(tmp_path):
    raw = (ROOT / "samples" / "hl7" / "adt_a01.hl7").read_text()
    message = tmp_path / "a01.hl7"
    message.write_text(raw)
    assert (
        main(
            [
                "--backend",
                "dir",
                "--root",
                str(tmp_path / "state"),
                "ingest",
                str(message),
                "--partner",
                "snf01",
                "--source",
                "PARTNER_EMR",
            ]
        )
        == 0
    )
    assert main(["--backend", "dir", "--root", str(tmp_path / "state"), "work"]) == 0
    restored = build_directory_pipeline(tmp_path / "state")
    assert len(restored.documents.list_resources("Patient")) == 1
    assert restored.audit.events


def test_localstack_init_script_is_valid_shell():
    script = ROOT / "scripts" / "localstack-init.sh"
    subprocess.run(["sh", "-n", str(script)], check=True)


def test_cli_module_help():
    completed = subprocess.run(
        ["python3", "-m", "emr_sync.cli", "--help"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "demo" in completed.stdout
