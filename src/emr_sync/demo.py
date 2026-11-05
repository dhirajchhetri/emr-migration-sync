"""Run the sample messages through the in-memory pipeline and return counts."""

from __future__ import annotations

import json
import os
from pathlib import Path

from emr_sync.cdc import apply_changes
from emr_sync.migration import run_facility
from emr_sync.pipeline import build_memory_pipeline


def samples_root() -> Path:
    configured = os.environ.get("EMR_SYNC_SAMPLES")
    if configured:
        return Path(configured)
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "samples" / "hl7"
        if candidate.is_dir():
            return parent / "samples"
    raise FileNotFoundError("Sample messages were not found. Set EMR_SYNC_SAMPLES.")


def run_demo() -> dict:
    pipeline = build_memory_pipeline()
    root = samples_root()
    live = [
        ("snf01", "PARTNER_EMR", root / "hl7" / "adt_a01.hl7", None),
        ("snf01", "PARTNER_EMR", root / "hl7" / "adt_a08.hl7", None),
        ("snf01", "PARTNER_EMR", root / "hl7" / "oru_r01.hl7", None),
        ("erx", "ERX", root / "ncpdp" / "newrx.xml", None),
        ("erx", "ERX", root / "ncpdp" / "cancelrx.xml", None),
        ("snf01", "PARTNER_EMR", root / "hl7" / "adt_invalid.hl7", None),
    ]
    for partner, source, path, content_type in live:
        pipeline.ingest(partner, source, path.read_text(), content_type)
    pipeline.process_available(max_messages=20)
    pipeline.replay_rejects()
    pipeline.process_available(max_messages=20)
    migration = run_facility(
        pipeline.documents,
        root / "migration" / "facility_a",
        facility_id="facility_a",
    )
    changes = [
        json.loads(line)
        for line in (root / "cdc" / "facility_a.jsonl").read_text().splitlines()
        if line.strip()
    ]
    cdc_results = apply_changes(pipeline.documents, changes, facility_id="facility_a")
    log_text = "\n".join(pipeline.logs)
    return {
        "patients": len(pipeline.documents.list_resources("Patient")),
        "encounters": len(pipeline.documents.list_resources("Encounter")),
        "observations": len(pipeline.documents.list_resources("Observation")),
        "medication_requests": len(pipeline.documents.list_resources("MedicationRequest")),
        "conditions": len(pipeline.documents.list_resources("Condition")),
        "reviews": len(pipeline.documents.list_reviews()),
        "audit_events": len(pipeline.audit.events),
        "reject_queue_depth": pipeline.queues.depth(pipeline.rejects_queue),
        "migration_status": migration["status"],
        "migration_entities": {
            item["entity"]: {
                "source_count": item["source_count"],
                "matched_count": item["matched_count"],
                "rejected_count": item["rejected_count"],
                "pending_review_count": item["pending_review_count"],
                "disposition": item["disposition"],
            }
            for item in migration["entities"]
        },
        "cdc_applied": sum(1 for item in cdc_results if item["status"] == "applied"),
        "cdc_skipped": sum(1 for item in cdc_results if item["status"] == "skipped"),
        "log_has_sample_name": "JANE" in log_text or "MAIN ST" in log_text,
    }
