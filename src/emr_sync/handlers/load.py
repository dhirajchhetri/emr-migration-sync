"""Load a facility extract and return its reconciliation report.

The state machine passes a curated prefix. JSON Lines manifests written by
the Glue job are the handoff, so this function does not need a Spark runtime.
"""

from __future__ import annotations

import json
from pathlib import Path

from emr_sync.migration import run_facility, run_rows, store_report


def handle_load(event: dict, pipeline) -> dict:
    if event.get("curated_prefix"):
        report = run_rows(
            pipeline.documents,
            _read_jsonl(pipeline, event["curated_prefix"], "patients"),
            _read_jsonl(pipeline, event["curated_prefix"], "wounds"),
            _read_jsonl(pipeline, event["curated_prefix"], "medications"),
            facility_id=event["facility_id"],
        )
    elif "facility_dir" in event:
        report = run_facility(
            pipeline.documents,
            event["facility_dir"],
            facility_id=event["facility_id"],
        )
    else:
        rows = event.get("rows") or {}
        report = run_rows(
            pipeline.documents,
            rows.get("patients") or [],
            rows.get("wounds") or [],
            rows.get("medications") or [],
            facility_id=event["facility_id"],
        )
    store_report(pipeline.objects, pipeline.reports_bucket, report)
    return report


def _read_jsonl(pipeline, prefix: str, kind: str) -> list[dict]:
    name = f"{kind}.jsonl"
    if prefix.startswith("s3://"):
        bucket, key = _split_s3(prefix)
        object_key = f"{key}/{name}" if key else name
        body = pipeline.objects.get(bucket, object_key)
    else:
        body = (Path(prefix) / name).read_text()
    return [json.loads(line) for line in body.splitlines() if line.strip()]


def _split_s3(uri: str) -> tuple[str, str]:
    remainder = uri[5:].rstrip("/")
    bucket, _, key = remainder.partition("/")
    return bucket, key


def handler(event, context=None):
    from emr_sync.runtime import pipeline_from_env

    return handle_load(event, pipeline_from_env())
