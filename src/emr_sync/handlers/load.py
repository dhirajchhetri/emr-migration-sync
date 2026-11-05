"""Load a facility extract and return its reconciliation report."""

from __future__ import annotations

from emr_sync.migration import run_facility, run_rows, store_report


def handle_load(event: dict, pipeline) -> dict:
    if "facility_dir" in event:
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


def handler(event, context=None):
    from emr_sync.runtime import pipeline_from_env

    return handle_load(event, pipeline_from_env())
