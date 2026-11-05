"""Confirm a migration report and write it to the reports bucket."""

from __future__ import annotations

from emr_sync.migration import confirm_report, store_report


def handle_reconcile(event: dict, pipeline) -> dict:
    report = event.get("report") or event
    confirmed = confirm_report(report)
    store_report(pipeline.objects, pipeline.reports_bucket, confirmed)
    return confirmed


def handler(event, context=None):
    from emr_sync.runtime import pipeline_from_env

    return handle_reconcile(event, pipeline_from_env())
