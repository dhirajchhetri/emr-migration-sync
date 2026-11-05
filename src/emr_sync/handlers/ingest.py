"""API Gateway or direct invoke entry point for the ingest API."""

from __future__ import annotations

import json

from emr_sync.errors import MessageRejected
from emr_sync.pipeline import Pipeline


def handle_ingest(event: dict, pipeline: Pipeline) -> dict:
    if "httpMethod" in event or "requestContext" in event:
        headers = {str(key).lower(): value for key, value in (event.get("headers") or {}).items()}
        raw = event.get("body") or ""
        partner = headers.get("x-partner-id", "")
        source = headers.get("x-source-system") or partner
        try:
            result = pipeline.ingest(partner, source, raw, headers.get("content-type"))
        except MessageRejected as exc:
            return _http(400, {"reason_code": exc.reason})
        status = 202 if result.status in {"queued", "duplicate"} else 503
        return _http(status, result.as_dict())
    try:
        result = pipeline.ingest(
            event["partner_id"],
            event.get("source_system") or event["partner_id"],
            event["raw"],
            event.get("content_type"),
        )
    except MessageRejected as exc:
        return {"reason_code": exc.reason}
    return result.as_dict()


def handler(event, context=None):
    from emr_sync.runtime import pipeline_from_env

    return handle_ingest(event, pipeline_from_env())


def _http(status: int, body: dict) -> dict:
    return {"statusCode": status, "body": json.dumps(body)}
