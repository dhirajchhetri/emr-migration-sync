"""SQS consumer. Permanent rejects stay in the application reject queue.

Unexpected failures are reported as batch item failures so the queue can
retry them, and then redrive them after the receive limit.
"""

from __future__ import annotations

import json

from emr_sync.errors import TransientFailure
from emr_sync.pipeline import Pipeline


def handle_worker(event: dict, pipeline: Pipeline) -> dict:
    records = event.get("Records")
    if not records:
        results = pipeline.process_available(int(event.get("max_messages", 10)))
        return {"results": [item.as_dict() for item in results]}
    failures = []
    for record in records:
        try:
            pipeline.process_body(json.loads(record["body"]))
        except TransientFailure:
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}


def handler(event, context=None):
    from emr_sync.runtime import pipeline_from_env

    return handle_worker(event, pipeline_from_env())
