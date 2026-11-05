"""Move rejected messages back onto the inbound queue."""

from __future__ import annotations

from emr_sync.pipeline import Pipeline


def handle_replay(event: dict, pipeline: Pipeline) -> dict:
    return {"replayed": pipeline.replay_rejects()}


def handler(event, context=None):
    from emr_sync.runtime import pipeline_from_env

    return handle_replay(event, pipeline_from_env())
