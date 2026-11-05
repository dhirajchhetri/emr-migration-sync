"""Build a pipeline from the environment.

`EMR_SYNC_BACKEND=memory` is the default used by tests and the local demo.
`aws` is wired in the AWS adapter and expects the resource names in the
environment.
"""

from __future__ import annotations

import os

from emr_sync.pipeline import Pipeline, build_memory_pipeline


def pipeline_from_env() -> Pipeline:
    backend = os.environ.get("EMR_SYNC_BACKEND", "memory")
    if backend == "memory":
        return build_memory_pipeline()
    if backend == "aws":
        from emr_sync.aws_backend import build_aws_pipeline

        return build_aws_pipeline()
    raise RuntimeError(f"Unknown EMR_SYNC_BACKEND '{backend}'")
