"""Synthesize the pipeline stack.

The account and region default to placeholders so `cdk synth` runs without
AWS credentials. Pass `-c account=... -c region=...` for a real deployment.
"""

from __future__ import annotations

import os

import aws_cdk as cdk

from infra.pipeline_stack import PipelineStack

app = cdk.App()
account = app.node.try_get_context("account") or os.environ.get("CDK_DEFAULT_ACCOUNT")
region = app.node.try_get_context("region") or os.environ.get("CDK_DEFAULT_REGION")
PipelineStack(
    app,
    "EmrSyncPipeline",
    env=cdk.Environment(account=account or "111111111111", region=region or "us-east-1"),
)
app.synth()
