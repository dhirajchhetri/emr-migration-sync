"""S3, SQS, Lambda, Glue, Step Functions, DynamoDB, and CloudWatch.

Raw objects stay versioned. A customer managed key encrypts the data stores
and the queues. Each function gets the actions it calls and no others.
The Glue role still uses the AWS managed Glue service role because the Data
Catalog permission list is long; bucket access is granted only on these buckets.
"""

from __future__ import annotations

from pathlib import Path

from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    Tags,
)
from aws_cdk import aws_apigateway as apigateway
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_assets as assets
from aws_cdk import aws_sqs as sqs
from aws_cdk import aws_stepfunctions as sfn
from constructs import Construct

_CODE = lambda_.Code.from_asset("src", exclude=["**/__pycache__/**", "**/*.pyc"])


class PipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        Tags.of(self).add("project", "emr-migration-sync")

        key = kms.Key(
            self,
            "DataKey",
            alias="emr-sync-data",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
            description="Encrypts raw messages, queues, and the clinical tables.",
        )
        raw = self._bucket("RawBucket", key, versioned=True)
        curated = self._bucket("CuratedBucket", key)
        quarantine = self._bucket("QuarantineBucket", key)
        reports = self._bucket("ReportsBucket", key)

        infra_dlq = sqs.Queue(
            self,
            "InfraDlq",
            queue_name="emr-sync-dlq.fifo",
            fifo=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=key,
            retention_period=Duration.days(14),
        )
        rejects = sqs.Queue(
            self,
            "Rejects",
            queue_name="emr-sync-rejects.fifo",
            fifo=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=key,
            retention_period=Duration.days(14),
        )
        inbound = sqs.Queue(
            self,
            "Inbound",
            queue_name="emr-sync-inbound.fifo",
            fifo=True,
            encryption=sqs.QueueEncryption.KMS,
            encryption_master_key=key,
            visibility_timeout=Duration.seconds(90),
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=5, queue=infra_dlq),
        )

        idempotency = self._table("Idempotency", "emr-sync-idempotency", key)
        audit = self._table("Audit", "emr-sync-audit", key)
        clinical = self._table("Clinical", "emr-sync-clinical", key)
        review = self._table("Review", "emr-sync-review", key)

        environment = {
            "EMR_SYNC_BACKEND": "aws",
            "RAW_BUCKET": raw.bucket_name,
            "CURATED_BUCKET": curated.bucket_name,
            "QUARANTINE_BUCKET": quarantine.bucket_name,
            "REPORTS_BUCKET": reports.bucket_name,
            "INBOUND_QUEUE_URL": inbound.queue_url,
            "REJECTS_QUEUE_URL": rejects.queue_url,
            "IDEMPOTENCY_TABLE": idempotency.table_name,
            "AUDIT_TABLE": audit.table_name,
            "CLINICAL_TABLE": clinical.table_name,
            "REVIEW_TABLE": review.table_name,
        }
        ingest_fn = self._function(
            "Ingest", "emr-sync-ingest", "emr_sync.handlers.ingest.handler", environment
        )
        worker_fn = self._function(
            "Worker", "emr-sync-worker", "emr_sync.handlers.worker.handler", environment
        )
        replay_fn = self._function(
            "Replay", "emr-sync-replay", "emr_sync.handlers.replay.handler", environment
        )
        load_fn = self._function(
            "Load",
            "emr-sync-load",
            "emr_sync.handlers.load.handler",
            environment,
            timeout_seconds=120,
        )
        reconcile_fn = self._function(
            "Reconcile",
            "emr-sync-reconcile",
            "emr_sync.handlers.reconcile.handler",
            environment,
        )
        worker_fn.add_event_source(
            event_sources.SqsEventSource(inbound, batch_size=1, report_batch_item_failures=True)
        )

        raw.grant_put(ingest_fn)
        inbound.grant_send_messages(ingest_fn)
        rejects.grant_send_messages(ingest_fn)
        audit.grant_write_data(ingest_fn)

        raw.grant_read(worker_fn)
        inbound.grant_consume_messages(worker_fn)
        rejects.grant_send_messages(worker_fn)
        for table in (idempotency, audit, clinical, review):
            table.grant_read_write_data(worker_fn)

        inbound.grant_send_messages(replay_fn)
        rejects.grant_consume_messages(replay_fn)
        idempotency.grant_read_write_data(replay_fn)
        audit.grant_write_data(replay_fn)

        curated.grant_read(load_fn)
        reports.grant_put(load_fn)
        clinical.grant_read_write_data(load_fn)
        review.grant_read_write_data(load_fn)
        audit.grant_write_data(load_fn)

        reports.grant_read_write(reconcile_fn)

        for function in (ingest_fn, worker_fn, replay_fn, load_fn, reconcile_fn):
            key.grant_encrypt_decrypt(function)

        api = apigateway.RestApi(self, "IngestApi", rest_api_name="emr-sync-ingest")
        messages = api.root.add_resource("messages")
        messages.add_method("POST", apigateway.LambdaIntegration(ingest_fn))

        script = assets.Asset(self, "NormalizeScript", path="jobs/normalize_facility_extract.py")
        library = assets.Asset(self, "LibrarySource", path="src")
        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )
        script.grant_read(glue_role)
        library.grant_read(glue_role)
        raw.grant_read(glue_role)
        curated.grant_read_write(glue_role)
        quarantine.grant_read_write(glue_role)
        key.grant_encrypt_decrypt(glue_role)
        job = glue.CfnJob(
            self,
            "NormalizeJob",
            name="emr-sync-normalize-facility",
            role=glue_role.role_arn,
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=2,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=script.s3_object_url,
            ),
            default_arguments={
                "--extra-py-files": library.s3_object_url,
                "--enable-continuous-cloudwatch-log": "true",
                "--TempDir": "s3://" + curated.bucket_name + "/tmp/",
            },
        )

        state_role = iam.Role(
            self,
            "MigrationStateRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        state_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun",
                ],
                resources=[
                    f"arn:aws:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:job/emr-sync-normalize-facility"
                ],
            )
        )
        load_fn.grant_invoke(state_role)
        reconcile_fn.grant_invoke(state_role)
        state_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "events:PutRule",
                    "events:PutTargets",
                    "events:DescribeRule",
                    "events:DeleteRule",
                    "events:RemoveTargets",
                ],
                resources=[f"arn:aws:events:{Aws.REGION}:{Aws.ACCOUNT_ID}:rule/StepFunctions*"],
            )
        )
        machine = sfn.CfnStateMachine(
            self,
            "Migration",
            state_machine_name="emr-sync-migration",
            role_arn=state_role.role_arn,
            definition_string=self._definition(load_fn, reconcile_fn),
        )
        machine.node.add_dependency(job)
        machine.node.add_dependency(state_role)

        cloudwatch.Alarm(
            self,
            "RejectsAlarm",
            metric=rejects.metric_approximate_number_of_messages_visible(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="Rejected messages are waiting for replay.",
        )
        cloudwatch.Alarm(
            self,
            "InfraDlqAlarm",
            metric=infra_dlq.metric_approximate_number_of_messages_visible(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            alarm_description="The inbound queue redrive target has messages.",
        )

        CfnOutput(self, "IngestApiUrl", value=api.url)
        CfnOutput(self, "RawBucketName", value=raw.bucket_name)
        CfnOutput(self, "InboundQueueUrl", value=inbound.queue_url)
        CfnOutput(self, "StateMachineArn", value=machine.attr_arn)

    def _bucket(self, construct_id: str, key: kms.Key, versioned: bool = False) -> s3.Bucket:
        return s3.Bucket(
            self,
            construct_id,
            versioned=versioned,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

    def _table(self, construct_id: str, table_name: str, key: kms.Key) -> dynamodb.Table:
        return dynamodb.Table(
            self,
            construct_id,
            table_name=table_name,
            partition_key=dynamodb.Attribute(name="pk", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            encryption=dynamodb.TableEncryption.CUSTOMER_MANAGED,
            encryption_key=key,
            removal_policy=RemovalPolicy.RETAIN,
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
        )

    def _function(
        self,
        construct_id: str,
        function_name: str,
        handler: str,
        environment: dict[str, str],
        timeout_seconds: int = 30,
    ) -> lambda_.Function:
        log_group = logs.LogGroup(
            self,
            construct_id + "Logs",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )
        return lambda_.Function(
            self,
            construct_id,
            function_name=function_name,
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler=handler,
            code=_CODE,
            timeout=Duration.seconds(timeout_seconds),
            memory_size=256,
            environment=environment,
            log_group=log_group,
        )

    def _definition(self, load_fn: lambda_.Function, reconcile_fn: lambda_.Function) -> str:
        text = Path(__file__).with_name("statemachine.asl.json").read_text()
        before, rest = text.split("${LoadFunctionArn}", 1)
        middle, after = rest.split("${ReconcileFunctionArn}", 1)
        return Fn.join("", [before, load_fn.function_arn, middle, reconcile_fn.function_arn, after])
