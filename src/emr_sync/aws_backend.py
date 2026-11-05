"""AWS adapters for S3, SQS, and DynamoDB.

Queue deduplication on SQS is silent: a repeated send is accepted and then
dropped by the service. The idempotency table is what makes a replay safe.
`head` then `put` on S3 is good enough for this reference and is not a lock.
"""

from __future__ import annotations

import json
import os

from emr_sync.audit import AuditEvent, AuditLog
from emr_sync.breaker import CircuitBreaker
from emr_sync.phi import PhiLogger
from emr_sync.pipeline import Pipeline


class S3Objects:
    def __init__(self, client) -> None:
        self.client = client

    def put_if_absent(self, bucket: str, key: str, body: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return False
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
                raise
        self.put(bucket, key, body)
        return True

    def put(self, bucket: str, key: str, body: str) -> None:
        self.client.put_object(Bucket=bucket, Key=key, Body=body.encode())

    def get(self, bucket: str, key: str) -> str:
        from botocore.exceptions import ClientError

        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            raise KeyError(key) from exc
        return response["Body"].read().decode()


class SqsQueues:
    def __init__(self, client, urls: dict[str, str]) -> None:
        self.client = client
        self.urls = urls
        self.inflight: dict[str, dict] = {}

    def send(self, queue: str, body: dict, group_id: str, dedup_id: str) -> bool:
        self.client.send_message(
            QueueUrl=self.urls[queue],
            MessageBody=json.dumps(body),
            MessageGroupId=group_id[:128],
            MessageDeduplicationId=dedup_id[:128],
        )
        return True

    def receive(self, queue: str) -> dict | None:
        response = self.client.receive_message(
            QueueUrl=self.urls[queue],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            MessageSystemAttributeNames=["All"],
        )
        messages = response.get("Messages") or []
        if not messages:
            return None
        message = messages[0]
        attributes = message.get("Attributes") or {}
        parsed = {
            "body": json.loads(message["Body"]),
            "group_id": attributes.get("MessageGroupId", ""),
            "dedup_id": attributes.get("MessageDeduplicationId", ""),
            "receipt": message["ReceiptHandle"],
            "receive_count": int(attributes.get("ApproximateReceiveCount", "1")),
        }
        self.inflight[parsed["receipt"]] = parsed
        return parsed

    def delete(self, queue: str, receipt: str) -> None:
        self.inflight.pop(receipt, None)
        self.client.delete_message(QueueUrl=self.urls[queue], ReceiptHandle=receipt)

    def release(self, queue: str, receipt: str, dead_letter: str, max_receives: int) -> str:
        message = self.inflight.get(receipt)
        count = message["receive_count"] if message else max_receives
        if count >= max_receives and message is not None:
            self.send(
                dead_letter,
                message["body"],
                message["group_id"] or "replay",
                message["dedup_id"] or receipt[:80],
            )
            self.delete(queue, receipt)
            return "dead_lettered"
        self.client.change_message_visibility(
            QueueUrl=self.urls[queue],
            ReceiptHandle=receipt,
            VisibilityTimeout=0,
        )
        return "retried"

    def depth(self, queue: str) -> int:
        response = self.client.get_queue_attributes(
            QueueUrl=self.urls[queue],
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])


class DynamoIdempotency:
    def __init__(self, client, table: str) -> None:
        self.client = client
        self.table = table

    def reserve(self, key: str, body_hash: str) -> str:
        from botocore.exceptions import ClientError

        try:
            self.client.put_item(
                TableName=self.table,
                Item={
                    "pk": {"S": key},
                    "body_hash": {"S": body_hash},
                    "status": {"S": "inflight"},
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return "proceed"
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        current = self._get(key)
        if current is None:
            return "proceed"
        if current["body_hash"] != body_hash:
            return "conflict"
        if current["status"] in {"complete", "rejected"}:
            return "duplicate"
        return "proceed"

    def complete(self, key: str, summary: dict) -> None:
        self._set(key, "complete", json.dumps(summary))

    def reject(self, key: str, reason: str) -> None:
        if self._get(key) is None:
            self.client.put_item(
                TableName=self.table,
                Item={
                    "pk": {"S": key},
                    "body_hash": {"S": ""},
                    "status": {"S": "rejected"},
                    "reason": {"S": reason},
                },
            )
            return
        self._set(key, "rejected", reason)

    def clear(self, key: str) -> None:
        self.client.delete_item(TableName=self.table, Key={"pk": {"S": key}})

    def _get(self, key: str) -> dict | None:
        response = self.client.get_item(TableName=self.table, Key={"pk": {"S": key}})
        item = response.get("Item")
        if not item:
            return None
        return {
            "body_hash": item.get("body_hash", {}).get("S", ""),
            "status": item.get("status", {}).get("S", ""),
        }

    def _set(self, key: str, status: str, detail: str) -> None:
        self.client.update_item(
            TableName=self.table,
            Key={"pk": {"S": key}},
            UpdateExpression="SET #status = :status, detail = :detail",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": {"S": status},
                ":detail": {"S": detail},
            },
        )


class DynamoDocuments:
    def __init__(self, client, clinical_table: str, review_table: str) -> None:
        self.client = client
        self.clinical_table = clinical_table
        self.review_table = review_table
        self.checkpoints = CheckpointMap(self)

    def put_resource(self, resource: dict) -> None:
        self._put(
            self.clinical_table,
            f"{resource['resourceType']}/{resource['id']}",
            resource["resourceType"],
            resource,
        )

    def get_resource(self, resource_type: str, resource_id: str) -> dict | None:
        item = self._get(self.clinical_table, f"{resource_type}/{resource_id}")
        if item is None:
            return None
        return json.loads(item)

    def list_resources(self, resource_type: str) -> list[dict]:
        return [json.loads(item) for item in self._scan(self.clinical_table, resource_type)]

    def put_review(self, review: dict) -> None:
        self._put(self.review_table, review["id"], "MatchReview", review)

    def list_reviews(self) -> list[dict]:
        return [json.loads(item) for item in self._scan(self.review_table, "MatchReview")]

    def save_checkpoint(self, token: str, sequence: int) -> None:
        self.client.put_item(
            TableName=self.clinical_table,
            Item={
                "pk": {"S": f"checkpoint#{token}"},
                "resource_type": {"S": "Checkpoint"},
                "sequence": {"N": str(sequence)},
            },
        )

    def load_checkpoints(self) -> dict[str, int]:
        found: dict[str, int] = {}
        for item in self._scan_raw(self.clinical_table, "Checkpoint"):
            token = item["pk"]["S"].split("#", 1)[1]
            found[token] = int(item["sequence"]["N"])
        return found

    def _put(self, table: str, key: str, kind: str, document: dict) -> None:
        self.client.put_item(
            TableName=table,
            Item={
                "pk": {"S": key},
                "resource_type": {"S": kind},
                "document": {"S": json.dumps(document)},
            },
        )

    def _get(self, table: str, key: str) -> str | None:
        response = self.client.get_item(TableName=table, Key={"pk": {"S": key}})
        item = response.get("Item")
        if not item:
            return None
        return item["document"]["S"]

    def _scan(self, table: str, kind: str) -> list[str]:
        return [item["document"]["S"] for item in self._scan_raw(table, kind)]

    def _scan_raw(self, table: str, kind: str) -> list[dict]:
        items: list[dict] = []
        kwargs: dict = {
            "TableName": table,
            "FilterExpression": "resource_type = :kind",
            "ExpressionAttributeValues": {":kind": {"S": kind}},
        }
        while True:
            response = self.client.scan(**kwargs)
            items.extend(response.get("Items", []))
            token = response.get("LastEvaluatedKey")
            if not token:
                return items
            kwargs["ExclusiveStartKey"] = token


class CheckpointMap(dict):
    def __init__(self, store: DynamoDocuments) -> None:
        super().__init__(store.load_checkpoints())
        self.store = store

    def __setitem__(self, key: str, value: int) -> None:
        super().__setitem__(key, value)
        self.store.save_checkpoint(key, int(value))


class DynamoAudit(AuditLog):
    def __init__(self, client, table: str) -> None:
        super().__init__()
        self.client = client
        self.table = table

    def record(self, event: AuditEvent) -> None:
        super().record(event)
        self.client.put_item(
            TableName=self.table,
            Item={
                "pk": {"S": event.event_id},
                "document": {"S": json.dumps(event.as_dict())},
            },
        )


def build_aws_pipeline() -> Pipeline:
    import boto3

    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    client_kwargs = {"region_name": region}
    if endpoint:
        client_kwargs["endpoint_url"] = endpoint
    session = boto3.session.Session()
    s3 = session.client("s3", **client_kwargs)
    sqs = session.client("sqs", **client_kwargs)
    dynamodb = session.client("dynamodb", **client_kwargs)
    inbound = _queue_url(sqs, "INBOUND_QUEUE_URL", "INBOUND_QUEUE_NAME", "emr-sync-inbound.fifo")
    rejects = _queue_url(sqs, "REJECTS_QUEUE_URL", "REJECTS_QUEUE_NAME", "emr-sync-rejects.fifo")
    logs: list[str] = []
    return Pipeline(
        objects=S3Objects(s3),
        queues=SqsQueues(sqs, {"inbound": inbound, "rejects": rejects}),
        idempotency=DynamoIdempotency(
            dynamodb, os.environ.get("IDEMPOTENCY_TABLE", "emr-sync-idempotency")
        ),
        documents=DynamoDocuments(
            dynamodb,
            os.environ.get("CLINICAL_TABLE", "emr-sync-clinical"),
            os.environ.get("REVIEW_TABLE", "emr-sync-review"),
        ),
        audit=DynamoAudit(dynamodb, os.environ.get("AUDIT_TABLE", "emr-sync-audit")),
        logger=PhiLogger(logs.append),
        breaker=CircuitBreaker(),
        raw_bucket=os.environ.get("RAW_BUCKET", "emr-sync-raw"),
        reports_bucket=os.environ.get("REPORTS_BUCKET", "emr-sync-reports"),
    )


def _queue_url(client, url_env: str, name_env: str, default_name: str) -> str:
    configured = os.environ.get(url_env)
    if configured:
        return configured
    name = os.environ.get(name_env, default_name)
    return client.get_queue_url(QueueName=name)["QueueUrl"]
