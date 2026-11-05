import pytest

from emr_sync.aws_backend import DynamoIdempotency

pytest.importorskip("botocore")
from botocore.exceptions import ClientError


class _Table:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def put_item(self, **kwargs):
        key = kwargs["Item"]["pk"]["S"]
        if "ConditionExpression" in kwargs and key in self.items:
            raise ClientError(
                {"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}},
                "PutItem",
            )
        self.items[key] = kwargs["Item"]

    def get_item(self, **kwargs):
        key = kwargs["Key"]["pk"]["S"]
        if key not in self.items:
            return {}
        return {"Item": self.items[key]}

    def delete_item(self, **kwargs):
        self.items.pop(kwargs["Key"]["pk"]["S"], None)

    def update_item(self, **kwargs):
        key = kwargs["Key"]["pk"]["S"]
        values = kwargs["ExpressionAttributeValues"]
        self.items[key]["status"] = values[":status"]
        self.items[key]["detail"] = values[":detail"]


def test_idempotency_reserve_complete_conflict_and_clear():
    store = DynamoIdempotency(_Table(), "emr-sync-idempotency")
    assert store.reserve("PARTNER#MSG1", "hash-a") == "proceed"
    assert store.reserve("PARTNER#MSG1", "hash-a") == "proceed"
    store.complete("PARTNER#MSG1", {"resource_ids": ["pat-1"]})
    assert store.reserve("PARTNER#MSG1", "hash-a") == "duplicate"
    assert store.reserve("PARTNER#MSG1", "hash-b") == "conflict"
    store.clear("PARTNER#MSG1")
    assert store.reserve("PARTNER#MSG1", "hash-b") == "proceed"
    store.reject("PARTNER#MSG1", "INVALID_DOB")
    assert store.client.items["PARTNER#MSG1"]["status"]["S"] == "rejected"
    assert store.reserve("PARTNER#MSG1", "hash-b") == "duplicate"
