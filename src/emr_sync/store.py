"""In-memory stand-ins for S3, SQS, DynamoDB, and the clinical store.

The same pipeline object talks to these during tests and to the AWS adapters
when a deployment is configured. Copies are returned on read so a caller can
edit a resource and put it back without mutating the stored document.
"""

from __future__ import annotations

import copy


class MemoryObjects:
    def __init__(self) -> None:
        self.buckets: dict[str, dict[str, str]] = {}

    def put_if_absent(self, bucket: str, key: str, body: str) -> bool:
        slot = self.buckets.setdefault(bucket, {})
        if key in slot:
            return False
        slot[key] = body
        return True

    def put(self, bucket: str, key: str, body: str) -> None:
        self.buckets.setdefault(bucket, {})[key] = body

    def get(self, bucket: str, key: str) -> str:
        try:
            return self.buckets[bucket][key]
        except KeyError as exc:
            raise KeyError(key) from exc


class MemoryQueue:
    """A single-consumer FIFO queue. Deduplication ids collapse resends."""

    def __init__(self) -> None:
        self.messages: dict[str, list[dict]] = {}
        self.seen: dict[str, set[str]] = {}
        self.inflight: dict[str, dict[str, dict]] = {}
        self._sequence = 0

    def send(self, queue: str, body: dict, group_id: str, dedup_id: str) -> bool:
        seen = self.seen.setdefault(queue, set())
        if dedup_id in seen:
            return False
        seen.add(dedup_id)
        self._sequence += 1
        self.messages.setdefault(queue, []).append(
            {
                "body": copy.deepcopy(body),
                "group_id": group_id,
                "dedup_id": dedup_id,
                "receipt": f"r{self._sequence}",
                "receive_count": 0,
            }
        )
        return True

    def receive(self, queue: str) -> dict | None:
        pending = self.messages.get(queue) or []
        if not pending:
            return None
        message = pending.pop(0)
        message["receive_count"] += 1
        self.inflight.setdefault(queue, {})[message["receipt"]] = message
        return message

    def delete(self, queue: str, receipt: str) -> None:
        self.inflight.get(queue, {}).pop(receipt, None)

    def release(self, queue: str, receipt: str, dead_letter: str, max_receives: int) -> str:
        message = self.inflight[queue].pop(receipt)
        if message["receive_count"] >= max_receives:
            self.messages.setdefault(dead_letter, []).append(message)
            return "dead_lettered"
        self.messages[queue].append(message)
        return "retried"

    def depth(self, queue: str) -> int:
        return len(self.messages.get(queue) or [])


class MemoryIdempotency:
    def __init__(self) -> None:
        self.items: dict[str, dict] = {}

    def reserve(self, key: str, body_hash: str) -> str:
        current = self.items.get(key)
        if current is None:
            self.items[key] = {
                "body_hash": body_hash,
                "status": "inflight",
                "summary": None,
                "reason": None,
            }
            return "proceed"
        if current["body_hash"] != body_hash:
            return "conflict"
        if current["status"] in {"complete", "rejected"}:
            return "duplicate"
        return "proceed"

    def complete(self, key: str, summary: dict) -> None:
        self.items[key]["status"] = "complete"
        self.items[key]["summary"] = summary

    def reject(self, key: str, reason: str) -> None:
        current = self.items.setdefault(
            key,
            {"body_hash": "", "status": "rejected", "summary": None, "reason": reason},
        )
        current["status"] = "rejected"
        current["reason"] = reason

    def clear(self, key: str) -> None:
        self.items.pop(key, None)


class MemoryDocuments:
    def __init__(self) -> None:
        self.resources: dict[str, dict] = {}
        self.reviews: dict[str, dict] = {}
        self.checkpoints: dict[str, int] = {}

    def put_resource(self, resource: dict) -> None:
        key = f"{resource['resourceType']}/{resource['id']}"
        self.resources[key] = copy.deepcopy(resource)

    def get_resource(self, resource_type: str, resource_id: str) -> dict | None:
        found = self.resources.get(f"{resource_type}/{resource_id}")
        if found is None:
            return None
        return copy.deepcopy(found)

    def list_resources(self, resource_type: str) -> list[dict]:
        prefix = f"{resource_type}/"
        return [
            copy.deepcopy(value) for key, value in self.resources.items() if key.startswith(prefix)
        ]

    def put_review(self, review: dict) -> None:
        self.reviews[review["id"]] = copy.deepcopy(review)

    def list_reviews(self) -> list[dict]:
        return [copy.deepcopy(value) for value in self.reviews.values()]
