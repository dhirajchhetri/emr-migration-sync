"""A directory-backed pipeline for local commands.

State is a single JSON file. It is the stand-in for the AWS resources when
you want `ingest` and `work` to survive separate processes without LocalStack.
The file holds synthetic sample data; it is not a place to put a real extract.
"""

from __future__ import annotations

import json
from pathlib import Path

from emr_sync.audit import AuditEvent, AuditLog
from emr_sync.breaker import CircuitBreaker
from emr_sync.phi import PhiLogger
from emr_sync.pipeline import Pipeline
from emr_sync.store import MemoryDocuments, MemoryIdempotency, MemoryObjects, MemoryQueue


class DirectoryState:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.objects = MemoryObjects()
        self.queues = MemoryQueue()
        self.idempotency = MemoryIdempotency()
        self.documents = MemoryDocuments()
        self.audit = AuditLog()
        self.logs: list[str] = []
        self.load()

    def load(self) -> None:
        path = self.root / "state.json"
        if not path.exists():
            return
        data = json.loads(path.read_text())
        self.objects.buckets = data["buckets"]
        self.queues.messages = data["messages"]
        self.queues.seen = {name: set(values) for name, values in data["seen"].items()}
        self.queues.inflight = data.get("inflight") or {}
        self.queues._sequence = data.get("queue_sequence", 0)
        self.idempotency.items = data["idempotency"]
        self.documents.resources = data["resources"]
        self.documents.reviews = data["reviews"]
        self.documents.checkpoints = {key: int(value) for key, value in data["checkpoints"].items()}
        self.audit.events = [AuditEvent(**item) for item in data["audit"]]
        self.logs = list(data.get("logs") or [])

    def save(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        payload = {
            "buckets": self.objects.buckets,
            "messages": self.queues.messages,
            "seen": {name: sorted(values) for name, values in self.queues.seen.items()},
            "inflight": self.queues.inflight,
            "queue_sequence": self.queues._sequence,
            "idempotency": self.idempotency.items,
            "resources": self.documents.resources,
            "reviews": self.documents.reviews,
            "checkpoints": self.documents.checkpoints,
            "audit": self.audit.as_dicts(),
            "logs": self.logs,
        }
        (self.root / "state.json").write_text(json.dumps(payload))


def build_directory_pipeline(root: str | Path) -> Pipeline:
    state = DirectoryState(Path(root))
    pipeline = Pipeline(
        objects=state.objects,
        queues=state.queues,
        idempotency=state.idempotency,
        documents=state.documents,
        audit=state.audit,
        logger=PhiLogger(state.logs.append),
        breaker=CircuitBreaker(),
        persister=state.save,
    )
    pipeline.logs = state.logs
    return pipeline
