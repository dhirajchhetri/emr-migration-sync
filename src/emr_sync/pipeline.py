"""Ingest, translate, and replay.

Ingest writes the raw payload first, then queues a pointer. The worker loads
that object, reserves an idempotency key of source system plus control id, and
applies the message. A second delivery with the same body is a no-op. A second
body that reuses the control id is rejected and the first apply stands.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from emr_sync.apply import apply_hl7, apply_script
from emr_sync.audit import AuditEvent, AuditLog
from emr_sync.breaker import CircuitBreaker
from emr_sync.errors import MessageRejected, TransientFailure
from emr_sync.hl7 import parse_hl7
from emr_sync.ids import group_key
from emr_sync.ncpdp import parse_script
from emr_sync.phi import PhiLogger
from emr_sync.store import MemoryDocuments, MemoryIdempotency, MemoryObjects, MemoryQueue

_TOKEN = re.compile(r"[^A-Za-z0-9_-]")


@dataclass
class IngestResult:
    status: str
    control_id: str
    raw_key: str
    group_id: str
    content_type: str

    def as_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProcessResult:
    status: str
    control_id: str
    reason_code: str | None = None
    resource_ids: tuple[str, ...] = ()

    def as_dict(self) -> dict:
        return {
            "status": self.status,
            "control_id": self.control_id,
            "reason_code": self.reason_code,
            "resource_ids": list(self.resource_ids),
        }


class Pipeline:
    def __init__(
        self,
        objects,
        queues,
        idempotency,
        documents,
        audit,
        logger: PhiLogger,
        breaker: CircuitBreaker,
        *,
        raw_bucket: str = "raw",
        reports_bucket: str = "reports",
        inbound_queue: str = "inbound",
        rejects_queue: str = "rejects",
        max_receives: int = 3,
        clock=None,
        persister=None,
    ) -> None:
        self.objects = objects
        self.queues = queues
        self.idempotency = idempotency
        self.documents = documents
        self.audit = audit
        self.logger = logger
        self.breaker = breaker
        self.raw_bucket = raw_bucket
        self.reports_bucket = reports_bucket
        self.inbound_queue = inbound_queue
        self.rejects_queue = rejects_queue
        self.max_receives = max_receives
        self.clock = clock or (lambda: datetime.now(UTC))
        self.persister = persister
        self._sequence = 0
        self.logs: list[str] = []

    def ingest(
        self,
        partner_id: str,
        source_system: str,
        raw: str,
        content_type: str | None = None,
    ) -> IngestResult:
        if not partner_id or not source_system:
            raise MessageRejected("MISSING_PARTNER")
        kind, control_id, mrn = sniff(raw, content_type)
        body_hash = hashlib.sha256(raw.encode()).hexdigest()
        if not control_id:
            control_id = "unparsed-" + body_hash[:16]
        raw_key = f"raw/{partner_id}/{control_id}/{body_hash}.txt"
        self.objects.put_if_absent(self.raw_bucket, raw_key, raw)
        ordering = group_key(partner_id, mrn or control_id)
        body = {
            "partner_id": partner_id,
            "source_system": source_system,
            "content_type": kind,
            "control_id": control_id,
            "raw_key": raw_key,
            "body_hash": body_hash,
            "group_id": ordering,
        }
        self._audit("message.stored", partner_id, source_system, control_id, "stored")
        if not self.breaker.allow(partner_id):
            self._send_reject(body, "PARTNER_CIRCUIT_OPEN")
            self._audit(
                "message.circuit_open",
                partner_id,
                source_system,
                control_id,
                "circuit_open",
                reason="PARTNER_CIRCUIT_OPEN",
            )
            self._flush()
            return IngestResult("circuit_open", control_id, raw_key, ordering, kind)
        dedup = _token(f"{source_system}-{control_id}-{body_hash[:24]}")
        queued = self.queues.send(self.inbound_queue, body, ordering, dedup)
        status = "queued" if queued else "duplicate"
        action = "message.queued" if queued else "message.duplicate"
        self._audit(action, partner_id, source_system, control_id, status)
        self._flush()
        return IngestResult(status, control_id, raw_key, ordering, kind)

    def process_body(self, body: dict) -> ProcessResult:
        partner = body["partner_id"]
        source = body["source_system"]
        control = body["control_id"]
        key = f"{source}#{control}"
        try:
            raw = self.objects.get(self.raw_bucket, body["raw_key"])
        except KeyError as exc:
            self.breaker.record_failure(partner)
            raise TransientFailure("raw object missing") from exc
        reservation = self.idempotency.reserve(key, body["body_hash"])
        if reservation == "duplicate":
            self.breaker.record_success(partner)
            self._audit("message.duplicate", partner, source, control, "duplicate")
            return ProcessResult("duplicate", control)
        if reservation == "conflict":
            self._send_reject(body, "CONTROL_ID_CONFLICT")
            self._audit(
                "message.rejected",
                partner,
                source,
                control,
                "rejected",
                reason="CONTROL_ID_CONFLICT",
            )
            return ProcessResult("rejected", control, "CONTROL_ID_CONFLICT")
        try:
            outcome = self._translate(raw, body)
        except MessageRejected as exc:
            self.idempotency.reject(key, exc.reason)
            self._send_reject(body, exc.reason)
            self.breaker.record_failure(partner)
            self._audit(
                "message.rejected",
                partner,
                source,
                control,
                "rejected",
                reason=exc.reason,
            )
            return ProcessResult("rejected", control, exc.reason)
        except TransientFailure:
            self.breaker.record_failure(partner)
            raise
        except Exception as exc:
            self.breaker.record_failure(partner)
            raise TransientFailure("translation failed") from exc
        self.idempotency.complete(key, {"resource_ids": list(outcome.resource_ids)})
        self.breaker.record_success(partner)
        self._audit(
            "message.applied",
            partner,
            source,
            control,
            "applied",
            resource_type="Patient",
            resource_id=outcome.patient_id,
        )
        if outcome.review_id:
            self._audit(
                "match.review",
                partner,
                source,
                control,
                "review",
                resource_type="MatchReview",
                resource_id=outcome.review_id,
            )
        return ProcessResult("applied", control, resource_ids=outcome.resource_ids)

    def process_available(self, max_messages: int = 10) -> list[ProcessResult]:
        results: list[ProcessResult] = []
        for _ in range(max_messages):
            message = self.queues.receive(self.inbound_queue)
            if message is None:
                break
            try:
                result = self.process_body(message["body"])
            except TransientFailure:
                action = self.queues.release(
                    self.inbound_queue,
                    message["receipt"],
                    self.rejects_queue,
                    self.max_receives,
                )
                control = message["body"]["control_id"]
                if action == "dead_lettered":
                    key = f"{message['body']['source_system']}#{control}"
                    self.idempotency.reject(key, "RETRIES_EXHAUSTED")
                    self._audit(
                        "message.rejected",
                        message["body"]["partner_id"],
                        message["body"]["source_system"],
                        control,
                        "rejected",
                        reason="RETRIES_EXHAUSTED",
                    )
                    results.append(ProcessResult("rejected", control, "RETRIES_EXHAUSTED"))
                else:
                    results.append(ProcessResult("retry", control))
                continue
            self.queues.delete(self.inbound_queue, message["receipt"])
            results.append(result)
        self._flush()
        return results

    def replay_rejects(self) -> int:
        count = 0
        while True:
            message = self.queues.receive(self.rejects_queue)
            if message is None:
                break
            body = dict(message["body"])
            key = f"{body['source_system']}#{body['control_id']}"
            self.idempotency.clear(key)
            count += 1
            dedup = _token(f"replay-{body['body_hash']}-{count}-{self._next()}")
            ordering = body.get("group_id") or group_key(body["partner_id"], body["control_id"])
            self.queues.send(self.inbound_queue, body, ordering, dedup)
            self.queues.delete(self.rejects_queue, message["receipt"])
            self._audit(
                "message.replayed",
                body["partner_id"],
                body["source_system"],
                body["control_id"],
                "replayed",
            )
        self._flush()
        return count

    def _translate(self, raw: str, body: dict):
        if body["content_type"] == "hl7v2":
            return apply_hl7(
                self.documents, parse_hl7(raw), body["source_system"], body["partner_id"]
            )
        if body["content_type"] == "ncpdp":
            return apply_script(
                self.documents, parse_script(raw), body["source_system"], body["partner_id"]
            )
        raise MessageRejected("UNSUPPORTED_CONTENT_TYPE")

    def _send_reject(self, body: dict, reason: str) -> None:
        payload = dict(body)
        payload["reason_code"] = reason
        self._sequence += 1
        dedup = _token(f"reject-{body['body_hash']}-{reason}-{self._sequence}")
        ordering = body.get("group_id") or group_key(body["partner_id"], body["control_id"])
        self.queues.send(self.rejects_queue, payload, ordering, dedup)

    def _audit(
        self,
        action: str,
        partner_id: str,
        source_system: str,
        control_id: str | None,
        outcome: str,
        reason: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
    ) -> None:
        self.audit.record(
            AuditEvent(
                event_id="evt-" + uuid.uuid4().hex[:12],
                recorded_at=_stamp(self.clock()),
                action=action,
                partner_id=partner_id,
                source_system=source_system,
                message_control_id=control_id,
                outcome=outcome,
                reason_code=reason,
                resource_type=resource_type,
                resource_id=resource_id,
            )
        )
        self.logger.info(
            action,
            partner_id=partner_id,
            source_system=source_system,
            message_control_id=control_id or "",
            outcome=outcome,
            reason_code=reason or "",
            resource_type=resource_type or "",
            resource_id=resource_id or "",
        )

    def _flush(self) -> None:
        if self.persister is not None:
            self.persister()

    def _next(self) -> int:
        self._sequence += 1
        return self._sequence


def build_memory_pipeline(**kwargs) -> Pipeline:
    logs: list[str] = []
    pipeline = Pipeline(
        objects=MemoryObjects(),
        queues=MemoryQueue(),
        idempotency=MemoryIdempotency(),
        documents=MemoryDocuments(),
        audit=kwargs.get("audit") or AuditLog(),
        logger=PhiLogger(logs.append),
        breaker=CircuitBreaker(
            threshold=kwargs.get("failure_threshold", 5),
            cooldown_seconds=kwargs.get("cooldown_seconds", 30.0),
            clock=kwargs.get("breaker_clock"),
        ),
        raw_bucket=kwargs.get("raw_bucket", "raw"),
        reports_bucket=kwargs.get("reports_bucket", "reports"),
        inbound_queue=kwargs.get("inbound_queue", "inbound"),
        rejects_queue=kwargs.get("rejects_queue", "rejects"),
        max_receives=kwargs.get("max_receives", 3),
        clock=kwargs.get("clock"),
        persister=kwargs.get("persister"),
    )
    pipeline.logs = logs
    return pipeline


def sniff(raw: str, content_type: str | None) -> tuple[str, str | None, str | None]:
    text = raw.lstrip("\ufeff").lstrip()
    hinted = _kind(content_type)
    if hinted == "hl7v2" or (hinted is None and text.startswith("MSH")):
        try:
            message = parse_hl7(text)
        except MessageRejected:
            return "hl7v2", _msh_control(text), None
        return "hl7v2", message.control_id, message.person.mrn
    if hinted == "ncpdp" or (hinted is None and text.startswith("<")):
        try:
            message = parse_script(text)
        except MessageRejected:
            return "ncpdp", _xml_id(text), None
        return "ncpdp", message.message_id, message.person.mrn
    return "unknown", None, None


def _kind(content_type: str | None) -> str | None:
    if not content_type:
        return None
    lowered = content_type.lower()
    if "hl7" in lowered:
        return "hl7v2"
    if "xml" in lowered or "script" in lowered or "ncpdp" in lowered:
        return "ncpdp"
    return None


def _msh_control(text: str) -> str | None:
    line = text.splitlines()[0] if text else ""
    parts = line.split("|")
    if len(parts) > 9 and parts[9].strip():
        return parts[9].strip()
    return None


def _xml_id(text: str) -> str | None:
    match = re.search(r"<MessageId>\s*([^<]+?)\s*</MessageId>", text)
    if match:
        return match.group(1)
    return None


def _token(value: str) -> str:
    cleaned = _TOKEN.sub("", value)[:48]
    digest = hashlib.sha256(value.encode()).hexdigest()[:32]
    return f"{cleaned}-{digest}"[:128]


def _stamp(value) -> str:
    if isinstance(value, datetime):
        current = value.astimezone(UTC) if value.tzinfo else value
        return current.strftime("%Y-%m-%dT%H:%M:%SZ")
    return str(value)
