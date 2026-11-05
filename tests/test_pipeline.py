import json
from pathlib import Path

from emr_sync.handlers.ingest import handle_ingest
from emr_sync.handlers.replay import handle_replay
from emr_sync.handlers.worker import handle_worker
from emr_sync.pipeline import build_memory_pipeline

ROOT = Path(__file__).resolve().parents[1]
HL7 = ROOT / "samples" / "hl7"
NCPDP = ROOT / "samples" / "ncpdp"


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _Flaky:
    def __init__(self, inner, failures: int) -> None:
        self._inner = inner
        self._failures = failures

    def put_resource(self, resource):
        if self._failures:
            self._failures -= 1
            raise RuntimeError("store unavailable")
        self._inner.put_resource(resource)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _text(folder: Path, name: str) -> str:
    return (folder / name).read_text()


def _run(pipeline, partner, source, raw, content_type=None):
    pipeline.ingest(partner, source, raw, content_type)
    return pipeline.process_available()


def test_live_path_upserts_and_links_a_prescription_to_the_same_patient():
    pipeline = build_memory_pipeline()
    _run(pipeline, "snf01", "PARTNER_EMR", _text(HL7, "adt_a01.hl7"))
    _run(pipeline, "snf01", "PARTNER_EMR", _text(HL7, "adt_a08.hl7"))
    _run(pipeline, "snf01", "PARTNER_EMR", _text(HL7, "oru_r01.hl7"))
    _run(pipeline, "erx", "ERX", _text(NCPDP, "newrx.xml"))
    cancel = _run(pipeline, "erx", "ERX", _text(NCPDP, "cancelrx.xml"))
    patients = pipeline.documents.list_resources("Patient")
    assert len(patients) == 1
    systems = {item["system"] for item in patients[0]["identifier"]}
    assert systems == {"urn:mrn:PARTNER_EMR", "urn:mrn:ERX"}
    assert patients[0]["telecom"][0]["value"] == "5125550199"
    encounters = pipeline.documents.list_resources("Encounter")
    assert len(encounters) == 1
    assert encounters[0]["status"] == "in-progress"
    assert pipeline.documents.list_resources("Observation")
    assert pipeline.documents.list_resources("DiagnosticReport")
    medications = pipeline.documents.list_resources("MedicationRequest")
    assert len(medications) == 1
    assert medications[0]["status"] == "cancelled"
    assert medications[0]["subject"]["reference"] == f"Patient/{patients[0]['id']}"
    assert cancel[0].status == "applied"
    log_text = "\n".join(pipeline.logs)
    assert "JANE" not in log_text
    assert "MAIN ST" not in log_text
    assert "5125550199" not in log_text


def test_duplicate_delivery_does_not_create_another_patient():
    pipeline = build_memory_pipeline()
    raw = _text(HL7, "adt_a01.hl7")
    first = pipeline.ingest("snf01", "PARTNER_EMR", raw)
    second = pipeline.ingest("snf01", "PARTNER_EMR", raw)
    assert first.status == "queued"
    assert second.status == "duplicate"
    results = pipeline.process_available()
    assert [item.status for item in results] == ["applied"]
    assert len(pipeline.documents.list_resources("Patient")) == 1
    again = pipeline.process_body(
        {
            "partner_id": "snf01",
            "source_system": "PARTNER_EMR",
            "content_type": "hl7v2",
            "control_id": first.control_id,
            "raw_key": first.raw_key,
            "body_hash": first.raw_key.rsplit("/", 1)[-1].removesuffix(".txt"),
            "group_id": first.group_id,
        }
    )
    assert again.status == "duplicate"
    assert len(pipeline.documents.list_resources("Patient")) == 1


def test_same_control_id_with_a_different_body_is_a_conflict():
    pipeline = build_memory_pipeline()
    original = _text(HL7, "adt_a01.hl7")
    changed = original.replace("5125550100", "5125550111")
    _run(pipeline, "snf01", "PARTNER_EMR", original)
    conflict = _run(pipeline, "snf01", "PARTNER_EMR", changed)
    assert conflict[0].status == "rejected"
    assert conflict[0].reason_code == "CONTROL_ID_CONFLICT"
    patient = pipeline.documents.list_resources("Patient")[0]
    assert patient["telecom"][0]["value"] == "5125550100"
    assert pipeline.queues.depth("rejects") == 1


def test_invalid_message_is_rejected_and_replay_does_not_invent_a_patient():
    pipeline = build_memory_pipeline()
    ingested = pipeline.ingest("snf01", "PARTNER_EMR", _text(HL7, "adt_invalid.hl7"))
    assert ingested.control_id == "MSG00999"
    rejected = pipeline.process_available()
    assert rejected[0].reason_code == "INVALID_DOB"
    assert pipeline.documents.list_resources("Patient") == []
    assert handle_replay({}, pipeline) == {"replayed": 1}
    again = pipeline.process_available()
    assert again[0].reason_code == "INVALID_DOB"
    assert pipeline.documents.list_resources("Patient") == []
    assert pipeline.queues.depth("rejects") == 1


def test_ordering_key_is_stable_per_patient_and_hides_the_mrn():
    pipeline = build_memory_pipeline()
    first = pipeline.ingest("snf01", "PARTNER_EMR", _text(HL7, "adt_a01.hl7"))
    second = pipeline.ingest("snf01", "PARTNER_EMR", _text(HL7, "adt_a08.hl7"))
    other = pipeline.ingest(
        "snf01",
        "PARTNER_EMR",
        _text(HL7, "adt_a01.hl7").replace("MRN1001", "MRN7777").replace("MSG00001", "MSG7777"),
    )
    assert first.group_id == second.group_id
    assert first.group_id != other.group_id
    assert "MRN1001" not in first.group_id
    results = pipeline.process_available()
    applied = [item.control_id for item in results if item.status == "applied"]
    assert applied[:2] == ["MSG00001", "MSG00003"]


def test_uncertain_match_does_not_merge():
    pipeline = build_memory_pipeline()
    _run(pipeline, "snf01", "PARTNER_EMR", _text(HL7, "adt_a01.hl7"))
    variant = _text(HL7, "adt_a01.hl7").replace("MRN1001", "MRN2002").replace("MSG00001", "MSG2002")
    variant = variant.replace("|F|", "|M|", 1)
    results = _run(pipeline, "snf01", "PARTNER_EMR", variant)
    assert results[0].status == "applied"
    assert len(pipeline.documents.list_resources("Patient")) == 2
    reviews = pipeline.documents.list_reviews()
    assert len(reviews) == 1
    assert reviews[0]["status"] == "pending"
    golden = next(
        item
        for item in pipeline.documents.list_resources("Patient")
        if any(ident["value"] == "MRN1001" for ident in item["identifier"])
    )
    assert all(ident["value"] != "MRN2002" for ident in golden["identifier"])
    assert any(event.action == "match.review" for event in pipeline.audit.events)


def test_transient_failure_retries_then_applies():
    pipeline = build_memory_pipeline(max_receives=3)
    pipeline.documents = _Flaky(pipeline.documents, failures=1)
    pipeline.ingest("snf01", "PARTNER_EMR", _text(HL7, "adt_a01.hl7"))
    results = pipeline.process_available()
    assert [item.status for item in results] == ["retry", "applied"]
    assert len(pipeline.documents.list_resources("Patient")) == 1


def test_retries_exhausted_land_on_the_reject_queue():
    pipeline = build_memory_pipeline(max_receives=2)
    pipeline.documents = _Flaky(pipeline.documents, failures=5)
    pipeline.ingest("snf01", "PARTNER_EMR", _text(HL7, "adt_a01.hl7"))
    statuses = []
    statuses.extend(item.status for item in pipeline.process_available())
    statuses.extend(item.status for item in pipeline.process_available())
    assert statuses == ["retry", "rejected"]
    assert pipeline.documents.list_resources("Patient") == []
    assert pipeline.queues.depth("rejects") == 1


def test_open_breaker_keeps_the_raw_object_and_pauses_the_partner():
    clock = _Clock()
    pipeline = build_memory_pipeline(failure_threshold=1, cooldown_seconds=10, breaker_clock=clock)
    pipeline.ingest("snf01", "PARTNER_EMR", _text(HL7, "adt_invalid.hl7"))
    pipeline.process_available()
    paused = pipeline.ingest(
        "snf01",
        "PARTNER_EMR",
        _text(HL7, "adt_a01.hl7"),
    )
    assert paused.status == "circuit_open"
    assert pipeline.objects.get(pipeline.raw_bucket, paused.raw_key).startswith("MSH|")
    assert pipeline.queues.depth("inbound") == 0
    other = pipeline.ingest("hospital", "HOSP", _text(HL7, "adt_a08.hl7"))
    assert other.status == "queued"
    clock.now = 10
    resumed = pipeline.ingest(
        "snf01",
        "PARTNER_EMR",
        _text(HL7, "oru_r01.hl7"),
    )
    assert resumed.status == "queued"


def test_http_ingest_and_sqs_worker_share_the_pipeline():
    pipeline = build_memory_pipeline()
    response = handle_ingest(
        {
            "httpMethod": "POST",
            "headers": {"X-Partner-Id": "snf01", "Content-Type": "application/hl7-v2"},
            "body": _text(HL7, "adt_a01.hl7"),
        },
        pipeline,
    )
    assert response["statusCode"] == 202
    queued = json.loads(response["body"])
    worker = handle_worker(
        {
            "Records": [
                {
                    "messageId": "1",
                    "body": json.dumps(
                        {
                            "partner_id": "snf01",
                            "source_system": "snf01",
                            "content_type": queued["content_type"],
                            "control_id": queued["control_id"],
                            "raw_key": queued["raw_key"],
                            "body_hash": queued["raw_key"].rsplit("/", 1)[-1].removesuffix(".txt"),
                            "group_id": queued["group_id"],
                        }
                    ),
                }
            ]
        },
        pipeline,
    )
    assert worker["batchItemFailures"] == []
    assert len(pipeline.documents.list_resources("Patient")) == 1


def test_unknown_payload_is_stored_and_rejected():
    pipeline = build_memory_pipeline()
    pipeline.ingest("snf01", "PARTNER_EMR", "not a clinical message")
    results = pipeline.process_available()
    assert results[0].reason_code == "UNSUPPORTED_CONTENT_TYPE"
    assert pipeline.objects.buckets["raw"]
