from emr_sync.audit import AuditEvent, AuditLog
from emr_sync.phi import PhiLogger, redact_text


def test_structured_phi_is_dropped_and_patterns_are_masked():
    lines: list[str] = []
    logger = PhiLogger(lines.append)
    logger.info(
        "message.note",
        partner_id="snf01",
        family_name="DOE",
        mrn="MRN1001",
        raw="MSH|^~\\&|X\nPID|1||MRN1001||DOE^JANE",
        detail="call 512-555-0100 or a@b.co about 123-45-6789",
    )
    text = lines[0]
    assert "snf01" in text
    assert "DOE" not in text
    assert "JANE" not in text
    assert "MRN1001" not in text
    assert "512-555-0100" not in text
    assert "a@b.co" not in text
    assert "123-45-6789" not in text
    assert "[redacted]" in text


def test_pid_segment_is_masked_even_inside_a_detail_string():
    masked = redact_text("PID|1||MRN9||DOE^JANE^A")
    assert masked == "PID|[redacted]"
    assert "DOE" not in masked


def test_audit_event_has_no_demographic_fields():
    log = AuditLog()
    event = AuditEvent(
        event_id="evt-1",
        recorded_at="2026-11-02T12:00:00Z",
        action="message.applied",
        partner_id="snf01",
        source_system="PARTNER_EMR",
        message_control_id="MSG00001",
        outcome="applied",
        resource_type="Patient",
        resource_id="pat-abc",
    )
    log.record(event)
    stored = log.as_dicts()[0]
    assert set(stored) == {
        "event_id",
        "recorded_at",
        "action",
        "partner_id",
        "source_system",
        "message_control_id",
        "outcome",
        "reason_code",
        "resource_type",
        "resource_id",
    }
    assert stored["message_control_id"] == "MSG00001"
