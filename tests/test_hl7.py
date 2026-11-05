from datetime import date
from pathlib import Path

import pytest

from emr_sync.errors import MessageRejected
from emr_sync.hl7 import parse_hl7

SAMPLES = Path(__file__).resolve().parents[1] / "samples" / "hl7"


def _read(name: str) -> str:
    return (SAMPLES / name).read_text()


def test_adt_a01_sample_reads_patient_and_visit():
    message = parse_hl7(_read("adt_a01.hl7"))
    assert message.message_type == "ADT"
    assert message.event == "A01"
    assert message.control_id == "MSG00001"
    assert message.source_facility == "SNF01"
    assert message.person.mrn == "MRN1001"
    assert message.person.family_name == "DOE"
    assert message.person.given_name == "JANE"
    assert message.person.middle_name == "A"
    assert message.person.birth_date == date(1980, 3, 15)
    assert message.person.sex == "F"
    assert message.person.city == "AUSTIN"
    assert message.person.postal_code == "78701"
    assert message.person.phone == "5125550100"
    assert message.visit is not None
    assert message.visit.patient_class == "I"
    assert message.visit.visit_number == "VN1001"
    assert message.visit.location == "WARD1^101"
    assert message.visit.attending == "SMITH, ALICE"
    assert message.visit.admit_at is not None


def test_adt_a03_and_a08_samples():
    updated = parse_hl7(_read("adt_a08.hl7"))
    assert updated.event == "A08"
    assert updated.person.phone == "5125550199"
    assert updated.person.address_line == "500 OAK AVE"
    discharged = parse_hl7(_read("adt_a03.hl7"))
    assert discharged.event == "A03"
    assert discharged.visit is not None
    assert discharged.visit.discharge_at is not None


def test_oru_sample_groups_observations_under_the_order():
    message = parse_hl7(_read("oru_r01.hl7"))
    assert message.event == "R01"
    assert len(message.orders) == 1
    order = message.orders[0]
    assert order.service_code == "39125-4"
    assert order.service_system == "LN"
    assert [item.value for item in order.observations] == ["3.2", "1.1"]
    assert order.observations[0].units == "cm"
    assert order.observations[0].code == "39126-2"


def test_invalid_dob_sample_is_rejected_without_echoing_the_value():
    with pytest.raises(MessageRejected) as caught:
        parse_hl7(_read("adt_invalid.hl7"))
    assert caught.value.reason == "INVALID_DOB"
    assert "19801340" not in str(caught.value)


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("EVN|A01", "MISSING_MSH"),
        (
            "MSH|^~\\&|A|B|C|D|20261102||ADT^A04|MSG1|P|2.5\n"
            "PID|1||MRN1^^^X^MR||DOE^JANE||19800315|F\n"
            "PV1|1|I||||||||||||||||VN1",
            "UNSUPPORTED_MESSAGE_TYPE",
        ),
        (
            "MSH|^~\\&|A|B|C|D|20261102||ADT^A01|MSG1|P|2.5\nPV1|1|I||||||||||||||||VN1",
            "MISSING_PID",
        ),
        (
            "MSH|^~\\&|A|B|C|D|20261102||ADT^A01|MSG1|P|2.5\n"
            "PID|1||||DOE^JANE||19800315|F\n"
            "PV1|1|I||||||||||||||||VN1",
            "MISSING_PATIENT_ID",
        ),
        (
            "MSH|^~\\&|A|B|C|D|20261102||ORU^R01|MSG1|P|2.5\n"
            "PID|1||MRN1^^^X^MR||DOE^JANE||19800315|F\n",
            "MISSING_ORDER",
        ),
    ],
)
def test_rejection_reasons(raw: str, reason: str):
    with pytest.raises(MessageRejected) as caught:
        parse_hl7(raw)
    assert caught.value.reason == reason
