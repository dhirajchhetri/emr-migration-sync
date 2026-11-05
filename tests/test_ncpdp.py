from datetime import date
from pathlib import Path

import pytest

from emr_sync.errors import MessageRejected
from emr_sync.ncpdp import parse_script

SAMPLES = Path(__file__).resolve().parents[1] / "samples" / "ncpdp"


def _read(name: str) -> str:
    return (SAMPLES / name).read_text()


def test_newrx_normalizes_ndc_and_patient():
    message = parse_script(_read("newrx.xml"))
    assert message.kind == "NewRx"
    assert message.message_id == "RX1001"
    assert message.person.mrn == "MRN1001"
    assert message.person.family_name == "DOE"
    assert message.person.birth_date == date(1980, 3, 15)
    assert message.medication is not None
    assert message.medication.ndc == "00456012301"
    assert message.medication.rxnorm == "979334"
    assert message.medication.order_number == "PON1001"
    assert message.medication.sig.startswith("Apply")


def test_cancel_renewal_fill_and_history():
    cancelled = parse_script(_read("cancelrx.xml"))
    assert cancelled.kind == "CancelRx"
    assert cancelled.relates_to == "RX1001"
    renewal = parse_script(_read("rxrenewal.xml"))
    assert renewal.kind == "RxRenewalRequest"
    assert renewal.medication is not None
    assert renewal.medication.ndc == "00456012301"
    assert renewal.medication.sig.startswith("Apply")
    filled = parse_script(_read("rxfill.xml"))
    assert filled.kind == "RxFill"
    history = parse_script(_read("history.xml"))
    assert history.kind == "RxHistoryResponse"
    assert len(history.medications) == 2
    assert {item.description for item in history.medications} == {
        "Collagenase ointment",
        "Silver sulfadiazine 1% cream",
    }


def test_namespaced_message_uses_local_names():
    raw = """
    <Message xmlns="urn:example:script">
      <Header><MessageId>RX9</MessageId></Header>
      <Body>
        <NewRx>
          <Patient>
            <MedicalRecordNumber>MRN9</MedicalRecordNumber>
            <LastName>NG</LastName>
            <FirstName>MINH</FirstName>
            <DateOfBirth>1992-07-19</DateOfBirth>
            <Gender>F</Gender>
          </Patient>
          <Medication>
            <DrugDescription>Example ointment</DrugDescription>
          </Medication>
        </NewRx>
      </Body>
    </Message>
    """
    message = parse_script(raw)
    assert message.message_id == "RX9"
    assert message.person.mrn == "MRN9"


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        ("<Message><Header></Header><Body><NewRx/></Body></Message>", "MISSING_MESSAGE_ID"),
        ("this is not xml", "MALFORMED_XML"),
        (
            "<Message><Header><MessageId>X</MessageId></Header><Body><Refill/></Body></Message>",
            "UNSUPPORTED_SCRIPT_TYPE",
        ),
        (
            """
            <Message><Header><MessageId>X</MessageId></Header><Body>
              <CancelRx>
                <Patient>
                  <MedicalRecordNumber>MRN1</MedicalRecordNumber>
                  <LastName>DOE</LastName><FirstName>JANE</FirstName>
                  <DateOfBirth>1980-03-15</DateOfBirth><Gender>F</Gender>
                </Patient>
              </CancelRx>
            </Body></Message>
            """,
            "MISSING_PRESCRIPTION_REFERENCE",
        ),
    ],
)
def test_script_rejection_reasons(raw: str, reason: str):
    with pytest.raises(MessageRejected) as caught:
        parse_script(raw)
    assert caught.value.reason == reason
