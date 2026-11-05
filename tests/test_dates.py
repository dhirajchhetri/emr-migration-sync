from datetime import date, datetime

from emr_sync.dates import parse_date, parse_timestamp, to_fhir_datetime


def test_parses_hl7_and_iso_dates():
    assert parse_timestamp("20261102143045") == datetime(2026, 11, 2, 14, 30, 45)
    assert parse_date("1980-03-15") == date(1980, 3, 15)
    assert parse_date("19800315") == date(1980, 3, 15)


def test_rejects_impossible_dates():
    assert parse_date("19800231") is None
    assert parse_date("not-a-date") is None
    assert parse_date("") is None
    assert parse_date(None) is None


def test_fhir_datetime_is_explicit_utc():
    assert to_fhir_datetime(datetime(2026, 11, 2, 12, 0, 0)) == "2026-11-02T12:00:00Z"
