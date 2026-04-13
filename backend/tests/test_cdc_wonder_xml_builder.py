"""Tests for the CDC Wonder XML request body builder."""

from backend.etl.cdc_wonder.xml_builder import build_request_xml
from backend.etl.cdc_wonder.constants import DB_UCD_2018_2023, DB_UCD_1999_2020


def test_build_request_returns_string():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2019,
        icd_codes=["I00-I99"], age_groups=["25-34", "35-44"],
    )
    assert isinstance(xml, str)
    assert xml.startswith("<?xml")


def test_build_request_contains_group_by_county():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2019,
        icd_codes=["I00-I99"], age_groups=["25-34"],
    )
    assert "county" in xml.lower() or "V1-level2" in xml


def test_build_request_contains_year():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2022,
        icd_codes=["I00-I99"], age_groups=["25-34"],
    )
    assert "2022" in xml


def test_build_request_contains_icd_codes():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2019,
        icd_codes=["I20-I25"], age_groups=["25-34"],
    )
    assert "I20-I25" in xml


def test_build_request_contains_all_age_groups():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2019,
        icd_codes=["I00-I99"], age_groups=["25-34", "35-44", "45-54"],
    )
    for age in ["25-34", "35-44", "45-54"]:
        assert age in xml


def test_build_request_accepts_assurance_flag():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2019,
        icd_codes=["I00-I99"], age_groups=["25-34"],
    )
    assert "accept_datause_restrictions" in xml
    assert "true" in xml.lower()


def test_build_request_d76_uses_d76_field_names():
    xml = build_request_xml(
        database=DB_UCD_1999_2020, year=2016,
        icd_codes=["I00-I99"], age_groups=["25-34"],
    )
    assert "D76." in xml


def test_build_request_d158_uses_d158_field_names():
    xml = build_request_xml(
        database=DB_UCD_2018_2023, year=2019,
        icd_codes=["I00-I99"], age_groups=["25-34"],
    )
    assert "D158." in xml
