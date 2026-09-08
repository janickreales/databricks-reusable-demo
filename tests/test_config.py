"""Pruebas para la configuración central del pipeline de rentabilidad TPC-H."""

from src import config


def test_catalog_and_schema_are_non_empty_strings() -> None:
    assert isinstance(config.CATALOG, str) and config.CATALOG
    assert isinstance(config.SCHEMA, str) and config.SCHEMA


def test_part_name_filter_is_non_empty_string() -> None:
    assert isinstance(config.PART_NAME_FILTER, str) and config.PART_NAME_FILTER


def test_serverless_dbu_rate_is_positive_number() -> None:
    assert isinstance(config.SERVERLESS_DBU_RATE_USD, (int, float))
    assert config.SERVERLESS_DBU_RATE_USD > 0
