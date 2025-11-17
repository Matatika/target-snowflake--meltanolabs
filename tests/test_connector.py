"""Unit tests for the Snowflake connector."""

from __future__ import annotations

from unittest import mock

import pytest
import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy as sa

from target_snowflake.connector import SnowflakeConnector, SnowflakeTimestampType
from target_snowflake.snowflake_types import NUMBER, VARIANT


@pytest.fixture
def connector():
    config = {
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "database": "TEST_DATABASE",
        "timestamp_type": SnowflakeTimestampType.TIMESTAMP_NTZ,
    }

    return SnowflakeConnector(config=config)


@pytest.fixture
def mock_engine_connect(connector: SnowflakeConnector):
    with mock.patch("sqlalchemy.engine.Engine.connect") as connect_mock:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchall.return_value = [(None, connector.config["database"])]

        context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = connection

        connect_mock.return_value = context_manager
        yield connect_mock


@pytest.mark.parametrize(
    ("schema", "expected_type"),
    [
        pytest.param({"type": "object"}, VARIANT, id="object"),
        pytest.param({"type": ["array", "null"]}, VARIANT, id="array"),
        pytest.param({"type": ["array", "object", "string"]}, VARIANT, id="array_object_string"),
        pytest.param({"type": ["integer", "null"]}, NUMBER, id="integer"),
        pytest.param({"type": ["number", "null"]}, sct.DOUBLE, id="number"),
        pytest.param({"type": ["string", "null"], "format": "date-time"}, sct.TIMESTAMP_NTZ, id="date-time"),
        # Upstream types
        pytest.param({"type": ["string", "null"]}, sa.types.VARCHAR, id="string"),
        pytest.param({"type": ["boolean", "null"]}, sa.types.BOOLEAN, id="boolean"),
        pytest.param({"type": "string", "format": "time"}, sa.types.TIME, id="time"),
        pytest.param({"type": "string", "format": "date"}, sa.types.DATE, id="date"),
        pytest.param({"type": "string", "format": "uuid"}, sa.types.VARCHAR, id="uuid"),
    ],
)
def test_jsonschema_to_sql(connector: SnowflakeConnector, schema: dict, expected_type: type[sa.types.TypeEngine]):
    sql_type = connector.to_sql_type(schema)
    assert isinstance(sql_type, expected_type)

@pytest.mark.parametrize(
    ("config", "expected_type"),
    [
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_TZ}, sct.TIMESTAMP_TZ),
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_LTZ}, sct.TIMESTAMP_LTZ),
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_NTZ}, sct.TIMESTAMP_NTZ),
    ],
)
def test_datetime_to_sql(connector: SnowflakeConnector, config: dict, expected_type: type[sa.types.TypeEngine]):
    connector.config.update(config)
    schema = {"type": ["string", "null"], "format": "date-time"}
    sql_type = connector.to_sql_type(schema)
    assert isinstance(sql_type, expected_type)


def test_to_sql_type_with_max_varchar_length(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "maxLength": 1_000_000})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 1_000_000

    sql_type = connector.to_sql_type({"type": "string", "maxLength": SnowflakeConnector.max_varchar_length + 1})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == SnowflakeConnector.max_varchar_length


def test_email_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "email"})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 254


def test_uri_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "uri"})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 2083


def test_hostname_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "hostname"})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 253


def test_ipv4_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "ipv4"})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 15


def test_ipv6_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "ipv6"})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 45


def test_uuid_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "uuid"})
    assert isinstance(sql_type, sa.types.VARCHAR)
    assert sql_type.length == 36


def test_singer_decimal(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type(
        {
            "type": "string",
            "format": "x-singer.decimal",
            "precision": 38,
            "scale": 18,
        },
    )
    assert isinstance(sql_type, sa.types.DECIMAL)
    assert sql_type.precision == 38
    assert sql_type.scale == 18


@pytest.mark.parametrize(
    ("config", "identifier", "expected_formatted"),
    [
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "email", "email"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "email_address", "email_address"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "emailAddress", "emailAddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "EmailAddress", "EmailAddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "EMAIL_ADDRESS", "EMAIL_ADDRESS"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "EMAILADDRESS", "EMAILADDRESS"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "user", "user"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "email", "email"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "email_address", "email_address"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "emailAddress", "emailaddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "EmailAddress", "emailaddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "EMAIL_ADDRESS", "email_address"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "EMAILADDRESS", "emailaddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "user", "USER"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "email", "email"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "email_address", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "emailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "EmailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "EMAIL_ADDRESS", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "EMAILADDRESS", "emailaddress"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "user", "user"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "email", "email"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "email_address", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "emailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "EmailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "EMAIL_ADDRESS", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "EMAILADDRESS", "emailaddress"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "user", "USER"),
    ],
)
@pytest.mark.usefixtures("mock_engine_connect")
def test_format_identifier(connector: SnowflakeConnector, config: dict, identifier: str, expected_formatted: str):
    connector.config.update(config)
    formatted = connector.format_identifier(identifier)
    assert formatted == expected_formatted
