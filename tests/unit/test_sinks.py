"""Unit tests for SnowflakeSink batch dispatch (no Snowflake connection needed)."""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest import mock

import pytest
import sqlalchemy as sa
from singer_sdk.helpers._batch import JSONLinesEncoding

from target_snowflake.arrow_batch import ArrowEncoding
from target_snowflake.connector import SnowflakeConnector
from target_snowflake.sinks import SnowflakeSink


@pytest.fixture
def counter():
    """Stand-in for Sink.record_counter_metric, whose __enter__ returns self."""
    counter = mock.MagicMock()
    counter.__enter__.return_value = counter
    return counter


@pytest.fixture
def fake_sink(counter):
    """A duck-typed stand-in for SnowflakeSink, avoiding a real DB connection.

    `process_batch_files`/`_process_arrow_batch_files` are plain functions on the
    class, so they can be called with any object exposing the attributes they
    touch -- no need to construct a fully wired Sink/Target/Connector.
    """
    return SimpleNamespace(
        stream_name="test_stream",
        full_table_name="DB.SCHEMA.TABLE",
        config={},
        logger=mock.MagicMock(),
        record_counter_metric=counter,
        insert_batch_files_via_internal_stage=mock.MagicMock(return_value=42),
        _process_arrow_batch_files=mock.MagicMock(return_value=7),
    )


def test_process_batch_files_jsonl_dispatches_to_stage(fake_sink, counter):
    SnowflakeSink.process_batch_files(fake_sink, JSONLinesEncoding(), ["file:///tmp/a.jsonl"])

    fake_sink.insert_batch_files_via_internal_stage.assert_called_once_with(
        full_table_name="DB.SCHEMA.TABLE",
        files=["file:///tmp/a.jsonl"],
    )
    fake_sink._process_arrow_batch_files.assert_not_called()  # noqa: SLF001
    counter.increment.assert_called_once_with(42)


def test_process_batch_files_arrow_dispatches_to_arrow_handler(fake_sink, counter):
    SnowflakeSink.process_batch_files(fake_sink, ArrowEncoding(), ["file:///tmp/a.arrow"])

    fake_sink._process_arrow_batch_files.assert_called_once_with(["file:///tmp/a.arrow"])  # noqa: SLF001
    fake_sink.insert_batch_files_via_internal_stage.assert_not_called()
    counter.increment.assert_called_once_with(7)


def test_process_batch_files_unsupported_encoding_raises(fake_sink):
    class _UnsupportedEncoding:
        format = "unsupported"

    with pytest.raises(NotImplementedError, match="Unsupported batch file encoding"):
        SnowflakeSink.process_batch_files(fake_sink, _UnsupportedEncoding(), [])  # type: ignore[arg-type]


def test_process_arrow_batch_files_converts_and_loads_via_parquet_format(fake_sink):
    captured_output_dir = {}

    def fake_convert(manifest, output_dir, *, clean_up_source_files):
        captured_output_dir["path"] = output_dir
        assert manifest == ["file:///tmp/a.arrow"]
        assert clean_up_source_files is False
        assert os.path.isdir(output_dir)
        return ["file:///tmp/converted.parquet"]

    with mock.patch("target_snowflake.sinks.convert_arrow_manifest_to_parquet", side_effect=fake_convert):
        record_count = SnowflakeSink._process_arrow_batch_files(fake_sink, ["file:///tmp/a.arrow"])  # noqa: SLF001

    assert record_count == 42
    fake_sink.insert_batch_files_via_internal_stage.assert_called_once_with(
        full_table_name="DB.SCHEMA.TABLE",
        files=["file:///tmp/converted.parquet"],
        file_type="PARQUET",
    )
    # the temp output dir is always cleaned up afterward
    assert not os.path.exists(captured_output_dir["path"])


def test_process_arrow_batch_files_honors_clean_up_batch_files_config(fake_sink):
    fake_sink.config = {"clean_up_batch_files": True}

    with mock.patch(
        "target_snowflake.sinks.convert_arrow_manifest_to_parquet",
        return_value=[],
    ) as convert_mock:
        SnowflakeSink._process_arrow_batch_files(fake_sink, ["file:///tmp/a.arrow"])  # noqa: SLF001

    assert convert_mock.call_args.kwargs["clean_up_source_files"] is True


def test_get_file_format_name_creates_lazily_and_memoizes_per_type():
    connector = mock.MagicMock()
    sink = SimpleNamespace(
        database_name="DB",
        schema_name="SCHEMA",
        stream_name="test_stream",
        connector=connector,
        _file_formats={},
    )

    json_format = SnowflakeSink._get_file_format_name(sink, "JSON")  # noqa: SLF001
    json_format_again = SnowflakeSink._get_file_format_name(sink, "JSON")  # noqa: SLF001
    parquet_format = SnowflakeSink._get_file_format_name(sink, "PARQUET")  # noqa: SLF001

    # only created once per distinct file type, and reused thereafter
    assert json_format == json_format_again
    assert json_format != parquet_format
    assert connector.create_file_format.call_count == 2
    connector.create_file_format.assert_any_call(file_format=json_format, file_type="JSON")
    connector.create_file_format.assert_any_call(file_format=parquet_format, file_type="PARQUET")


def test_clean_up_drops_only_file_formats_actually_created():
    connector = mock.MagicMock()
    sink = SimpleNamespace(connector=connector, _file_formats={"JSON": 'DB.SCHEMA."tf-x"'})

    SnowflakeSink.clean_up(sink)

    connector.drop_file_format.assert_called_once_with(file_format='DB.SCHEMA."tf-x"')


def test_process_arrow_batch_files_cleans_up_output_dir_even_on_error(fake_sink):
    captured_output_dir = {}

    def fake_convert(manifest, output_dir, *, clean_up_source_files):  # noqa: ARG001
        captured_output_dir["path"] = output_dir
        msg = "boom"
        raise RuntimeError(msg)

    with (
        mock.patch("target_snowflake.sinks.convert_arrow_manifest_to_parquet", side_effect=fake_convert),
        pytest.raises(RuntimeError, match="boom"),
    ):
        SnowflakeSink._process_arrow_batch_files(fake_sink, ["file:///tmp/a.arrow"])  # noqa: SLF001

    assert not os.path.exists(captured_output_dir["path"])


def test_setup_invalidates_cached_inspector():
    """setup() must drop the connector's cached Inspector after creating the table.

    snowflake-sqlalchemy memoises columns per schema on the Inspector, so a table
    created here stays invisible to later reflection unless the Inspector is
    dropped. That surfaced as NoSuchTableError in activate_version for every
    stream after the first.
    """
    connector = SnowflakeConnector(
        config={
            "user": "test_user",
            "password": "test_password",
            "account": "test_account",
            "database": "TEST_DATABASE",
        },
    )
    connector._inspector = mock.Mock(spec=sa.Inspector)
    connector.table_cache["DB.SCHEMA.TABLE"] = {"id": mock.Mock()}
    connector.prepare_schema = mock.MagicMock()
    connector.prepare_table = mock.MagicMock()

    fake_sink = SimpleNamespace(
        schema_name="SCHEMA",
        full_table_name="DB.SCHEMA.TABLE",
        schema={},
        key_properties=[],
        config={},
        logger=mock.MagicMock(),
        connector=connector,
        conform_name=lambda name, object_type=None: name,  # noqa: ARG005
        conform_schema=lambda schema: schema,
    )

    SnowflakeSink.setup(fake_sink)

    assert connector._inspector is None
    assert "DB.SCHEMA.TABLE" not in connector.table_cache
