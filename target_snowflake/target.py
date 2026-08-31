"""Snowflake target class."""

from __future__ import annotations

import logging.config
import typing as t

import click
from singer_sdk import typing as th
from singer_sdk.helpers._typing import DatetimeErrorTreatmentEnum
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.target_base import SQLTarget

from target_snowflake.connector import SnowflakeTimestampType
from target_snowflake.initializer import initializer
from target_snowflake.sinks import SnowflakeSink

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {"snowflake.connector": {"level": "WARNING"}},
    },
)


class TargetSnowflake(SQLTarget):
    """Target for Snowflake."""

    name = "target-snowflake"
    package_name = "meltanolabs_target_snowflake"

    # From https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The login name for your Snowflake user.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="The password for your Snowflake user.",
        ),
        th.Property(
            "private_key",
            th.StringType,
            required=False,
            secret=True,
            description=(
                "The private key contents, in PEM or base64-encoding format. "
                "For KeyPair authentication either `private_key` or `private_key_path` "
                "must be provided."
            ),
        ),
        th.Property(
            "private_key_path",
            th.StringType,
            required=False,
            description=(
                "Path to file containing private key. For KeyPair authentication either "
                "private_key or private_key_path must be provided."
            ),
        ),
        th.Property(
            "private_key_passphrase",
            th.StringType,
            required=False,
            description="Passphrase to decrypt private key if encrypted.",
        ),
        th.Property(
            "account",
            th.StringType,
            required=True,
            description="Your account identifier. See [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).",
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="The initial database for the Snowflake session.",
        ),
        th.Property(
            "schema",
            th.StringType,
            description="The initial schema for the Snowflake session.",
        ),
        th.Property(
            "warehouse",
            th.StringType,
            description="The initial warehouse for the session.",
        ),
        th.Property(
            "role",
            th.StringType,
            description="The initial role for the session.",
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=True,
            description="Whether to add metadata columns.",
        ),
        th.Property(
            "clean_up_batch_files",
            th.BooleanType,
            default=True,
            description="Whether to remove batch files after processing.",
        ),
        th.Property(
            "use_browser_authentication",
            th.BooleanType,
            default=False,
            description="Whether to use SSO authentication using an external browser.",
        ),
        th.Property(
            "timestamp_type",
            th.StringType,
            allowed_values=[t.name for t in SnowflakeTimestampType],
            default=SnowflakeTimestampType.TIMESTAMP_NTZ,
            description="Snowflake timestamp type to use for date-time properties.",
        ),
        th.Property(
            "uuid_format",
            th.StringType,
            allowed_values=["native", "string"],
            default="string",
            description=(
                "Snowflake column type/value format for `format: uuid` string properties. "
                "'string' (default) uses a STRING(36) column and writes the value as-is, preserving dashes. "
                "'native' uses SQLAlchemy's native UUID type, which compiles to CHAR(32) on Snowflake and strips "
                "dashes from the value."
            ),
        ),
        th.Property(
            "quoted_identifiers_ignore_case",
            th.BooleanType,
            default=False,
            description=(
                "Whether letters in double-quoted object identifiers are stored and resolved as uppercase letters."
            ),
        ),
        th.Property(
            "normalise_casing",
            th.BooleanType,
            default=True,
            description="Whether to normalise identifiers into snake_case.",
        ),
        th.Property(
            "use_raw_stream_names",
            th.BooleanType,
            default=True,
            description=(
                "Whether to use raw stream names as table names instead of informal Singer convention as last "
                "hyphen-separated part of stream name."
            ),
        ),
        th.Property(
            "datetime_error_treatment",
            th.StringType,
            allowed_values=[t.value for t in DatetimeErrorTreatmentEnum],
            default=DatetimeErrorTreatmentEnum.ERROR.value,
            description="How invalid date-time values should be handled.",
        ),
        th.Property(
            "load_method",
            th.StringType,
            allowed_values=["upsert", "overwrite"],
            default="upsert",
            description=(
                "Controls how records are written to the destination table. "
                "'upsert' (default) uses MERGE INTO, matching on key properties to update "
                "existing rows and insert new ones. "
                "'overwrite' truncates the table then uses COPY INTO, replacing all existing "
                "rows — faster for initial loads but destructive if run on a populated table."
            ),
        ),
    ).to_dict()

    default_sink_class = SnowflakeSink

    #: A list of capabilities supported by this target.
    capabilities: t.ClassVar[list[CapabilitiesEnum]] = [
        *SQLTarget.capabilities,
        PluginCapabilities.BATCH,
    ]

    @classmethod
    def cb_inititalize(
        cls: type[TargetSnowflake],
        ctx: click.Context,
        param: click.Option,  # noqa: ARG003
        value: bool,  # noqa: FBT001
    ) -> None:
        if value:
            initializer()
            ctx.exit()

    @classmethod
    def get_singer_command(cls: type[TargetSnowflake]) -> click.Command:
        """Execute standard CLI handler for targets.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
        command.params.extend(
            [
                click.Option(
                    ["--initialize"],
                    is_flag=True,
                    help="Interactive Snowflake account initialization.",
                    callback=cls.cb_inititalize,
                    expose_value=False,
                ),
            ],
        )

        return command


if __name__ == "__main__":
    TargetSnowflake.cli()
