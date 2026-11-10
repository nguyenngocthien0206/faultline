"""Resources: which isolated workspace a run works in, and how to reach the warehouse."""

import os
import re

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from dagster import ConfigurableResource, EnvVar
from pydantic import field_validator

WORKSPACE_PATTERN = re.compile(r"^fl_[0-9a-f]{8}$")
LANDING_BUCKET = "landing"


class Workspace(ConfigurableResource):
    """One experiment's sandbox, chosen per run through run config.

    The same name is the Postgres schema of the source tables, the ClickHouse
    database and the S3 prefix in the landing bucket. The platform validates it
    too, so it can never be pointed at a shared schema or database by mistake.
    """

    name: str

    @field_validator("name")
    @classmethod
    def _must_be_a_workspace(cls, value: str) -> str:
        if not WORKSPACE_PATTERN.fullmatch(value):
            raise ValueError(f"{value!r} is not a workspace name (fl_ + 8 hex chars)")
        return value

    @property
    def source_schema(self) -> str:
        return self.name

    @property
    def database(self) -> str:
        return self.name

    @property
    def landing_url(self) -> str:
        return f"s3://{LANDING_BUCKET}/{self.name}"


class ClickHouse(ConfigurableResource):
    host: str
    port: int = 8123
    user: str
    password: str

    def client(self) -> Client:
        return clickhouse_connect.get_client(
            host=self.host, port=self.port, username=self.user, password=self.password
        )


def clickhouse_from_env() -> ClickHouse:
    return ClickHouse(
        host=EnvVar("CLICKHOUSE_HOST"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        user=EnvVar("CLICKHOUSE_USER"),
        password=EnvVar("CLICKHOUSE_PASSWORD"),
    )
