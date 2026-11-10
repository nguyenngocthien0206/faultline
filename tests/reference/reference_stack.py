"""Connection settings and clients for the reference stack, shared by its tests."""

import os
from dataclasses import dataclass

from faultline.connectors.clickhouse import ClickHouseClient
from faultline.connectors.dagster import DagsterClient
from faultline.connectors.postgres import SourceDatabase

# Defaults match reference/docker-compose.yml (test-only credentials).
SOURCE_DSN = os.environ.get(
    "FAULTLINE_SOURCE_DSN", "postgresql://faultline:faultline@127.0.0.1:5433/coffee"
)
CLICKHOUSE_URL = os.environ.get("FAULTLINE_CLICKHOUSE_URL", "http://127.0.0.1:8123")
DAGSTER_URL = os.environ.get("FAULTLINE_DAGSTER_URL", "http://127.0.0.1:3000/graphql")


@dataclass
class Stack:
    source: SourceDatabase
    clickhouse: ClickHouseClient
    dagster: DagsterClient

    @classmethod
    def from_env(cls) -> "Stack":
        return cls(
            source=SourceDatabase(SOURCE_DSN),
            clickhouse=ClickHouseClient(CLICKHOUSE_URL, user="faultline", password="faultline"),
            dagster=DagsterClient(DAGSTER_URL, location="coffee_platform", timeout=10),
        )
