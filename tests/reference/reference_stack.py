"""Connection settings and clients for the reference stack, shared by its tests.

Credentials come from the same file docker compose reads, reference/.env
(falling back to .env.example when the stack was never started), so changing
a password happens in exactly one place. Environment variables win over both.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from faultline.adapters.reference import ReferenceStackConfig
from faultline.connectors.clickhouse import ClickHouseClient
from faultline.connectors.dagster import DagsterClient
from faultline.connectors.postgres import SourceDatabase

REFERENCE_DIR = Path(__file__).resolve().parents[2] / "reference"


def parse_env_file(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE reader: comments, blank lines and surrounding quotes."""
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def stack_settings() -> dict[str, str]:
    env_file = REFERENCE_DIR / ".env"
    if not env_file.exists():
        env_file = REFERENCE_DIR / ".env.example"
    settings = parse_env_file(env_file)
    settings.update({k: v for k, v in os.environ.items() if k in settings})
    return settings


def reference_config() -> ReferenceStackConfig:
    """Adapter configuration for the local stack, from the same settings as Stack."""
    s = stack_settings()
    return ReferenceStackConfig(
        source_dsn=(
            f"postgresql://{s['SOURCE_DB_USER']}:{s['SOURCE_DB_PASSWORD']}"
            f"@127.0.0.1:5433/{s['SOURCE_DB_NAME']}"
        ),
        clickhouse_user=s["CLICKHOUSE_USER"],
        clickhouse_password=s["CLICKHOUSE_PASSWORD"],
    )


@dataclass
class Stack:
    source: SourceDatabase
    clickhouse: ClickHouseClient
    dagster: DagsterClient

    @classmethod
    def from_env(cls) -> "Stack":
        s = stack_settings()
        source_dsn = (
            f"postgresql://{s['SOURCE_DB_USER']}:{s['SOURCE_DB_PASSWORD']}"
            f"@127.0.0.1:5433/{s['SOURCE_DB_NAME']}"
        )
        return cls(
            source=SourceDatabase(source_dsn),
            clickhouse=ClickHouseClient(
                "http://127.0.0.1:8123",
                user=s["CLICKHOUSE_USER"],
                password=s["CLICKHOUSE_PASSWORD"],
            ),
            dagster=DagsterClient(
                "http://127.0.0.1:3000/graphql", location="coffee_platform", timeout=10
            ),
        )
