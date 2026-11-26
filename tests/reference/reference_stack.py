"""Connection settings and clients for the reference stack, shared by its tests.

Credentials come from the same file docker compose reads, reference/.env
(falling back to .env.example when the stack was never started), so changing
a password happens in exactly one place. Environment variables win over both.
"""

from dataclasses import dataclass
from pathlib import Path

from faultline.adapters.reference import ReferenceStackConfig, config_from_env_file
from faultline.connectors.clickhouse import ClickHouseClient
from faultline.connectors.dagster import DagsterClient
from faultline.connectors.postgres import SourceDatabase
from faultline.runner import Experiment
from faultline.safety import Target
from faultline.spec import load_experiment, load_target

ROOT = Path(__file__).resolve().parents[2]
REFERENCE_DIR = ROOT / "reference"


def env_file() -> Path:
    path = REFERENCE_DIR / ".env"
    return path if path.exists() else REFERENCE_DIR / ".env.example"


def reference_config() -> ReferenceStackConfig:
    """Adapter configuration for the local stack."""
    return config_from_env_file(env_file())


def reference_target(edition: str) -> Target:
    """targets/reference-<edition>.yaml, on the stack the tests use."""
    spec = load_target(ROOT / "targets" / f"reference-{edition}.yaml")
    return spec.build(config=reference_config())


def experiment(name: str, **dataset: object) -> Experiment:
    """experiments/<name>.yaml, with its dataset section overridden by `dataset`."""
    return load_experiment(ROOT / "experiments" / f"{name}.yaml", dataset=dataset)


@dataclass
class Stack:
    source: SourceDatabase
    clickhouse: ClickHouseClient
    dagster: DagsterClient

    @classmethod
    def from_env(cls) -> "Stack":
        config = reference_config()
        return cls(
            source=SourceDatabase(config.source_dsn),
            clickhouse=ClickHouseClient(
                config.clickhouse_url,
                user=config.clickhouse_user,
                password=config.clickhouse_password,
            ),
            dagster=DagsterClient(config.dagster_url, location=config.location),
        )
