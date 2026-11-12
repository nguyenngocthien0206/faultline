"""Adapter for faultline's reference stack (Postgres, dlt, S3, ClickHouse, dbt, Dagster).

Everything goes through public interfaces: the source database for input,
Dagster's GraphQL API for runs, ClickHouse's HTTP API for snapshots. An adapter
for a team's own Dagster or ClickHouse platform would look much the same.

The stack hosts two editions of the same pipeline, each a Dagster job: naive
(common shortcuts) and hardened (established patterns). They write the same
tables, so one contract and one set of experiments serve both.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path

from faultline.adapters.base import (
    Adapter,
    AdapterError,
    Capabilities,
    Capability,
    RunRequest,
    RunResult,
    RunStatus,
    TableContract,
)
from faultline.connectors.clickhouse import ClickHouseClient
from faultline.connectors.dagster import DagsterClient
from faultline.connectors.postgres import SourceDatabase
from faultline.connectors.s3 import LandingBucket
from faultline.connectors.workspace import new_workspace_name, validate_workspace_name
from faultline.sources.pos import apply_day, create_tables


class Edition(StrEnum):
    NAIVE = "naive"
    HARDENED = "hardened"


JOBS: dict[Edition, str] = {Edition.NAIVE: "pos_daily", Edition.HARDENED: "pos_daily_hardened"}
HARDENED_EXTRACT_OP = "hardened_pos_landing"  # takes the lag window as op config


def target_name(edition: Edition | str, lag_days: int | None = None) -> str:
    """How a result names the platform it ran on, e.g. reference-hardened(lag=1d).

    faultline does not know the platform's default lag, so the name shows the
    lag only when faultline set one.
    """
    name = f"reference-{Edition(edition)}"
    return name if lag_days is None else f"{name}(lag={lag_days}d)"


CONSUMER_TABLES: tuple[TableContract, ...] = (
    TableContract(
        "silver_orders",
        key=("order_id",),
        measures=("subtotal_vnd", "discount_vnd", "total_vnd"),
        truth_dataset="pos/orders",
    ),
    TableContract(
        "silver_order_items",
        key=("order_id", "line_no"),
        measures=("qty", "unit_price_vnd"),
        truth_dataset="pos/order_items",
    ),
    TableContract(
        "silver_payments",
        key=("payment_id",),
        measures=("amount_vnd",),
        truth_dataset="pos/payments",
    ),
    TableContract(
        "gold_daily_store_sales",
        key=("order_date", "store_id"),
        measures=("orders", "items_sold", "gross_vnd", "discount_vnd", "net_vnd"),
    ),
    TableContract(
        "gold_daily_payments",
        key=("paid_date", "store_id", "method"),
        measures=("charges", "charged_vnd", "refunded_vnd", "net_vnd"),
    ),
)


# Generated dataset -> where the platform lands its files, under the workspace
# prefix of the landing bucket (dlt: <dataset>/<table>/dt=<partition>/).
LANDING_DATASETS: dict[str, str] = {
    "pos/orders": "pos/orders",
    "pos/order_items": "pos/order_items",
    "pos/payments": "pos/payments",
}


@dataclass(frozen=True)
class ReferenceStackConfig:
    source_dsn: str
    clickhouse_user: str
    clickhouse_password: str
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_url: str = "http://127.0.0.1:9000"
    landing_bucket: str = "landing"
    clickhouse_url: str = "http://127.0.0.1:8123"
    dagster_url: str = "http://127.0.0.1:3000/graphql"
    location: str = "coffee_platform"
    run_timeout_seconds: float = 900.0


def parse_env_file(path: Path) -> dict[str, str]:
    """Minimal KEY=VALUE reader: comments, blank lines and surrounding quotes."""
    values: dict[str, str] = {}
    for raw in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def config_from_env_file(
    path: Path, *, environ: Mapping[str, str] | None = None
) -> ReferenceStackConfig:
    """Connection settings for a local reference stack, from the env file compose reads.

    Process environment variables with the same names win over the file.
    """
    settings = parse_env_file(path)
    env = os.environ if environ is None else environ
    settings.update({k: v for k, v in env.items() if k in settings})
    return ReferenceStackConfig(
        source_dsn=(
            f"postgresql://{settings['SOURCE_DB_USER']}:{settings['SOURCE_DB_PASSWORD']}"
            f"@127.0.0.1:5433/{settings['SOURCE_DB_NAME']}"
        ),
        clickhouse_user=settings["CLICKHOUSE_USER"],
        clickhouse_password=settings["CLICKHOUSE_PASSWORD"],
        s3_access_key=settings["S3_ACCESS_KEY"],
        s3_secret_key=settings["S3_SECRET_KEY"],
    )


class ReferenceAdapter(Adapter):
    def __init__(
        self,
        config: ReferenceStackConfig,
        generated_root: Path,
        *,
        edition: Edition | str = Edition.NAIVE,
        lag_days: int | None = None,
        workspace: str | None = None,
    ) -> None:
        self.edition = Edition(edition)
        if lag_days is not None and self.edition is not Edition.HARDENED:
            raise ValueError("only the hardened edition has a lag window")
        if lag_days is not None and lag_days < 0:
            raise ValueError(f"lag_days must be 0 or more, got {lag_days}")
        self.lag_days = lag_days
        self.config = config
        self.generated_root = Path(generated_root)
        self.workspace = validate_workspace_name(workspace or new_workspace_name())
        self.source = SourceDatabase(config.source_dsn)
        self.clickhouse = ClickHouseClient(
            config.clickhouse_url, user=config.clickhouse_user, password=config.clickhouse_password
        )
        self.dagster = DagsterClient(config.dagster_url, location=config.location)
        self.landing = LandingBucket(
            config.s3_url,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            bucket=config.landing_bucket,
        )
        self._created = False

    @property
    def job(self) -> str:
        return JOBS[self.edition]

    @property
    def target(self) -> str:
        return target_name(self.edition, self.lag_days)

    def run_config(self) -> dict[str, object]:
        """Dagster run config: the sandbox to work in and, if set, the lag window."""
        config: dict[str, object] = {
            "resources": {"workspace": {"config": {"name": self.workspace}}}
        }
        if self.lag_days is not None:
            config["ops"] = {HARDENED_EXTRACT_OP: {"config": {"lag_days": self.lag_days}}}
        return config

    def capabilities(self) -> Capabilities:
        # Concurrent sandboxes: each workspace has its own source schema,
        # ClickHouse database, S3 prefix and dlt state, and Dagster starts runs
        # without a queue.
        features = frozenset(
            {
                Capability.DELIVER_DAY,
                Capability.RUN_PARTITION,
                Capability.SNAPSHOT,
                Capability.CONCURRENT_SANDBOXES,
                Capability.LANDING_FILES,
            }
        )
        return Capabilities(features=features, tables=CONSUMER_TABLES)

    def setup(self) -> None:
        load_error = self.dagster.location_error()
        if load_error:
            raise AdapterError(f"Dagster code location failed to load: {load_error}")
        self.source.create_workspace(self.workspace)
        self._created = True
        create_tables(self.source, self.workspace)

    def deliver(self, day: date) -> None:
        apply_day(self.source, self.workspace, self.generated_root, day)

    def run(self, request: RunRequest) -> RunResult:
        run_id = self.dagster.launch_run(
            self.job,
            partition=request.partition.isoformat(),
            run_config=self.run_config(),
            tags={"faultline/workspace": self.workspace},
        )
        outcome = self.dagster.wait_for_run(run_id, timeout=self.config.run_timeout_seconds)
        status = RunStatus.SUCCEEDED if outcome.succeeded else RunStatus.FAILED
        return RunResult(status=status, errors=outcome.errors, run_id=run_id)

    def snapshot(self, table: str, dest: Path) -> Path:
        contract = self.capabilities().table(table)  # only declared tables can be read
        return self.clickhouse.export_parquet(
            f"SELECT * FROM {self.workspace}.{contract.name}", dest
        )

    def landing_files(self, dataset: str, partition: date) -> list[str]:
        prefix = self.landing_key(dataset, partition)
        return [key.removeprefix(prefix) for key in self.landing.list(prefix)]

    def read_landing_file(self, dataset: str, partition: date, file: str) -> bytes:
        return self.landing.get(self.landing_key(dataset, partition, file))

    def write_landing_file(self, dataset: str, partition: date, file: str, data: bytes) -> None:
        self.landing.put(self.landing_key(dataset, partition, file), data)

    def landing_key(self, dataset: str, partition: date, file: str = "") -> str:
        """The bucket key of one landing file (with no file: of the partition's folder).

        Always inside this workspace and a declared dataset: a file name with a
        separator or a dot path is refused, so no fault can reach anywhere else.
        """
        if dataset not in LANDING_DATASETS:
            raise AdapterError(
                f"no landing files for {dataset!r}; known: {sorted(LANDING_DATASETS)}"
            )
        if file in (".", "..") or "/" in file or "\\" in file:
            raise AdapterError(f"refusing landing file name {file!r}")
        folder = f"{self.workspace}/{LANDING_DATASETS[dataset]}/dt={partition.isoformat()}/"
        return folder + file

    def teardown(self) -> None:
        if not self._created:
            return
        self.source.drop_workspace(self.workspace)
        self.clickhouse.drop_workspace(self.workspace)
        self.landing.delete_workspace(self.workspace)
        self.source.close()
        self._created = False
