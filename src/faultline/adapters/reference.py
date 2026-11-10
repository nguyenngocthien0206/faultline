"""Adapter for faultline's reference stack (Postgres, dlt, S3, ClickHouse, dbt, Dagster).

Everything goes through public interfaces: the source database for input,
Dagster's GraphQL API for runs, ClickHouse's HTTP API for snapshots. An adapter
for a team's own Dagster or ClickHouse platform would look much the same.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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
from faultline.connectors.workspace import new_workspace_name, validate_workspace_name
from faultline.sources.pos import apply_day, create_tables

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


@dataclass(frozen=True)
class ReferenceStackConfig:
    source_dsn: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_url: str = "http://127.0.0.1:8123"
    dagster_url: str = "http://127.0.0.1:3000/graphql"
    location: str = "coffee_platform"
    job: str = "pos_daily"
    run_timeout_seconds: float = 900.0


class ReferenceAdapter(Adapter):
    def __init__(
        self,
        config: ReferenceStackConfig,
        generated_root: Path,
        *,
        workspace: str | None = None,
    ) -> None:
        self.config = config
        self.generated_root = Path(generated_root)
        self.workspace = validate_workspace_name(workspace or new_workspace_name())
        self.source = SourceDatabase(config.source_dsn)
        self.clickhouse = ClickHouseClient(
            config.clickhouse_url, user=config.clickhouse_user, password=config.clickhouse_password
        )
        self.dagster = DagsterClient(config.dagster_url, location=config.location)
        self._created = False

    def capabilities(self) -> Capabilities:
        features = frozenset(
            {Capability.DELIVER_DAY, Capability.RUN_PARTITION, Capability.SNAPSHOT}
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
        run_config = {"resources": {"workspace": {"config": {"name": self.workspace}}}}
        run_id = self.dagster.launch_run(
            self.config.job,
            partition=request.partition.isoformat(),
            run_config=run_config,
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

    def teardown(self) -> None:
        if not self._created:
            return
        self.source.drop_workspace(self.workspace)
        self.clickhouse.drop_workspace(self.workspace)
        self.source.close()
        self._created = False
