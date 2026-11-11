"""The POS pipeline, hardened edition: the same tools as naive, established patterns.

    extract  dlt with a lag window: every run reads again the last `lag_days`
             of changes, so rows that reach the source late but carry their
             real, older timestamps (a till syncing after an outage) are still
             extracted. The window is the platform's lateness promise: a row
             later than that is lost here too, just as in naive.
    bronze   the run's day is rebuilt in a staging table from all its landing
             files, then swapped in with REPLACE PARTITION: a rerun replaces
             the day instead of appending it a second time
    silver   one row per key (latest version): the copies made by the lag
             window, by reruns or by a replayed file collapse into one
    gold     aggregates final states (an order counts if it is completed now)
             and rewrites the touched business days with insert_overwrite

The lag window creates duplicates on purpose; idempotent bronze and keyed
silver are what make that safe. The three only work together.

Same ClickHouse table names as naive (a workspace runs one edition), different
Dagster asset keys (prefix `hardened`), so both live in one code location.

No `from __future__ import annotations` in Dagster code (see smoke.py).
"""

import json
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Config,
    MaterializeResult,
    asset,
    define_asset_job,
    in_process_executor,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from pydantic import Field

from coffee_platform.partitions import daily
from coffee_platform.pos import POS_TABLES, bronze_select, extract_to_landing, landing_files
from coffee_platform.resources import ClickHouse, Workspace

PREFIX = "hardened"
DEFAULT_LAG_DAYS = 3

HARDENED_DBT_PROJECT = DbtProject(
    project_dir=Path(__file__).resolve().parent.parent / "dbt_hardened"
)


# ------------------------------------------------------------------ extract


class ExtractConfig(Config):
    lag_days: int = Field(
        default=DEFAULT_LAG_DAYS,
        ge=0,
        description="How many days late a row may reach the source and still be counted.",
    )


@multi_asset(
    name="hardened_pos_landing",
    specs=[AssetSpec([PREFIX, f"landing_{t}"], group_name="hardened_landing") for t in POS_TABLES],
    partitions_def=daily,
)
def hardened_pos_landing(
    context: AssetExecutionContext, config: ExtractConfig, workspace: Workspace
):
    """dlt with a lag window of `lag_days`, as Parquet under dt=<partition>."""
    lag_seconds = timedelta(days=config.lag_days).total_seconds() or None
    context.log.info(f"lag window: {config.lag_days} days")
    counts = extract_to_landing(context, workspace, lag_seconds=lag_seconds)
    for table, rows in counts.items():
        yield MaterializeResult(
            asset_key=AssetKey([PREFIX, f"landing_{table}"]),
            metadata={"rows": rows, "lag_days": config.lag_days},
        )


# ------------------------------------------------------------------- bronze


def _bronze_asset(table: str):
    @asset(
        name=f"bronze_{table}",
        key_prefix=PREFIX,
        partitions_def=daily,
        deps=[AssetKey([PREFIX, f"landing_{table}"])],
        group_name="hardened_bronze",
    )
    def _bronze(
        context: AssetExecutionContext, workspace: Workspace, clickhouse: ClickHouse
    ) -> MaterializeResult:
        """Hardened: rebuild the run's day from its landing files and swap it in whole."""
        dt = context.partition_key
        files, data_fn = landing_files(clickhouse, workspace, table, dt)
        if files == 0:  # dlt writes nothing when a table had no changes
            context.log.info(f"no landing files for {table} on {dt}")
            return MaterializeResult(metadata={"files": 0, "rows": 0})

        target = f"{workspace.database}.bronze_{table}"
        staging = f"{target}__staging"
        select = bronze_select(data_fn, dt)
        client = clickhouse.client()
        client.command(f"CREATE DATABASE IF NOT EXISTS {workspace.database}")
        client.command(
            f"CREATE TABLE IF NOT EXISTS {target} "
            f"ENGINE = MergeTree PARTITION BY _dt ORDER BY tuple() EMPTY AS {select}"
        )
        # Build the day aside, then swap it in with one statement: a reader
        # sees the old day or the new one, never a half-loaded day.
        client.command(f"DROP TABLE IF EXISTS {staging}")
        client.command(f"CREATE TABLE {staging} AS {target}")
        client.command(f"INSERT INTO {staging} {select}")
        rows = int(client.command(f"SELECT count() FROM {staging}"))
        client.command(
            f"ALTER TABLE {target} REPLACE PARTITION tuple(toDate('{dt}')) FROM {staging}"
        )
        client.command(f"DROP TABLE {staging}")
        return MaterializeResult(metadata={"files": files, "rows": rows})

    return _bronze


HARDENED_BRONZE_ASSETS = [_bronze_asset(t) for t in POS_TABLES]


# ---------------------------------------------------------------- silver, gold


class HardenedTranslator(DagsterDbtTranslator):
    """Prefix the hardened models' asset keys. Sources declare theirs in sources.yml."""

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        key = super().get_asset_key(dbt_resource_props)
        if dbt_resource_props["resource_type"] == "source":
            return key
        return key.with_prefix(PREFIX)


@dbt_assets(
    manifest=HARDENED_DBT_PROJECT.manifest_path,
    partitions_def=daily,
    dagster_dbt_translator=HardenedTranslator(),
)
def hardened_pos_dbt(
    context: AssetExecutionContext, dbt_hardened: DbtCliResource, workspace: Workspace
):
    dbt_vars = {"workspace": workspace.name, "dt": context.partition_key}
    yield from dbt_hardened.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()


# In-process executor, for the same reason as naive's pos_daily.
pos_daily_hardened = define_asset_job(
    "pos_daily_hardened",
    selection=AssetSelection.assets(
        hardened_pos_landing, *HARDENED_BRONZE_ASSETS, hardened_pos_dbt
    ),
    partitions_def=daily,
    executor_def=in_process_executor,
)
