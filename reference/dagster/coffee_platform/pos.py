"""The POS pipeline, naive edition: source Postgres -> landing -> bronze -> dbt.

Every choice here is common in real pipelines and gives exact numbers on the
happy path. The flaws only show when something goes wrong around the pipeline:
bronze appends whatever files it finds, so a replayed run or a duplicated file
is loaded twice; gold nets change events and trusts each to arrive once.
The hardened edition (pos_hardened.py) reuses the extract and landing helpers.

No `from __future__ import annotations` in Dagster code (see smoke.py).
"""

import json
from pathlib import Path

import dlt
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    MaterializeResult,
    asset,
    define_asset_job,
    in_process_executor,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
from dlt.sources.sql_database import sql_database

from coffee_platform.partitions import daily
from coffee_platform.resources import ClickHouse, Workspace

LANDING_DATASET = "pos"

# table -> (incremental cursor column, primary key)
POS_TABLES: dict[str, tuple[str, str | list[str]]] = {
    "orders": ("updated_at", "order_id"),
    "order_items": ("created_at", ["order_id", "line_no"]),
    "payments": ("paid_at", "payment_id"),
}

DBT_PROJECT = DbtProject(project_dir=Path(__file__).resolve().parent.parent / "dbt")


# ------------------------------------------------------------------ extract


def extract_to_landing(
    context: AssetExecutionContext, workspace: Workspace, *, lag_seconds: float | None = None
) -> dict[str, int]:
    """dlt: rows past each table's cursor, as Parquet under dt=<partition>.

    `lag_seconds` steps the start of the window back from the stored cursor, so
    rows that reach the source late but with older timestamps are still picked
    up (the cursor itself only moves forward). Shared by both editions; naive
    passes no lag.
    """
    dt = context.partition_key
    source = sql_database(schema=workspace.source_schema, table_names=list(POS_TABLES))
    for table, (cursor, key) in POS_TABLES.items():
        source.resources[table].apply_hints(
            incremental=dlt.sources.incremental(cursor, lag=lag_seconds), primary_key=key
        )

    pipeline = dlt.pipeline(
        pipeline_name=f"pos_{workspace.name}",
        pipelines_dir=f"/tmp/dlt/{workspace.name}",
        destination=dlt.destinations.filesystem(
            bucket_url=workspace.landing_url,
            layout="{table_name}/dt={dt}/{load_id}.{file_id}.{ext}",
            extra_placeholders={"dt": dt},
        ),
        dataset_name=LANDING_DATASET,
    )
    pipeline.run(source, loader_file_format="parquet")
    counts = pipeline.last_trace.last_normalize_info.row_counts
    context.log.info(f"extracted {counts}")
    return {table: int(counts.get(table, 0)) for table in POS_TABLES}


@multi_asset(
    specs=[AssetSpec(f"landing_{t}", group_name="landing") for t in POS_TABLES],
    partitions_def=daily,
)
def pos_landing(context: AssetExecutionContext, workspace: Workspace):
    """Naive: dlt's default incremental cursor, no lag window."""
    counts = extract_to_landing(context, workspace)
    for table, rows in counts.items():
        yield MaterializeResult(asset_key=f"landing_{table}", metadata={"rows": rows})


# ------------------------------------------------------------------- bronze


def landing_files(
    clickhouse: ClickHouse, workspace: Workspace, table: str, dt: str
) -> tuple[int, str]:
    """How many landing files a table has under dt=<dt>, and the s3() call reading them."""
    pattern = f"{workspace.name}/{LANDING_DATASET}/{table}/dt={dt}/*.parquet"
    files_fn = f"s3(landing_s3, filename = '{pattern}', format = 'One')"
    data_fn = f"s3(landing_s3, filename = '{pattern}', format = 'Parquet')"
    files = int(
        clickhouse.client().command(
            f"SELECT count() FROM {files_fn} SETTINGS s3_throw_on_zero_files_match = 0"
        )
    )
    return files, data_fn


def bronze_select(data_fn: str, dt: str) -> str:
    """Every landed column, plus where each row came from and which partition loaded it."""
    return f"SELECT *, _path AS _source_file, toDate('{dt}') AS _dt FROM {data_fn}"


def _bronze_asset(table: str):
    @asset(
        name=f"bronze_{table}",
        partitions_def=daily,
        deps=[AssetKey(f"landing_{table}")],
        group_name="bronze",
    )
    def _bronze(
        context: AssetExecutionContext, workspace: Workspace, clickhouse: ClickHouse
    ) -> MaterializeResult:
        """Naive: append every file under dt=<partition>, whether seen before or not."""
        dt = context.partition_key
        files, data_fn = landing_files(clickhouse, workspace, table, dt)
        if files == 0:  # dlt writes nothing when a table had no changes
            context.log.info(f"no landing files for {table} on {dt}")
            return MaterializeResult(metadata={"files": 0, "rows": 0})

        target = f"{workspace.database}.bronze_{table}"
        client = clickhouse.client()
        select = bronze_select(data_fn, dt)
        client.command(f"CREATE DATABASE IF NOT EXISTS {workspace.database}")
        # Schema-on-first-file: whatever the first Parquet file looked like.
        client.command(
            f"CREATE TABLE IF NOT EXISTS {target} ENGINE = MergeTree ORDER BY tuple() "
            f"EMPTY AS {select}"
        )
        rows = int(client.command(f"SELECT count() FROM {data_fn}"))
        client.command(f"INSERT INTO {target} {select}")
        return MaterializeResult(metadata={"files": files, "rows": rows})

    return _bronze


BRONZE_ASSETS = [_bronze_asset(t) for t in POS_TABLES]


# ---------------------------------------------------------------- silver, gold


@dbt_assets(manifest=DBT_PROJECT.manifest_path, partitions_def=daily)
def pos_dbt(context: AssetExecutionContext, dbt: DbtCliResource, workspace: Workspace):
    dbt_vars = {"workspace": workspace.name, "dt": context.partition_key}
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()


# In-process executor: the steps run one after another anyway, and the default
# multiprocess executor starts a fresh Python per step that re-imports dlt, dbt
# and Dagster (about 15 s each on Docker Desktop). One process, one import.
pos_daily = define_asset_job(
    "pos_daily",
    selection=AssetSelection.assets(pos_landing, *BRONZE_ASSETS, pos_dbt),
    partitions_def=daily,
    executor_def=in_process_executor,
)
