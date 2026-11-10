"""Smoke job: one asset that touches every layer of the stack.

Postgres source -> dlt (incremental, Parquet) -> S3 landing -> ClickHouse.
If this runs green, the plumbing works and real pipelines can be built on it.

No `from __future__ import annotations` in Dagster code: Dagster inspects the
`context` annotation at load time and rejects it when it is only a string.
"""

import dlt
from dagster import AssetExecutionContext, MaterializeResult, asset, define_asset_job
from dlt.sources.sql_database import sql_table

from coffee_platform.partitions import daily
from coffee_platform.resources import ClickHouse, Workspace

SMOKE_DATASET = "smoke"


@asset(partitions_def=daily)
def smoke_ping(
    context: AssetExecutionContext, workspace: Workspace, clickhouse: ClickHouse
) -> MaterializeResult:
    dt = context.partition_key

    # 1. Extract the `ping` table incrementally from the source database.
    source = sql_table(
        schema=workspace.source_schema,
        table="ping",
        incremental=dlt.sources.incremental("updated_at"),
    )
    pipeline = dlt.pipeline(
        pipeline_name=f"smoke_{workspace.name}",
        pipelines_dir=f"/tmp/dlt/{workspace.name}",
        destination=dlt.destinations.filesystem(
            bucket_url=workspace.landing_url,
            layout="{table_name}/dt={dt}/{load_id}.{file_id}.{ext}",
            extra_placeholders={"dt": dt},
        ),
        dataset_name=SMOKE_DATASET,
    )
    info = pipeline.run(source, loader_file_format="parquet")
    context.log.info(str(info))

    # 2. Read the landed Parquet back through ClickHouse's S3 table function.
    pattern = f"{workspace.name}/{SMOKE_DATASET}/ping/dt={dt}/*.parquet"
    client = clickhouse.client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {workspace.database}")
    client.command(f"DROP TABLE IF EXISTS {workspace.database}.smoke_ping")
    client.command(
        f"CREATE TABLE {workspace.database}.smoke_ping ENGINE = MergeTree ORDER BY tuple() "
        f"AS SELECT id, updated_at FROM s3(landing_s3, filename = '{pattern}', format = 'Parquet')"
    )
    rows = client.command(f"SELECT count() FROM {workspace.database}.smoke_ping")
    return MaterializeResult(metadata={"rows": int(rows), "landing": pattern})


smoke_job = define_asset_job("smoke_job", selection=[smoke_ping], partitions_def=daily)
