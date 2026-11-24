"""The POS pipeline, hardened edition: the same tools as naive, established patterns.

    extract  dlt with a lag window: every run reads again the last `lag_days`
             of changes, so rows that reach the source late but carry their
             real, older timestamps (a till syncing after an outage) are still
             extracted. The window is the platform's lateness promise: a row
             later than that is lost here too, just as in naive.
    bronze   the run's day is rebuilt in a staging table from all its landing
             files, then swapped in with REPLACE PARTITION: a rerun replaces
             the day instead of appending it a second time. Before that, the
             files are reconciled against the extract's manifest (below)
    silver   one row per key (latest version): the copies made by the lag
             window, by reruns or by a replayed file collapse into one
    gold     aggregates final states (an order counts if it is completed now)
             and rewrites the touched business days with insert_overwrite

The lag window creates duplicates on purpose; idempotent bronze and keyed
silver are what make that safe. The three only work together.

Manifest, a delivery note between ingest and load. Each extract writes, next
to its files, one line per table: its load id, the rows dlt extracted, and
the files that load wrote (pos/_manifests/dt=<partition>/<load_id>.jsonl).
Bronze refuses to load a day whose files do not match their notes: a file no
manifest lists (stray), a listed file that is not there (missing), or a load
whose files hold another number of rows than it promised (a cut file still
parses, so only the count gives it away). The run fails, loudly, and the day
is left as it was until the files are right.

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
    Failure,
    MaterializeResult,
    asset,
    define_asset_job,
    in_process_executor,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets
from pydantic import Field

from coffee_platform.partitions import daily
from coffee_platform.pos import (
    LANDING_DATASET,
    POS_TABLES,
    Extract,
    bronze_select,
    extract_to_landing,
    landing_files,
)
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
    context: AssetExecutionContext,
    config: ExtractConfig,
    workspace: Workspace,
    clickhouse: ClickHouse,
):
    """dlt with a lag window of `lag_days`, as Parquet under dt=<partition>."""
    lag_seconds = timedelta(days=config.lag_days).total_seconds() or None
    context.log.info(f"lag window: {config.lag_days} days")
    extract = extract_to_landing(context, workspace, lag_seconds=lag_seconds)
    written = write_manifest(clickhouse, workspace, context.partition_key, extract)
    context.log.info(f"manifest: {written}")
    for table, rows in extract.rows.items():
        yield MaterializeResult(
            asset_key=AssetKey([PREFIX, f"landing_{table}"]),
            metadata={"rows": rows, "lag_days": config.lag_days},
        )


# ----------------------------------------------------------------- manifest

MANIFEST_STRUCTURE = "load_id String, table_name String, rows UInt64, files Array(String)"


def _manifest_glob(workspace: Workspace, dt: str, name: str = "*") -> str:
    return f"{workspace.name}/{LANDING_DATASET}/_manifests/dt={dt}/{name}.jsonl"


def write_manifest(
    clickhouse: ClickHouse, workspace: Workspace, dt: str, extract: Extract
) -> dict[str, int]:
    """One line per table the load wrote: rows promised, files written. Returns rows per table."""
    if extract.load_id is None:
        return {}
    client = clickhouse.client()
    lines = []
    for table, rows in extract.rows.items():
        if rows == 0:
            continue
        pattern = f"{workspace.name}/{LANDING_DATASET}/{table}/dt={dt}/{extract.load_id}.*"
        files = client.query(
            f"SELECT DISTINCT _file FROM s3(landing_s3, filename = '{pattern}', format = 'One') "
            "SETTINGS s3_throw_on_zero_files_match = 0"
        ).result_rows
        names = sorted(f[0] for f in files)
        lines.append(f"('{extract.load_id}', '{table}', {rows}, {names!r})")
    if not lines:
        return {}
    note = _manifest_glob(workspace, dt, extract.load_id)
    client.command(
        f"INSERT INTO FUNCTION s3(landing_s3, filename = '{note}', format = 'JSONEachRow', "
        f"structure = '{MANIFEST_STRUCTURE}') VALUES {', '.join(lines)}"
    )
    return {t: r for t, r in extract.rows.items() if r}


def reconcile(clickhouse: ClickHouse, workspace: Workspace, table: str, dt: str) -> list[str]:
    """What is wrong with a day's landing files of `table` against its manifests; empty if none."""
    client = clickhouse.client()
    data = f"{workspace.name}/{LANDING_DATASET}/{table}/dt={dt}/*.parquet"
    # Look for files first: reading Parquet from no file at all fails (ClickHouse
    # cannot infer a schema), and a day whose only file is missing has none.
    present = client.query(
        f"SELECT count() FROM s3(landing_s3, filename = '{data}', format = 'One') "
        "SETTINGS s3_throw_on_zero_files_match = 0"
    ).result_rows[0][0]
    actual = {}
    if present:
        actual = dict(
            client.query(
                f"SELECT _file, count() FROM s3(landing_s3, filename = '{data}', "
                "format = 'Parquet') GROUP BY _file"
            ).result_rows
        )
    notes = _manifest_glob(workspace, dt)
    promised = client.query(
        f"SELECT load_id, rows, files FROM s3(landing_s3, filename = '{notes}', "
        f"format = 'JSONEachRow', structure = '{MANIFEST_STRUCTURE}') "
        f"WHERE table_name = '{table}' SETTINGS s3_throw_on_zero_files_match = 0"
    ).result_rows

    problems = []
    listed = {file for _, _, files in promised for file in files}
    problems += [f"stray file {f}: no manifest lists it" for f in sorted(set(actual) - listed)]
    for load_id, rows, files in promised:
        missing = [f for f in files if f not in actual]
        problems += [f"file {f} of load {load_id} is missing" for f in missing]
        held = sum(actual.get(f, 0) for f in files)
        if not missing and held != rows:
            problems.append(f"load {load_id} promised {rows} rows, its files hold {held}")
    return problems


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
        """Hardened: check the day's files against their manifest, rebuild the day, swap it in."""
        dt = context.partition_key
        problems = reconcile(clickhouse, workspace, table, dt)
        if problems:
            raise Failure(
                description=f"{table} on {dt}: landing files do not match their manifest: "
                + "; ".join(problems),
                metadata={"problems": problems},
            )
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

# Ingest and load halves, as for naive (see pos.py). The lag window is config
# of the extract op, so only the jobs that extract take it.
pos_ingest_hardened = define_asset_job(
    "pos_ingest_hardened",
    selection=AssetSelection.assets(hardened_pos_landing),
    partitions_def=daily,
    executor_def=in_process_executor,
)
pos_load_hardened = define_asset_job(
    "pos_load_hardened",
    selection=AssetSelection.assets(*HARDENED_BRONZE_ASSETS, hardened_pos_dbt),
    partitions_def=daily,
    executor_def=in_process_executor,
)
