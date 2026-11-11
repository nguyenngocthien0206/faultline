from dagster import Definitions
from dagster_dbt import DbtCliResource

from coffee_platform.pos import BRONZE_ASSETS, DBT_PROJECT, pos_daily, pos_dbt, pos_landing
from coffee_platform.pos_hardened import (
    HARDENED_BRONZE_ASSETS,
    HARDENED_DBT_PROJECT,
    hardened_pos_dbt,
    hardened_pos_landing,
    pos_daily_hardened,
)
from coffee_platform.resources import Workspace, clickhouse_from_env
from coffee_platform.smoke import smoke_job, smoke_ping

defs = Definitions(
    assets=[
        smoke_ping,
        pos_landing,
        *BRONZE_ASSETS,
        pos_dbt,
        hardened_pos_landing,
        *HARDENED_BRONZE_ASSETS,
        hardened_pos_dbt,
    ],
    jobs=[smoke_job, pos_daily, pos_daily_hardened],
    resources={
        "workspace": Workspace.configure_at_launch(),
        "clickhouse": clickhouse_from_env(),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT),
        "dbt_hardened": DbtCliResource(project_dir=HARDENED_DBT_PROJECT),
    },
)
