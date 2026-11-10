from dagster import Definitions
from dagster_dbt import DbtCliResource

from coffee_platform.pos import BRONZE_ASSETS, DBT_PROJECT, pos_daily, pos_dbt, pos_landing
from coffee_platform.resources import Workspace, clickhouse_from_env
from coffee_platform.smoke import smoke_job, smoke_ping

defs = Definitions(
    assets=[smoke_ping, pos_landing, *BRONZE_ASSETS, pos_dbt],
    jobs=[smoke_job, pos_daily],
    resources={
        "workspace": Workspace.configure_at_launch(),
        "clickhouse": clickhouse_from_env(),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT),
    },
)
