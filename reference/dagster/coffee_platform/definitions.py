from dagster import Definitions

from coffee_platform.resources import Workspace, clickhouse_from_env
from coffee_platform.smoke import smoke_job, smoke_ping

defs = Definitions(
    assets=[smoke_ping],
    jobs=[smoke_job],
    resources={
        "workspace": Workspace.configure_at_launch(),
        "clickhouse": clickhouse_from_env(),
    },
)
