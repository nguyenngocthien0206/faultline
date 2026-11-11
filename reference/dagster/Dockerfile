# One image for the three Dagster services (code server, webserver, daemon),
# like Dagster's own Docker deployment example. Build context: reference/.
FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/home \
    PYTHONPATH=/opt/dagster/app

# uv installs the ~200 pinned packages several times faster than pip, which
# matters because CI rebuilds this image on every run.
COPY --from=ghcr.io/astral-sh/uv:0.11.14 /uv /usr/local/bin/uv

# Dependencies first, in their own layer, so code changes rebuild in seconds.
# The cache mount keeps downloaded wheels across builds: when the lock changes,
# only the changed packages are fetched again.
COPY dagster/requirements.lock /tmp/requirements.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r /tmp/requirements.lock

COPY dagster/dagster.yaml dagster/workspace.yaml ${DAGSTER_HOME}/

# dbt projects (naive, hardened), parsed at build time so dagster-dbt finds
# target/manifest.json. Parsing never connects to the warehouse. The hardened
# project reads the shared macros from ../dbt, so both are copied first.
COPY dbt /opt/dagster/app/dbt
COPY dbt_hardened /opt/dagster/app/dbt_hardened
RUN dbt parse --project-dir /opt/dagster/app/dbt --profiles-dir /opt/dagster/app/dbt --quiet \
 && dbt parse --project-dir /opt/dagster/app/dbt_hardened \
      --profiles-dir /opt/dagster/app/dbt_hardened --quiet

COPY dagster/coffee_platform /opt/dagster/app/coffee_platform

WORKDIR /opt/dagster/app
