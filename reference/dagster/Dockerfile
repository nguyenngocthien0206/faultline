# One image for the three Dagster services (code server, webserver, daemon),
# like Dagster's own Docker deployment example.
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
COPY requirements.lock /tmp/requirements.lock
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r /tmp/requirements.lock

COPY dagster.yaml workspace.yaml ${DAGSTER_HOME}/
COPY coffee_platform /opt/dagster/app/coffee_platform

WORKDIR /opt/dagster/app
