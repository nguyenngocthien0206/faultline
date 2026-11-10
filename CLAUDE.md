# faultline: project conventions

faultline is a chaos and invariant testing tool for data platforms. It treats a platform as a black box, breaks things around it (source database, landing files, run execution), and scores the outputs against an oracle. Read [docs/design.md](docs/design.md) before changing anything structural; it is the source of truth for the architecture, interfaces and outcome rules.

## Commands

All Python commands go through uv; Docker commands go through compose. `make` wraps both.

| Task | Command |
|---|---|
| Install / sync env | `make install` (`uv sync --locked`) |
| Lint + format check | `make lint` |
| Auto-fix + format | `make fmt` |
| Tests that need no Docker | `make test` (`-m "not integration"`) |
| What CI runs on every OS | `make check` (lint + test) |
| Start the reference stack | `make ref-up` (build, start, wait until healthy) |
| Integration tests | `make test-integration` (needs `ref-up`; fails instead of skipping) |
| Stop stack, wipe volumes | `make ref-down` |
| Stack status / logs | `make ref-ps`, `make ref-logs` |
| Add a dependency | `uv add <pkg>` or `uv add --dev <pkg>`, never edit the lock by hand |
| Platform dependency | edit `reference/dagster/pyproject.toml`, then regenerate `requirements.lock` with the command in that file |

Local development pins Python 3.11 (`.python-version`), the lowest supported version, so newer syntax is caught locally. CI runs ubuntu 3.11, ubuntu 3.14 and windows 3.11 for unit tests, plus an ubuntu job that runs the integration tests against the reference stack. Integration tests skip when the stack is down unless `FAULTLINE_REQUIRE_STACK=1`.

Reference stack endpoints (all on 127.0.0.1, test-only credentials `faultline` / `faultline`): Dagster UI and GraphQL `:3000`, ClickHouse HTTP `:8123`, source Postgres `:5433`, RustFS S3 `:9000` and console `:9001/rustfs/console/` (`faultline` / `faultline-secret`; the bare `:9001` answers 403).

## Layout

```
src/faultline/                 the tool (src layout, built with uv_build)
  datagen/                     seeded coffee chain generator
  connectors/                  Dagster GraphQL, ClickHouse HTTP, Postgres clients + workspace guard
reference/                     the platform under test (docker compose), never imported by faultline
  dagster/coffee_platform/     Dagster definitions: dlt extraction, ClickHouse loads, dbt
tests/<area>/                  pytest; tests needing the stack are marked `integration`
experiments/  targets/         YAML specs (added once the spec exists)
docs/design.md                 architecture and decisions
```

See section 11 of the design for the full planned tree. Create modules only when they are needed.

## Rules

- **Black box.** `reference/` never imports faultline, and faultline never imports `coffee_platform`. They only meet through the source database, the S3 bucket, Dagster's GraphQL API and ClickHouse's HTTP API, the same interfaces an adapter for a team's own stack would use.
- **Workspaces.** Every experiment works in its own workspace (`fl_` + 8 hex): Postgres schema, ClickHouse database and S3 prefix of the same name. Anything destructive validates the name first (`faultline.connectors.workspace`); never drop or truncate by any other name.
- **Safety guard.** Anything destructive must go through the guard: target declares `environment: test` and `allow_destructive: true`, the runner re-checks, adapters check their own scope. Never add a bypass flag.
- **Determinism.** Seeded generators, integer money (VND), no wall-clock values in compared outputs. Faults are schedule transformers, so an experiment is plain data before it executes.
- **Tests.** Every fault and every property gets its own tests, including at least one that proves it catches the naive reference platform.
- **Keep it small.** Minimal dependencies for faultline itself (runtime: typer, faker, then pydantic, duckdb, pyyaml as needed; psycopg only in the `reference` extra). No speculative abstractions; get end to end first, then widen.

## Gotchas learned the hard way

- **No `from __future__ import annotations` in Dagster code.** Dagster inspects the `context` annotation at load time and rejects a string annotation with a confusing message.
- **A healthy code server can still have broken definitions.** Check `DagsterClient.location_error()`; the integration fixtures already do.
- **SQLAlchemy stays below 2.1 in the platform image.** 2.1 breaks Dagster's Postgres storage ("the connection is closed" on every launch).
- **RustFS is path-style only.** Every S3 client needs path-style addressing (dlt: `S3_URL_STYLE=path`).
- **ClickHouse XML users cannot receive SQL GRANTs.** Privileges such as named collection access go in `reference/clickhouse/users.d/`.
- **Git Bash rewrites absolute paths** passed to `docker exec` or `docker run`; prefix such commands with `MSYS_NO_PATHCONV=1`.
- **`from conftest import ...` picks the wrong file** once several test folders have a `conftest.py`. Put shared test helpers in a uniquely named module (`datagen_helpers.py`, `reference_stack.py`).

## Code style

- Python 3.11+, type hints everywhere.
- Ruff is the only linter and formatter (line length 100). Run `make fmt` before handing work over.
- Prefer frozen dataclasses for value objects and pydantic only for parsing user-facing specs.
- Docstrings and comments explain *why*, not what the next line does.

## Language and writing

- Code, comments, docstrings, README and docs are in English. Conversation with the maintainer is in Vietnamese.
- Never use the em dash character anywhere (code, comments, docs, commit messages).
- README and docs speak to both engineers and data team managers, weaving technical depth with operational meaning rather than splitting them into separate sections.

## Workflow

- Every issue starts with a plan the maintainer approves; implementation starts only after that.
- Work is organised issue by issue. Branch names, PR titles and PR descriptions never mention phases, milestones or step numbers.
- No ADRs. Design reasoning lives in `docs/design.md`.
- One commit per logical unit, Conventional Commits prefix (`feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `build`, `ci`), one-line message, no AI attribution trailer.
- `make check` (and `make test-integration` when the stack is touched) must be green before work is handed over. Do not push unless asked.
