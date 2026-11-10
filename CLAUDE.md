# faultline: project conventions

faultline is a chaos and invariant testing tool for data platforms. It treats a platform as a black box, breaks things around it (landing files, process execution, later sources), and scores the outputs against an oracle. Read [docs/design.md](docs/design.md) before changing anything structural; it is the source of truth for the architecture, interfaces and outcome rules.

## Commands

All commands go through uv. `make` wraps them.

| Task | Command |
|---|---|
| Install / sync env | `make install` (`uv sync --locked`) |
| Lint + format check | `make lint` |
| Auto-fix + format | `make fmt` |
| All tests | `make test` |
| Fast tests only | `make test-unit` (skips `@pytest.mark.integration`) |
| What CI runs | `make check` |
| Add a dependency | `uv add <pkg>` or `uv add --dev <pkg>`, never edit the lock by hand |

Local development pins Python 3.11 (`.python-version`), the lowest supported version, so newer syntax is caught locally. CI runs ubuntu 3.11, ubuntu 3.14 and windows 3.11.

## Layout

```
src/faultline/                 the tool (src layout, built with uv_build)
src/faultline/reference_platform/   the demo platform under test (black box)
tests/                         pytest; integration tests marked `integration`
experiments/  targets/         YAML specs (added once the spec exists)
docs/design.md                 architecture and decisions
```

See section 11 of the design for the full planned tree. Create modules only when they are needed.

## Rules

- **Black box.** `reference_platform` must never import from the rest of `faultline`. A test enforces this. faultline talks to it only through the adapter (files in, subprocess runs, Parquet out).
- **Safety guard.** Anything destructive must go through the guard: target declares `environment: test` and `allow_destructive: true`, the runner re-checks, adapters check their own paths. Never add a bypass flag.
- **Determinism.** Seeded generators, integer money (VND), no wall-clock values in compared outputs. Faults are schedule transformers, so an experiment is plain data before it executes.
- **Tests.** Every fault and every property gets its own tests, including at least one that proves it catches the naive reference platform.
- **Keep it small.** Minimal dependencies (runtime: typer, pydantic, duckdb, pyyaml, faker as they become needed). No speculative abstractions; get end to end first, then widen.

## Code style

- Python 3.11+, type hints everywhere, `from __future__` not needed.
- Ruff is the only linter and formatter (line length 100). Run `make fmt` before handing work over.
- Prefer frozen dataclasses for value objects and pydantic only for parsing user-facing specs.
- Docstrings and comments explain *why*, not what the next line does.

## Language and writing

- Code, comments, docstrings, README and docs are in English. Conversation with the maintainer is in Vietnamese.
- Never use the em dash character anywhere (code, comments, docs, commit messages).
- README and docs speak to both engineers and data team managers, weaving technical depth with operational meaning rather than splitting them into separate sections.

## Workflow

- Work is organised issue by issue. Branch names, PR titles and PR descriptions never mention phases, milestones or step numbers.
- No ADRs. Design reasoning lives in `docs/design.md`.
- One commit per logical unit, Conventional Commits prefix (`feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `build`, `ci`), one-line message, no AI attribution trailer.
- `make check` must be green before work is handed over. Do not push unless asked.
