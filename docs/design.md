# faultline design

Status: draft for review. Nothing here is implemented yet.

## 1. Purpose and scope

Data quality tools such as Soda and Great Expectations ask: *is this table clean?*
faultline asks a different question: *does the whole pipeline still produce the right answer when things go wrong around it?*

Real platforms rarely fail because a SQL statement is wrong on a sunny day. They fail when a file lands twice, a partition gets a late arrival after it was closed, a job dies halfway through a write, or a retry replays work that was already committed. Those failures are cheap to cause in a test environment and expensive to discover in production, where they usually surface as an on-call page or, worse, as a dashboard that is quietly wrong for weeks.

faultline treats the platform as a black box. It does not patch or fork dlt, Airbyte, ADF, dbt or anything else. It breaks the things *around* the platform:

```
[Fake source] -> ingestion (dlt/Airbyte/ADF) -> [Landing zone] -> Bronze -> Silver -> Gold
     (1)               (3)                           (2)                          ^
 mutate data      kill / rerun                drop bad files                read and score
```

1. **Source side**: faultline owns a fake source (test DB, mock API, file folder) and mutates it. Post-MVP.
2. **Landing side**: faultline impersonates the ingestion tool and writes files straight into landing. Works for any team regardless of the ingestion tool. MVP.
3. **Execution side**: faultline interrupts, retries, reruns and backfills runs. MVP.

After each scenario it reads the consumer-visible outputs and scores them against an oracle.

### Non-goals

- Testing business logic. Whether revenue should exclude refunds is a job for dbt tests or Soda.
- Fuzzing the internals of third-party tools.
- Running anywhere other than a disposable test or CI environment (see section 9).

## 2. Glossary

| Term | Meaning |
|---|---|
| **Oracle** | A *test oracle*: the source of the correct answer that outputs are compared against. Not the Oracle database. |
| **Fault** | One thing faultline breaks, for example "deliver the same file twice". |
| **Injection point** | Where a fault acts: `SOURCE`, `LANDING` or `EXECUTION`. |
| **Steady state** | The behaviour of the platform when nothing is broken. Established by a clean baseline run. |
| **Schedule** | The ordered list of actions an experiment performs: deliver files, trigger runs, backfill. |
| **Trace** | What actually happened when a schedule was executed: one entry per action, with run results and output snapshots. |
| **Checkpoint** | A labelled point in the trace where properties look at the outputs (for example `F`, end of the fault phase). |
| **Probe** | Extra actions a property adds to exercise itself, such as a rerun for idempotency. |
| **Property** | An invariant the platform should keep under faults, such as idempotency or atomicity. |
| **Signal** | An error or alert *raised by the platform itself*. A crash faultline injected is not a signal. |
| **Outcome** | The verdict per property and per experiment: `PASS`, `DETECTED`, `BROKEN`, `SILENT_CORRUPTION`, `SKIPPED`, `ERROR`. |

## 3. Architecture overview

```
experiment.yaml + target.yaml
          |
          v
   spec (pydantic) ---> safety guard ---> runner
                                            |
          +---------------------------------+
          v
   datagen (seeded) --> clean schedule --+--> baseline execution --> baseline snapshots --+
                                         |                                                |
                                         +--> faults.apply() --> faulted execution --> trace
                                                                    |                     |
                                                     adapter.place_input()                v
                                                     adapter.trigger_run()      oracle (baseline + key invariants)
                                                     adapter.read_output()                |
                                                                                          v
                                                                       properties.check() --> classifier
                                                                                          |
                                                                                          v
                                                                   results/*.json --> scorecard (md + json)
```

Core decisions, each with the reason behind it:

**An experiment is a schedule of actions, and a fault is a function that transforms the schedule.**
Actions are small data objects: `Deliver(file)`, `RunNext()`, `RunPartition(date)`, `Backfill(start, end)`, each optionally carrying an interrupt. A duplicate-file fault inserts a second `Deliver`. A late-file fault moves a `Deliver` later in the list. A crash fault attaches an interrupt to one run.
Why: the whole experiment is plain data before anything executes, so it is deterministic, easy to print and diff, faults compose by function composition, and fault logic can be unit tested without starting a platform.

**Execution produces a trace, and properties are checkers over the trace.**
This is the model Jepsen uses for distributed databases: record a history, then check it against a model. The runner records each action, its run result and a snapshot of the consumer-visible tables after every run. Properties never drive the platform directly (apart from declaring probes), they only read the trace.
Why: separating "what happened" from "is it acceptable" keeps properties pure and testable on hand-built traces, and lets one execution be scored by many properties.

**Adapters hand over outputs as Parquet snapshots.**
`read_output` exports a table to a Parquet file. All fingerprinting and diffing is done by faultline in DuckDB over those files.
Why: adapters stay thin (any warehouse can export Parquet), faultline needs no pandas or pyarrow dependency, and diffs scale beyond what fits comfortably in Python lists.

**Every platform run is a real run on a real stack, outside faultline's process.**
The reference platform runs in Docker and is driven only through public interfaces (a source database, an S3 bucket, Dagster's GraphQL API, ClickHouse's HTTP API).
Why: a crash must be a real crash. If the platform ran in faultline's process, a "crash" would be an exception that `finally` blocks and context managers clean up politely, which is exactly the situation that never happens when a node dies. Driving it through public interfaces also proves that faultline can plug into a team's platform the same way.

## 4. Key interfaces

Signatures are indicative; exact types are settled when each interface is implemented.

### 4.1 Adapter

Implemented in `faultline.adapters` (`base.py`, and `reference.py` for the reference stack):

```python
class Capability(StrEnum):
    DELIVER_DAY = "input.deliver_day"  # make one day of input available to the platform
    RUN_PARTITION = "run.partition"  # run the pipeline for one partition
    SNAPSHOT = "output.snapshot"  # export a consumer-visible table to Parquet


@dataclass(frozen=True)
class TableContract:
    name: str
    key: tuple[str, ...]
    measures: tuple[str, ...] = ()  # numeric columns whose totals are reported
    ignore: tuple[str, ...] = ()  # volatile columns left out of comparisons
    truth_dataset: str | None = None  # generated dataset whose keys must all appear


@dataclass(frozen=True)
class Capabilities:
    features: frozenset[Capability]
    tables: tuple[TableContract, ...]


class RunStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"  # the platform's own failure: a signal
    INTERRUPTED = "INTERRUPTED"  # killed by faultline on purpose: not a signal


@dataclass(frozen=True)
class RunRequest:
    partition: date


@dataclass(frozen=True)
class RunResult:
    status: RunStatus
    errors: tuple[str, ...] = ()  # root causes, not wrapper messages
    run_id: str | None = None


class Adapter(ABC):  # also a context manager: setup on enter, teardown on exit
    def capabilities(self) -> Capabilities: ...
    def setup(self) -> None: ...  # fresh, isolated sandbox
    def deliver(self, day: date) -> None: ...  # the platform's normal input for one day
    def run(self, request: RunRequest) -> RunResult: ...
    def snapshot(self, table: str, dest: Path) -> Path: ...  # Parquet
    def teardown(self) -> None: ...
```

**`deliver(day)` instead of `place_input(file)`.** The steady-state input is whatever "one more day" means for the platform: the reference stack replays the day's changes into its source Postgres, a file-based platform would drop the day's files into its landing zone. faultline does not need to know which. Faults that reach deeper (dropping an extra file into S3, rewriting a source row, killing a run) will use optional hooks declared as capabilities, added together with the faults that need them: `landing.files`, `source.mutate`, `interrupt.failpoint`, `run.backfill`.

The reference adapter checks that the Dagster code location loaded before setting up anything, creates the workspace (Postgres schema and source tables), replays days with `faultline.sources.pos`, launches `pos_daily` through GraphQL, and snapshots tables with ClickHouse's `FORMAT Parquet` streamed to disk (written to a temporary name and renamed, so a failed export never looks like a valid snapshot). It only exports tables it declares.

Design note, deviation from the brief: the brief lists `interrupt` as an optional method. Here an interrupt will be part of the `RunRequest` and implemented as a **failpoint**: the platform exits hard (`os._exit`) at a named point. An asynchronous `interrupt()` call would race with the run (did the write finish before the kill arrived?) and would make results flaky. Failpoints are deterministic, behave the same on Windows and Linux, and are a well known technique (FoundationDB, TiKV and etcd all use them). Black-box adapters that cannot host failpoints can still implement an interrupt point by other means, such as a cancel API or "kill when this file appears".

A capability the adapter does not declare turns the dependent fault or property into `SKIPPED`, never into a failure.

### 4.2 FaultInjector

```python
class InjectionPoint(StrEnum):
    SOURCE = "source"
    LANDING = "landing"
    EXECUTION = "execution"


class FaultInjector(ABC):
    name: ClassVar[str]
    injection_point: ClassVar[InjectionPoint]
    requires: ClassVar[frozenset[Capability]]

    def apply(self, schedule: Schedule, ctx: FaultContext) -> Schedule: ...
    # undo the cause, e.g. redeliver the correct file
    def heal(self, ctx: FaultContext) -> list[Action]: ...
    def affected_partitions(self, ctx: FaultContext) -> set[date]: ...
```

MVP faults:

| Fault | Point | What it does | Heal |
|---|---|---|---|
| `duplicate_file` | landing | Delivers a file twice, under the same name (overwrite) or a new name (ingestion retry) | none |
| `late_file` | landing | Delivers a file into partition D after D has been processed | none |
| `column_rename` | landing | Delivers a file whose column is renamed (`amount_vnd` -> `total_amount_vnd`) | redeliver with the agreed schema |
| `truncated_file` | landing | Cuts a file, by default on a line boundary so it still parses | redeliver the full file |
| `crash_mid_write` | execution | Interrupts a stage at `mid_write` (data or state file) | none |
| `retry_after_partial_commit` | execution | Interrupts at `after_write_before_state`, then lets the orchestrator retry | none |

A line-boundary truncation is the default on purpose. A file cut mid-line usually fails to parse and gets noticed. A file that is short but well formed is the dangerous case, and catching it needs a delivery contract (a manifest with a row count or checksum), which is exactly what the hardened platform demonstrates.

### 4.3 Property

```python
class Verdict(StrEnum):
    HOLDS = "holds"
    VIOLATED = "violated"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class PropertyResult:
    verdict: Verdict
    checkpoint: str | None
    evidence: str  # one human readable line, plus structured diff counts
    reason: str | None  # for SKIPPED


class Property(ABC):
    name: ClassVar[str]
    requires: ClassVar[frozenset[Capability]]

    def probe(self, ctx: ProbeContext) -> list[Action]:  # default: no probe
        return []

    def check(self, trace: Trace, oracle: Oracle) -> list[PropertyResult]: ...
```

`check` returns one result per checkpoint it inspects, and the classifier (section 6) turns those into a single outcome.

### 4.4 Oracle

The oracle is **hybrid**:

1. **Differential baseline.** faultline runs the clean schedule on a fresh environment and snapshots the consumer tables after every logical step. The faulted run must match those snapshots at the relevant checkpoints. faultline never needs to know how revenue is computed, so this works for any platform.
2. **Key invariants from the generator.** Because faultline generated the source data, it knows the set of keys that must exist per table (for example every `order_id`). It checks that each key appears exactly once and that none is missing. This also validates the baseline itself: a platform that is wrong even on a clean run would otherwise produce a wrong answer key.

Implemented in `faultline.oracle`, all in an embedded DuckDB over Parquet snapshots, whatever warehouse produced them:

```python
snapshot_all(adapter, dest_dir) -> dict[str, Path]  # every declared table to Parquet
fingerprint(path, contract) -> Fingerprint  # rows, distinct keys, row hash, measure sums
diff(expected, actual, contract) -> Diff  # bucket counts, sample keys, measure deltas, summary()
unique_keys(path, contract) -> InvariantResult
complete_keys(path, contract, generated_root) -> InvariantResult  # vs the generator's keys
```

Measured on the reference stack: two clean baselines in two separate workspaces fingerprint identically for every consumer table, which is what makes the differential oracle trustworthy.

**Fingerprint** per table: row count, distinct key count, a multiset hash, and the sum of each measure column. The multiset hash is the sum of per-row hashes modulo 2^64. XOR is the tempting choice and the wrong one: two identical rows XOR to zero, so a fingerprint built on XOR is blind to duplicates, which are one of the main things faultline exists to catch. DuckDB's `hash()` can change between DuckDB versions, so fingerprints are only compared within one faultline run, never stored.

**Diff** (when fingerprints differ) groups both snapshots by key (row count and row-hash sum per key), joins them, and puts every key into at most one bucket:

| Bucket | Meaning | Feeds property |
|---|---|---|
| missing | key in the expected snapshot only | no data loss |
| extra | key in the actual snapshot only | exactly-once |
| duplicated | more rows for the key in the actual snapshot | exactly-once |
| changed | same or fewer rows, different content | no silent corruption |

Totals alone are not enough. A unit test builds a snapshot with one key missing, one duplicated, one changed and one extra, and the revenue total comes out exactly equal to the expected one; only the keyed buckets reveal the damage. On the reference stack, rerunning one day of the naive pipeline yields: `silver_orders` identical, `silver_order_items` 133 duplicated keys, `gold_daily_store_sales` 4 changed keys with net revenue +19.3% and items sold +56.4% (duplicates on both sides of a join multiply instead of adding up).

**Canonicalisation** before hashing or diffing: columns ordered by name, timestamps normalised to UTC, declared `ignore_columns` dropped (load timestamps, run ids), money stored as integer VND. Floats are avoided in the reference data because `0.1 + 0.2` summed in a different order is a different number, and the diff would report noise as corruption.

## 5. Experiment lifecycle

```
0. guard        refuse unless the target is declared a destructive test environment
1. generate     dataset from seed -> clean schedule
2. baseline     workspace A: execute clean schedule, snapshot after every run, check key invariants
                  invariants fail -> ERROR "steady state not established", stop
3. fault phase  workspace B (fresh): execute faults.apply(clean schedule)        -> checkpoint F
4. recovery     execute fault.heal(), then rerun affected partitions until the
                outputs match the baseline or max_runs is reached                -> checkpoint R
5. probes       execute property probes (rerun, backfill), each with its own checkpoint
6. score        properties.check(trace, oracle) -> classifier -> results JSON (+ snapshots for diff)
```

Notes:

- Never inject faults into a system that is not healthy. That is the first rule of chaos engineering, and step 2 enforces it.
- The baseline is cached per (dataset spec, target), so a suite of experiments over the same dataset pays for it once.
- Recovery reruns the affected partitions because that is the standard operator move after an incident. The number of runs needed is reported as the convergence metric.
- Each experiment gets its own workspace directories, so experiments can run in parallel and never contaminate each other.

## 6. Outcome classification

A **signal** is an error or alert raised by the platform itself: a run that fails with its own error, or an alert it records. A run that died because faultline killed it is `INTERRUPTED` and is **not** a signal. The process dying says nothing about whether the platform noticed the data problem.

This is a deliberate consumer-centric view. When a naive job crashes halfway through rewriting a gold table, the scheduler shows red, but the analyst reading the table sees half a day of revenue with no marker on it. faultline scores what the data consumer experiences.

Per property, given results at checkpoints F and R:

| Situation | Outcome |
|---|---|
| Holds at F and at R | `PASS` |
| Violated at F with a signal, holds at R | `DETECTED` |
| Violated at R, with a signal in the relevant runs | `BROKEN` |
| Violated at F or R with no signal in the relevant runs | `SILENT_CORRUPTION` |
| Required capability missing, or the property was not exercised | `SKIPPED` (with reason) |
| Harness failure or baseline not established | `ERROR` |

- A violation at F without a signal is `SILENT_CORRUPTION` even if R later heals. Consumers read wrong numbers in between and nobody raised a hand.
- `ERROR` is an addition to the brief. Without it, a bug in faultline or an unhealthy baseline would be reported as a verdict about the platform.
- The experiment outcome is the worst property outcome, ordered `SILENT_CORRUPTION > BROKEN > DETECTED > PASS`. `SKIPPED` and `ERROR` are shown separately and never mask a real verdict.
- Atomicity uses its own checkpoint, right after an interrupted run: the consumer snapshot there must equal either the actual snapshot before that run or the baseline after that step. Anything in between is a partial result.

## 7. Properties

| Property | Checkpoint | Violated when | Probe |
|---|---|---|---|
| `idempotency` | after probe | outputs change when an already successful partition is rerun | rerun a processed partition |
| `atomicity` | right after each interrupted run | consumers can see a partial result | none (needs an interrupt fault, else `SKIPPED`) |
| `exactly_once` | F, R | duplicated keys, or aggregates inflated after a retry | none |
| `no_data_loss` | F, R | keys missing compared with the generator truth or baseline | none |
| `backfill_equivalence` | after probe | backfilling a date range gives a different result from the baseline incremental run | backfill a window |
| `convergence` | R | outputs do not match the baseline within `max_runs` (run count reported) | none |
| `no_silent_corruption` | F, R | the full snapshot differs from the baseline, including changed values such as revenue turned NULL | none |

Every property gets its own unit tests on hand-built traces, plus at least one integration test proving it catches the naive reference platform.

## 8. Reference platform

A small but real data stack ships with faultline, so the idea can be demonstrated end to end on a laptop and in CI with the tools teams actually run: Postgres as the business database, dlt for ingestion, an S3 bucket as the landing zone, ClickHouse as the warehouse, dbt for transformations and Dagster to orchestrate it all. It is a showcase, not a requirement: a team plugs faultline into its own stack by writing an adapter (section 4.1).

```
 faultline (host)                         reference stack (docker compose, reference/)
 ----------------                         ---------------------------------------------
 replay change set of day D  --SQL-->     source-db      Postgres, one schema per workspace
                                              | dlt sql_database, incremental on updated_at
                                              v
                                          rustfs         S3: landing/<ws>/pos/<table>/dt=D/*.parquet
                                              | ClickHouse s3() through the landing_s3 named collection
                                              v
                                          clickhouse     <ws>.bronze_*  --dbt-->  silver  -->  gold
 launchRun(partition = D)  --GraphQL-->   dagster        webserver + code server (runs) + daemon
 read outputs              --HTTP-->      clickhouse
```

**Services.**

| Service | Image | Role |
|---|---|---|
| `source-db` | `postgres:16` | the simulated business (POS) database faultline replays into and later corrupts |
| `rustfs` | `rustfs/rustfs:1.0.4` | S3-compatible landing zone (MinIO no longer publishes community images) |
| `rustfs-init` | `amazon/aws-cli` | one-shot: creates the `landing` bucket |
| `clickhouse` | `clickhouse/clickhouse-server:26.8.16.41` | warehouse; reads landing files with `s3(landing_s3, filename = ...)` so SQL never carries credentials |
| `dagster-db` | `postgres:16` | Dagster's own storage, deliberately separate from the business database |
| `dagster-code` | built from `reference/dagster/` | gRPC code server; runs execute here (`DefaultRunLauncher`, no Docker socket) |
| `dagster-webserver` | same image | UI and the GraphQL API faultline drives |
| `dagster-daemon` | same image | schedules, sensors, run retries |

Every port binds to `127.0.0.1`. Credentials are test-only values kept in one place, `reference/.env` (gitignored), which `make ref-up` creates from the committed `reference/.env.example`; compose, ClickHouse's named collection and the integration tests all read that file, and compose refuses to start when a variable is missing.

**Workspaces.** Each experiment works in its own workspace, a name like `fl_ab12cd34` that is at once the Postgres schema of its source tables, its ClickHouse database and its S3 prefix. The name travels in the run config of every Dagster run. Isolation lets experiments run in parallel on one stack, and the name doubles as a guard: faultline's connectors refuse to drop anything that does not match `^fl_[0-9a-f]{8}$`, and the platform validates the same pattern on its side.

**Black box.** The platform code (`reference/dagster/coffee_platform`) never imports faultline, and faultline never imports it; they live in different processes, images and dependency sets. faultline touches the platform only through the source database, the bucket, Dagster's GraphQL API and ClickHouse's HTTP API, exactly what an adapter for a team's stack would use. The demo therefore cannot cheat.

**The POS pipeline.** One Dagster job, `pos_daily`, partitioned by day; each run executes in a single process (`in_process_executor`, which saves about 15 s of re-imports per step):

1. `pos_landing` (dlt `sql_database`): extracts `orders`, `order_items` and `payments` incrementally on `updated_at`, `created_at` and `paid_at`, and writes Parquet to `landing/<ws>/pos/<table>/dt=<partition>/`.
2. `bronze_<table>` (ClickHouse): appends the partition's landing files with `INSERT ... SELECT FROM s3()`, adding `_source_file` and `_dt`. Tables take the schema of the first file seen.
3. dbt models (dagster-dbt, one asset per model), all in the workspace database:

| Table | Key | Materialization | Content |
|---|---|---|---|
| `silver_order_events` | none (change log) | view | every order row the source emitted, typed, with `extracted_on` |
| `silver_orders` | `order_id` | view | latest state per order (`argMax` by `updated_at`) |
| `silver_order_items` | `order_id, line_no` | view | order lines |
| `silver_payments` | `payment_id` | view | charges and negative refund rows |
| `gold_daily_store_sales` | `order_date, store_id` | incremental `delete_insert` | `orders`, `items_sold`, `gross_vnd`, `discount_vnd`, `net_vnd` per business day (Vietnam time) |
| `gold_daily_payments` | `paid_date, store_id, method` | incremental `delete_insert` | `charges`, `charged_vnd`, `refunded_vnd`, `net_vnd` |

Gold recomputes only the business days touched by the run's extract: the day itself plus the original days of refunds that came in. On the happy path the naive gold matches the generator's truth to the dong (checked by `tests/reference/test_naive_pipeline.py`).

**Naive versus hardened.** Naive is not a strawman. Every one of its choices is common in real pipelines and passes a happy-path test. Order lifecycle updates (voids, refunds days later) are part of the steady state, so naive does produce the right numbers when every change arrives once and in order. It breaks only when faults hit it.

| Concern | naive | hardened |
|---|---|---|
| Bronze load | appends every file under `dt=D` with `INSERT ... SELECT FROM s3()` | loads into a staging table and swaps the partition in with `REPLACE PARTITION`; a ledger skips files already loaded |
| Rerun and retry | appends the same rows again | idempotent: the partition is replaced by identical content |
| Order updates | gold nets change events (completed adds, refunded subtracts), trusting each event to arrive exactly once | silver keeps the latest row per `order_id` (`ReplacingMergeTree(updated_at)` read with `FINAL`, or `argMax`); gold aggregates latest states |
| Late rows at the source | dlt's default incremental cursor: a row whose `updated_at` is older than the cursor is never extracted | incremental with a lag window, then dedup by key downstream |
| Late files in landing | reads only `dt=<run date>` | finds unprocessed files in a lookback window and recomputes the business dates they touch |
| Schema drift | schema inferred from Parquet; a renamed column silently becomes NULL | explicit contract (dbt contracts and tests); mismatch alerts and fails the run |
| Partial file | loads whatever rows are there | reconciles row counts against dlt's load metadata, quarantines and alerts |
| Gold refresh | `delete+insert` incremental: two statements, a reader can see the gap | `insert_overwrite` by partition, one atomic swap |

Lessons worth stating plainly:

- **ClickHouse has no multi-statement transactions.** Atomicity comes from swapping whole partitions or tables (`REPLACE PARTITION`, `EXCHANGE TABLES`), and safety from making every step idempotent so that a replay is harmless. That is the situation of most real warehouses and lakes, which is why the hardened patterns here transfer.
- **Deduplication can be eventual.** `ReplacingMergeTree` removes duplicates when ClickHouse merges parts in the background, at a time nobody controls. A reader that forgets `FINAL` sees duplicates sometimes and not others; the platform must define one stable way for consumers to read.
- **Cursor and data live in different systems.** dlt stores its cursor with the load in the landing bucket, while bronze lives in ClickHouse. A crash after landing but before bronze leaves the cursor ahead of the warehouse: the next extract finds nothing new, and unless bronze picks up unprocessed landing files the day is silently missing. This is the classic window faultline's execution faults aim at.

**Interrupts.** Planned with the execution faults: the run carries a tag such as `faultline/failpoint=bronze:mid_write`, the asset checks it and exits hard (`os._exit`) inside the run worker, and Dagster records the run as failed. The tag is how faultline tells its own injected crash from a genuine platform failure.

## 9. Experiment spec, targets and the safety guard

**Target** (`targets/reference-naive.yaml`):

```yaml
name: reference-naive
adapter: reference
environment: test
allow_destructive: true
options:
  mode: naive
```

**Experiment** (`experiments/late-file.yaml`):

```yaml
name: late-file
description: A file for an already processed day lands two days late.
targets: [reference-naive, reference-hardened]   # --target on the CLI overrides
dataset:
  generator: coffee_vn
  profile: tiny          # tiny | demo | large
  seed: 42
  start_date: 2026-11-01
  days: 5                # any profile value can be overridden
steady_state:
  tables: [silver.orders, gold.daily_store_sales]
  invariants: [unique_keys, complete_keys]
faults:
  - type: late_file
    partition: 2026-11-02
    arrive_after: 2026-11-04
properties: [no_data_loss, no_silent_corruption, convergence]
recovery:
  max_runs: 3
```

**Safety guard.** faultline deliberately destroys data, so it must be hard to point at the wrong place. Three layers:

1. The pydantic model of a target rejects it unless it declares `environment: test` and `allow_destructive: true`. There is no CLI flag to override this.
2. The runner checks again immediately before every destructive call, so a target object built in code cannot skip validation.
3. Each adapter adds its own check on what it touches. The reference adapter only accepts a workdir under `.faultline/` or a temporary directory it created, and marks every workspace with a `.faultline-workspace` file before it deletes anything there.

The README will carry this warning prominently. The guard protects against mistakes, not against a config that says `environment: test` while pointing at production, which is why layer 3 exists for every adapter.

## 10. Dataset, CLI and scorecard

### 10.1 The coffee chain domain

faultline generates its own source data: a simulated Vietnamese coffee chain. The domain is deliberately rich, because each part of it opens a different way for a platform to fail. A toy table of orders would only ever exercise appends.

**Source systems.** A real chain does not have one source; it has several systems, each with its own format, clock and delivery habits.

| Source | Format | Clock | Delivers | Status |
|---|---|---|---|---|
| `backoffice` (ERP) | CSV | dates | one extract per dataset per day | built |
| `pos` (till in each store) | CSV | local time, `+07:00` | one extract per dataset per store per day | built |
| `delivery` (delivery app) | JSON | UTC | own order ids, status updates that lag | planned |
| `loyalty` (member app) | JSON events | UTC | append-only events with `event_id` | planned |

**Datasets built today.**

| Dataset | Delivery | Key | What changes over time |
|---|---|---|---|
| `backoffice/stores` | full daily snapshot | `store_id` | stores open and close inside the window (`status` planned, open, closed) |
| `backoffice/menu_items` | full daily snapshot | `item_id` | category price increases, a seasonal launch, a retired item (kept with `is_active=false`) |
| `backoffice/promotions` | full daily snapshot | `promo_code` | weekly campaigns with validity windows, plus one evergreen offer |
| `backoffice/members` | full on the first day, **delta** afterwards | `member_id` | new members, monthly tier upgrades |
| `pos/orders` | daily change extract per store | `order_id` | status: `completed`, `voided` (same day), `refunded` (one to three days later) |
| `pos/order_items` | with the order, on the day it is placed | `order_id, line_no` | immutable; `created_at` equals the order's `ordered_at` and is the extraction cursor |
| `pos/payments` | daily per store | `payment_id` | charges (about 10% split across two methods), negative refund rows |

Both snapshot and delta deliveries are intentional: a platform has to build SCD2 history from snapshots (compare today with yesterday) and merge deltas, the two patterns teams meet most.

**Event time versus ingestion time.** The `dt` partition of every landing file is the day the source *extracted* it, not the day the business event happened. The POS extract for `dt=D` contains every order created or changed on D, the equivalent of `WHERE updated_at` falls on D. So a refund issued on 3 November for an order placed on 1 November arrives in `dt=2026-11-03`, yet the revenue it removes belongs to 1 November. A platform that aggregates gold by landing partition gets every refund on the wrong day. Timestamps are local (`+07:00`); an order at 06:45 in Saigon is still the previous day in UTC, the other half of the same trap.

**Invariants the generator guarantees** (and its tests check):

- `subtotal_vnd = sum(qty * unit_price_vnd)` and `total_vnd = subtotal_vnd - discount_vnd`; percent discounts round down to whole thousands.
- Payments of an order sum to `total_vnd` when it is `completed` and to 0 when it is `voided` or `refunded`.
- `unit_price_vnd` is the menu price valid on the order day; promotions are active and their minimum spend is met; stores are open; members joined before the order day.
- Orders fall between 06:30 and 22:00 local time, with morning, lunch and late afternoon peaks and busier weekends.

**How the files are used.** The generator's files are *change sets*: the POS extract of `dt=D` is exactly what changed in the business on day D. On the reference stack faultline replays each day's change set into the source Postgres before running that day, so dlt's incremental extraction sees time pass the way it would in production. The same files, with their manifests, are what faultline drops straight into a landing zone when it impersonates the ingestion tool (point 2).

**Truth.** Alongside `landing/`, never inside it, the generator writes `truth/control_totals.csv`: per business date and store, the final counts per status, items sold, gross, discount and net revenue after every refund inside the window. The platform never sees it. Together with the key sets read from landing files, it is what the oracle can check outputs against.

### 10.2 Volume profiles and determinism

| Profile | Stores | Days | Orders per store per day | Members | Measured |
|---|---|---|---|---|---|
| `tiny` | 4 | 4 | 40 | 2,000 | about 420 orders, under a second; used by tests |
| `demo` | 50 | 30 | 300 | 20,000 | 456k orders, 784k lines, 147 MB, about 13 s on 16 cores |
| `large` | 200 | 365 | 350 | 200,000 | about 25M orders and 8 GB (extrapolated) |

Any profile value can be overridden. Rules that keep the output byte-identical:

- **One random stream per store-day**, seeded with `(seed, store, date)`. Stores are simulated in parallel worker processes, each store sequentially by day (a refund decided on D is delivered on D+k). The output is the same for 1 or 16 workers; a test enforces it.
- **Change events sit on a calendar anchored at `start_date`**, never derived from `days`: price increases every 30 days rotating through categories, store openings and closings at fixed offsets, monthly tier reviews, member sign-ups spread over a one-year horizon. Generating 50 days instead of 5 therefore leaves the first 5 days untouched, so cached baselines and short experiment windows stay valid.
- **Faker only produces names, phone numbers and street names.** Prices, menus, cities and volumes are hard-coded. A Faker upgrade may rename customers but never moves revenue, and verdicts do not depend on it anyway because the baseline is recomputed on every run.
- **Bytes are canonical**: UTF-8 without BOM, `\n` line endings, fixed column order, header-only files for empty extracts (an extract that found nothing must look different from a file that never arrived).

Workers receive only the spec and rebuild the dimensions from the seed. On Windows the process pool starts workers one after another, and shipping the dimensions to each one made start-up dominate the run (21 s down to 13 s on `demo`).

### 10.3 Domain slices still to build

Each slice is its own piece of work, planned after the first end-to-end demo runs.

| Slice | Adds | Failure modes it opens |
|---|---|---|
| Delivery app | orders from a delivery platform: JSON, UTC, external ids, commission, lagging status updates | merging sources, timezone skew, foreign keys across systems |
| Loyalty ledger | earn, redeem and reversal events with `event_id`; tiers computed from points | exactly-once on an append-only ledger (a retry that double-counts gives points away) |
| Shift closing | each store reports its own totals per shift | an independent reconciliation source for gold |
| Staff and shifts | employees, rosters, clock events, `cashier_id` on orders | POS schema evolution |
| Inventory and purchasing | ingredients, recipes, purchase orders, stock counts, waste | snapshot plus movement, running balances |
| Parent and child skew | order lines or payments arriving before or without their order | orphan rows, completeness across related files |

### 10.4 CLI

```
faultline generate --out DIR [--profile tiny|demo|large] [--seed N] [--start-date YYYY-MM-DD]
                   [--days N] [--stores N] [--orders-per-store-day N] [--members N] [--workers N]
faultline run <experiment.yaml> [--target NAME ...]
faultline run --all
faultline report [--format md|json]
```

`generate` refuses a non-empty output directory rather than deleting anything, and prints plain ASCII so the summary works on any console.

Results go to `.faultline/results/<run_id>/<experiment>__<target>.json`, with the snapshots needed to explain a diff.

**Scorecard.** A matrix of experiments by targets, the outcome of every property, the number of runs needed to converge, and one line of evidence per failure, for example `+200 duplicate rows on 2026-11-02, revenue +100%`. Markdown for humans and pull requests, JSON for CI gates.

## 11. Directory structure

```
faultline/
  pyproject.toml  uv.lock  Makefile  README.md  LICENSE
  .github/workflows/ci.yml
  docs/
    design.md
  src/faultline/
    cli.py  spec.py  safety.py  runner.py  trace.py  outcome.py
    adapters/      base.py  reference.py   (registry.py with the target spec)
    faults/        base.py  landing.py  execution.py
    properties/    base.py  idempotency.py  atomicity.py  ...
    oracle/        fingerprint.py  diff.py  invariants.py  snapshot.py
    datagen/       spec.py  catalog.py  dimensions.py  pos.py  writer.py  truth.py  generate.py
    report/        scorecard.py
    connectors/    dagster.py  clickhouse.py  postgres.py  workspace.py
    sources/       pos.py   (fake POS source: tables and day-by-day replay)
  reference/       the platform under test, never imported by faultline
    docker-compose.yml  .env.example   (credentials; the real .env is gitignored)
    clickhouse/    entrypoint.sh  config.d/
    dagster/       Dockerfile  pyproject.toml  requirements.lock  dagster.yaml  workspace.yaml
                   coffee_platform/   Dagster definitions, dlt and ClickHouse assets
    dbt/           dbt project: silver and gold models, ClickHouse profile
  experiments/     *.yaml
  targets/         *.yaml
  tests/           one folder per package area (datagen/, connectors/, reference/, ...);
                   tests that need the stack are marked `integration`
```

Runtime dependencies: typer, faker, duckdb (1.x, the oracle's engine), then pydantic and pyyaml as their features land. The optional extra `faultline[reference]` adds psycopg for the source database connector. Development: pytest, ruff. Nothing else unless a concrete need proves it. The reference platform has its own fully pinned dependency set in `reference/dagster/requirements.lock`, installed only in its image.

## 12. Expected results (MVP definition of done)

`make demo` runs every experiment against both reference targets and prints a scorecard close to this. If the real scorecard disagrees, either the platform or faultline has a bug, and both are worth finding.

| Experiment | naive | hardened |
|---|---|---|
| duplicate-file | SILENT_CORRUPTION | PASS |
| late-file | SILENT_CORRUPTION | PASS |
| schema-drift (column rename) | SILENT_CORRUPTION | DETECTED |
| truncated-file (line boundary) | SILENT_CORRUPTION | DETECTED |
| late-row-at-source (`updated_at` older than the dlt cursor) | SILENT_CORRUPTION (row never extracted) | PASS |
| crash between landing and bronze | SILENT_CORRUPTION (cursor ahead of the warehouse, the day never loads) | PASS |
| crash-mid-write (gold) | SILENT_CORRUPTION (readers see the gap of a two-statement refresh) | PASS |
| retry-after-partial-commit | SILENT_CORRUPTION (duplicates) | PASS |
| idempotency and backfill probes | SILENT_CORRUPTION | PASS |

At least one experiment must show `BROKEN` (noisy and unrecoverable); which one is settled when the execution faults are built on the new stack.

## 13. Design risks

1. **A self-fulfilling demo.** We write both the bugs and the tests, so faultline could end up catching only bugs designed for it. Mitigations: faults are defined against generic contracts (files, runs, keys), never against naive internals; hardened uses established patterns rather than tricks tuned to pass; the reference stack is built from real tools (dlt, Dagster, dbt, ClickHouse), and the first adapter for a team's own platform is the real check.
2. **Non-deterministic baselines.** Volatile columns, floats and row order can make two clean runs differ. Mitigations: canonicalisation, `ignore_columns`, integer money, and a self-check that runs the baseline twice in tests and expects identical fingerprints.
3. **Failpoints need platform cooperation.** Real tools do not expose them. Mitigation: interrupt points are generic, and each adapter chooses how to realise them (failpoint, cancel API, or kill on an observed event).
4. **Blurry outcome boundaries.** `DETECTED` versus `BROKEN` versus `SILENT_CORRUPTION` invites argument. Mitigations: the fixed decision table in section 6, injected crashes never count as signals, and every verdict carries evidence.
5. **Run time.** One `pos_daily` run takes about 27 s on Docker Desktop (measured; about 40 s from launch, since the run worker spends some 13 s importing code first). Most of it is fixed cost: importing dbt and its ClickHouse adapter alone takes about 10 s, while the six models execute in under 2 s. An experiment with dozens of runs therefore takes minutes. Mitigations: baseline caching, the `tiny` profile or short windows for the demo, several workspaces running in parallel on one stack. Not yet done: avoiding dbt's full re-parse on every run (changing `--vars` invalidates partial parsing, about 3 to 4 s). `make demo` target to be re-set once the first experiment runs end to end.
6. **Windows versus Linux.** Integration tests need Linux containers, which GitHub's Windows runners cannot host, so they run on ubuntu only while unit tests still run on Windows. Locally, Git Bash rewrites absolute paths passed to `docker exec` (`/usr/bin/x` becomes `C:/Program Files/Git/usr/bin/x`); set `MSYS_NO_PATHCONV=1` for such commands.
7. **Combining faults.** With several faults in one experiment, the expected state at intermediate checkpoints becomes hard to define. MVP: one fault per experiment is the norm. Final-state properties stay valid when faults are combined.
8. **An interface overfitted to one implementation.** Designing the adapter around a single platform risks baking in its quirks. Mitigation: keep the interface minimal now and revise it when the dlt adapter arrives.
9. **A misconfigured guard.** A target can claim `environment: test` and point at production. Mitigations: per-adapter checks on what they touch, and a clear README warning.
10. **A rich domain multiplies platform work.** Every dataset the generator adds is something the reference platform must ingest in both modes. Mitigations: domain slices land one at a time and only after the end-to-end demo works; experiments can run on the `tiny` profile or a short window of `demo`.
11. **A heavier, moving dependency stack.** Seven containers and about 200 pinned Python packages can break on upgrades: SQLAlchemy 2.1 broke Dagster's Postgres storage ("the connection is closed" on every launch) and had to be pinned below 2.1. Mitigations: everything pinned (images by exact tag, Python by lock file), one-command rebuild, CI runs the stack on every pull request.
12. **A young S3 implementation.** MinIO stopped publishing community images, and RustFS (1.0.x) replaced it. It serves path-style addressing only, so every S3 client must be configured for path style. If it misbehaves, SeaweedFS is the fallback; both speak S3, so only the compose file changes.
13. **Healthy is not loaded.** A Dagster code server reports healthy even when its definitions fail to import, and jobs then simply "do not exist". The integration fixtures check the code location's load status first and fail with the real import error.

## 14. After the MVP

- More source-side fakes beyond Postgres (point 1 itself is now in scope through the reference stack): a mock paginated API with duplicate pages and mid-pagination errors.
- A generic "files plus command" adapter, then adapters for Airbyte, ADF and Databricks jobs.
- Freshness SLA and alerting checks.
- Iceberg support.
- A soak mode with randomised kill timing on top of the deterministic failpoints.
