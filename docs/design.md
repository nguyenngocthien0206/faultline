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

**Every platform run is a subprocess.**
Why: a crash must be a real crash. If the platform ran in faultline's process, a "crash" would be an exception that `finally` blocks and context managers clean up politely, which is exactly the situation that never happens when a node dies.

## 4. Key interfaces

Signatures are indicative; exact types are settled when each interface is implemented.

### 4.1 Adapter

```python
class Capability(StrEnum):
    LANDING_FILES = "landing.files"  # can place arbitrary files in landing
    RUN_NEXT = "run.next"  # platform picks the next partition from its own cursor
    RUN_PARTITION = "run.partition"  # run one explicit partition
    RUN_BACKFILL = "run.backfill"  # run a date range
    INTERRUPT_FAILPOINT = "interrupt.failpoint"
    SIGNALS_ALERTS = "signals.alerts"  # exposes alerts beyond exit status


class InterruptPoint(StrEnum):
    BEFORE_WRITE = "before_write"
    MID_WRITE = "mid_write"
    AFTER_WRITE_BEFORE_STATE = "after_write_before_state"
    AFTER_STATE = "after_state"


@dataclass(frozen=True)
class Interrupt:
    stage: str  # "ingest" | "transform" | "aggregate" | "state" ... adapter-defined
    point: InterruptPoint


@dataclass(frozen=True)
class RunRequest:
    mode: Literal["next", "partition", "backfill"]
    partition: date | None = None
    start: date | None = None
    end: date | None = None
    interrupt: Interrupt | None = None


@dataclass(frozen=True)
class RunResult:
    status: Literal["SUCCEEDED", "FAILED", "INTERRUPTED"]
    attempts: int
    error: str | None
    alerts: list[Alert]


@dataclass(frozen=True)
class Capabilities:
    features: frozenset[Capability]
    interrupt_points: frozenset[tuple[str, InterruptPoint]]
    # table name -> key columns, measure columns, ignore columns
    consumer_tables: dict[str, TableContract]


class Adapter(ABC):
    def capabilities(self) -> Capabilities: ...
    def setup(self, workdir: Path) -> None: ...  # fresh, isolated environment
    def place_input(self, item: InputFile) -> None: ...  # dataset, partition, filename, bytes
    def trigger_run(self, request: RunRequest) -> RunResult: ...
    def read_output(self, table: str, dest: Path) -> Path: ...  # Parquet snapshot
    def teardown(self) -> None: ...
```

Design note, deviation from the brief: the brief lists `interrupt` as an optional method. Here an interrupt is part of the `RunRequest` and is implemented as a **failpoint**: the platform exits hard (`os._exit`) at a named point. An asynchronous `interrupt()` call would race with the run (did the write finish before the kill arrived?) and would make results flaky. Failpoints are deterministic, behave the same on Windows and Linux, and are a well known technique (FoundationDB, TiKV and etcd all use them). Black-box adapters that cannot host failpoints can still implement an interrupt point by other means, such as a cancel API or "kill when this file appears"; that is post-MVP.

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

```python
class Oracle:
    def expected(self, step: int) -> Snapshot: ...  # baseline after logical step
    def final(self) -> Snapshot: ...
    def check_invariants(self, snapshot: Snapshot) -> list[Violation]: ...
```

**Fingerprint** per table: row count, distinct key count, a multiset hash, and the sum of each measure column. The multiset hash is the sum of per-row hashes modulo 2^64. XOR is the tempting choice and the wrong one: two identical rows XOR to zero, so a fingerprint built on XOR is blind to duplicates, which are one of the main things faultline exists to catch.

**Diff** (when fingerprints differ) uses DuckDB `EXCEPT ALL` in both directions plus a group-by on keys, and reports four buckets:

| Bucket | Feeds property |
|---|---|
| missing keys | no data loss |
| extra keys / duplicated keys | exactly-once |
| same key, different values | no silent corruption |

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

A small local platform ships with faultline so the idea can be demonstrated end to end on a laptop and in CI.

**Location and isolation.** It lives in `src/faultline/reference_platform/`. A test parses its source with `ast` and fails if it imports anything from `faultline` outside its own package. The platform is a genuine black box: it cannot peek at faultline's oracle or faults, so the demo cannot cheat.

**Workspace layout.**

```
<workdir>/
  landing/                                  same layout as the generator output (section 10)
    backoffice/<dataset>/dt=YYYY-MM-DD/<dataset>.csv + <dataset>.manifest.json
    pos/<dataset>/dt=YYYY-MM-DD/store=<id>/<dataset>.csv + <dataset>.manifest.json
  warehouse.duckdb                          schemas: bronze, silver, gold, ops
  state.json                                cursor (naive mode only)
```

**Processes.** `python -m faultline.reference_platform run --mode naive|hardened ...` starts an orchestrator process that owns the retry policy. Each attempt spawns a worker process that runs the stages in order: ingest (landing to bronze), transform (bronze to silver), aggregate (silver to gold). Failpoints come from the environment variable `FAULTLINE_FAILPOINT=stage:point`, fire once (a marker file in the workspace records that they fired), and exit with code 86, which is reserved so the adapter can tell an injected crash from a real failure. Alerts are written to `ops.alerts`.

Splitting orchestrator and worker mirrors real schedulers such as Airflow or ADF: the task process dies, the scheduler survives and retries.

**Consumer-visible tables** (planned, settled when the platform is built). Silver: `orders` (key `order_id`, latest state), `order_items` (key `order_id, line_no`), `payments` (key `payment_id`), plus SCD2 history for stores, menu items and members. Gold: `daily_store_sales` (key `order_date, store_id`; measures `orders`, `items_sold`, `gross_vnd`, `discount_vnd`, `net_vnd`), keyed by the day the order was placed.

**Naive versus hardened.** Naive is not a strawman. Every one of its choices is common in real pipelines and passes a happy-path test. Order lifecycle updates (voids, refunds days later) are part of the steady state, so naive does handle updates that arrive in order: it merges by key and recomputes the business dates the changed rows belong to. It breaks only when faults hit it.

| Concern | naive | hardened |
|---|---|---|
| Writes | blind `INSERT` append | partition overwrite inside one transaction, upsert by `order_id` |
| Cursor | saved before the data write | saved in the same DuckDB transaction as the data |
| State file | rewritten in place with `open("w")` | written to a temp file, then `os.replace` (atomic rename) |
| Dedup | none | file manifest keyed by sha256, plus dedup by key |
| Late data | reads only `dt=<run date>` | scans for unprocessed files within a lookback window and recomputes affected partitions |
| Schema drift | auto-detects columns, unknown names silently become NULL | schema contract, mismatch raises an alert and fails the run |
| Truncated file | loads whatever parses | compares row count with the manifest, quarantines and alerts |
| Gold refresh | `DELETE` then batched `INSERT`, autocommit | build a staging table, swap in one transaction |
| Order updates | last row loaded wins | row with the latest `updated_at` wins, so an out-of-order replay cannot undo a refund |
| Dimension history | overwrite in place (SCD1) | SCD2 built from daily snapshots and member deltas, so a backfill uses the values valid on that day |

Two lessons worth stating plainly:

- DuckDB itself is ACID. A single SQL statement never leaves half its rows behind. The naive atomicity failures come from **multi-step jobs where each step autocommits**, which is how real pipelines usually break: the database kept its promise, the job did not.
- If state and data can be committed in one transaction, do it. When they cannot (state in Airflow, data in Snowflake), make every write idempotent so that replaying a step is harmless. Hardened shows the first; faultline's retry and rerun faults test the second.

**Cursor-driven runs.** The normal schedule uses `RunNext`: the platform reads its own cursor and processes the next partition, as dlt and ADF watermarks do. This is what makes the classic window visible: if naive saves the cursor and then crashes before writing, the orchestrator's retry asks "what is next?" and moves on to the following day. The skipped day is gone and every later run is green.

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
| `pos/order_items` | with the order, on the day it is placed | `order_id, line_no` | immutable |
| `pos/payments` | daily per store | `payment_id` | charges (about 10% split across two methods), negative refund rows |

Both snapshot and delta deliveries are intentional: a platform has to build SCD2 history from snapshots (compare today with yesterday) and merge deltas, the two patterns teams meet most.

**Event time versus ingestion time.** The `dt` partition of every landing file is the day the source *extracted* it, not the day the business event happened. The POS extract for `dt=D` contains every order created or changed on D, the equivalent of `WHERE updated_at` falls on D. So a refund issued on 3 November for an order placed on 1 November arrives in `dt=2026-11-03`, yet the revenue it removes belongs to 1 November. A platform that aggregates gold by landing partition gets every refund on the wrong day. Timestamps are local (`+07:00`); an order at 06:45 in Saigon is still the previous day in UTC, the other half of the same trap.

**Invariants the generator guarantees** (and its tests check):

- `subtotal_vnd = sum(qty * unit_price_vnd)` and `total_vnd = subtotal_vnd - discount_vnd`; percent discounts round down to whole thousands.
- Payments of an order sum to `total_vnd` when it is `completed` and to 0 when it is `voided` or `refunded`.
- `unit_price_vnd` is the menu price valid on the order day; promotions are active and their minimum spend is met; stores are open; members joined before the order day.
- Orders fall between 06:30 and 22:00 local time, with morning, lunch and late afternoon peaks and busier weekends.

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
  pyproject.toml  uv.lock  Makefile  README.md  LICENSE  CLAUDE.md
  .github/workflows/ci.yml
  docs/
    design.md
  src/faultline/
    cli.py  spec.py  safety.py  runner.py  trace.py  outcome.py
    adapters/      base.py  reference.py  registry.py
    faults/        base.py  landing.py  execution.py
    properties/    base.py  idempotency.py  atomicity.py  ...
    oracle/        fingerprint.py  snapshot.py  diff.py  truth.py
    datagen/       spec.py  catalog.py  dimensions.py  pos.py  writer.py  truth.py  generate.py
    report/        scorecard.py
    reference_platform/
                   __main__.py  orchestrator.py  stages.py  naive.py  hardened.py  failpoints.py
  experiments/     *.yaml
  targets/         *.yaml
  tests/           one folder per package area (datagen/, ...); slow tests marked `integration`
```

Runtime dependencies: typer, pydantic, duckdb, pyyaml, faker. Development: pytest, ruff. Nothing else unless a concrete need proves it.

## 12. Expected results (MVP definition of done)

`make demo` runs every experiment against both reference targets and prints a scorecard close to this. If the real scorecard disagrees, either the platform or faultline has a bug, and both are worth finding.

| Experiment | naive | hardened |
|---|---|---|
| duplicate-file | SILENT_CORRUPTION | PASS |
| late-file | SILENT_CORRUPTION | PASS |
| schema-drift (column rename) | SILENT_CORRUPTION | DETECTED |
| truncated-file (line boundary) | SILENT_CORRUPTION | DETECTED |
| crash-mid-write (gold) | SILENT_CORRUPTION (cursor moved first, retry skips the day) | PASS |
| crash-mid-write (state file) | BROKEN (state.json corrupted, every later run fails) | PASS |
| retry-after-partial-commit | SILENT_CORRUPTION (duplicates) | PASS |
| idempotency and backfill probes | SILENT_CORRUPTION | PASS |

## 13. Design risks

1. **A self-fulfilling demo.** We write both the bugs and the tests, so faultline could end up catching only bugs designed for it. Mitigations: faults are defined against generic contracts (files, runs, keys), never against naive internals; hardened uses established patterns rather than tricks tuned to pass; the first post-MVP adapter (dlt) is the real check.
2. **Non-deterministic baselines.** Volatile columns, floats and row order can make two clean runs differ. Mitigations: canonicalisation, `ignore_columns`, integer money, and a self-check that runs the baseline twice in tests and expects identical fingerprints.
3. **Failpoints need platform cooperation.** Real tools do not expose them. Mitigation: interrupt points are generic, and each adapter chooses how to realise them (failpoint, cancel API, or kill on an observed event).
4. **Blurry outcome boundaries.** `DETECTED` versus `BROKEN` versus `SILENT_CORRUPTION` invites argument. Mitigations: the fixed decision table in section 6, injected crashes never count as signals, and every verdict carries evidence.
5. **Run time.** A subprocess per run costs roughly a second, more on Windows. Mitigations: baseline caching, a small default dataset, and parallel experiments in separate workspaces. Target: `make demo` in about two minutes, measured as soon as the first experiment runs end to end.
6. **Windows versus Linux.** Windows locks open DuckDB files and has different signal semantics. Mitigations: `os._exit` failpoints, connections closed explicitly, one temp directory per experiment, CI on both ubuntu and windows.
7. **Combining faults.** With several faults in one experiment, the expected state at intermediate checkpoints becomes hard to define. MVP: one fault per experiment is the norm. Final-state properties stay valid when faults are combined.
8. **An interface overfitted to one implementation.** Designing the adapter around a single platform risks baking in its quirks. Mitigation: keep the interface minimal now and revise it when the dlt adapter arrives.
9. **A misconfigured guard.** A target can claim `environment: test` and point at production. Mitigations: per-adapter checks on what they touch, and a clear README warning.
10. **A rich domain multiplies platform work.** Every dataset the generator adds is something the reference platform must ingest in both modes. Mitigations: domain slices land one at a time and only after the end-to-end demo works; experiments can run on the `tiny` profile or a short window of `demo`.

## 14. After the MVP

- Source-side faults (point 1) with fake sources: a test Postgres, a mock paginated API.
- Adapters for dlt, Airbyte, ADF, dbt and Dagster.
- Freshness SLA and alerting checks.
- Iceberg support.
- A soak mode with randomised kill timing on top of the deterministic failpoints.
