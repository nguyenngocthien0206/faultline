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
    # Several sandboxes can run at the same time without touching each other:
    # the runner then runs the baseline and the fault phase side by side.
    CONCURRENT_SANDBOXES = "sandbox.concurrent"
    # List, read and write the files ingestion leaves for the warehouse.
    LANDING_FILES = "landing.files"
    # Run one half of a partition: ingest (source to landing) or load (landing to tables).
    RUN_STAGES = "run.stages"


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
class Stage(StrEnum):
    INGEST = "ingest"
    LOAD = "load"


@dataclass(frozen=True)
class RunRequest:
    partition: date
    stage: Stage | None = None  # None: both halves, as every normal day


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

    # Optional, with LANDING_FILES. A file is addressed by dataset (as named in
    # the generated data, "pos/order_items"), partition and bare file name.
    def landing_files(self, dataset: str, partition: date) -> list[str]: ...
    def read_landing_file(self, dataset: str, partition: date, file: str) -> bytes: ...
    def write_landing_file(self, dataset: str, partition: date, file: str, data: bytes) -> None: ...
    def delete_landing_file(self, dataset: str, partition: date, file: str) -> None: ...
```

**`deliver(day)` instead of `place_input(file)`.** The steady-state input is whatever "one more day" means for the platform: the reference stack replays the day's changes into its source Postgres, a file-based platform would drop the day's files into its landing zone. faultline does not need to know which. Faults that reach deeper (dropping an extra file into S3, rewriting a source row, killing a run) use optional hooks declared as capabilities, added together with the faults that need them: `landing.files` (built, with `duplicate_file`), `run.stages`, `interrupt.failpoint` (built, with `crash_after_landing`), `run.retry` (built, with `retry_after_partial_commit`: the orchestrator may retry this one run, and the adapter reports it once its last attempt ended), `run.backfill` (built, with `backfill_equivalence`: one request for a range of partitions, run as the orchestrator runs backfills, one result per partition), then `source.mutate`.

**Stages: a gap between landing and loading.** Most landing faults act on a file after ingestion wrote it and before the warehouse read it: a file held back, cut short or with a renamed column. A platform that lands and loads in one run leaves no such gap, so an adapter can declare `run.stages` and run a partition's two halves on request: `ingest` (source to landing files) and `load` (landing files to the consumer tables). That is how many platforms already run (an ingestion tool, then the transformations), and a fault rewrites one day's `Run(D)` into `Run(D, ingest)`, its own actions, `Run(D, load)`. Every other day still runs whole. The reference stack has one job per edition and stage (`pos_ingest`, `pos_load`, and their hardened twins) next to `pos_daily`.

**Landing files without the layout.** A landing fault names a dataset, a partition and a file name, never a path. Where the file lives (`<workspace>/pos/order_items/dt=2026-11-02/` in the reference stack's bucket) is the adapter's business, so the same fault works on a platform that lands files anywhere else. The reference adapter reaches the bucket with boto3 (in the `reference` extra), refuses file names with a separator or a dot path and datasets it does not declare, so a fault cannot reach outside its sandbox, and empties the workspace's prefix on teardown.

**Leftover sandboxes.** A sandbox removes itself when its experiment ends, in all three stores. What a crash, an interrupted test or an older faultline left behind is swept up by `faultline clean` (`make ref-clean`): it lists every `fl_*` prefix in the landing bucket, schema in the source and database in the warehouse, and drops those that carry faultline's sandbox mark through the same workspace guard (an fl_* name without the mark is reported and left alone, section 9). It refuses while a Dagster run is in progress or one changed in the last ten minutes, since between two runs a live sandbox has nothing running (it is delivering a day or taking a snapshot) and would look like a leftover; `--force` overrides when the person running it knows better.

The reference adapter checks that the Dagster code location loaded before setting up anything, creates the workspace (Postgres schema and source tables), replays days with `faultline.sources.pos`, launches `pos_daily` through GraphQL, and snapshots tables with ClickHouse's `FORMAT Parquet` streamed to disk (written to a temporary name and renamed, so a failed export never looks like a valid snapshot). It only exports tables it declares.

**A slow orchestrator is not a failed platform.** With a dozen sandboxes running at once, Dagster's webserver sometimes takes longer than the client's 30 s to answer. The first version of the client turned that into an exception: the experiment ended as `ERROR`, and worse, its teardown dropped the sandbox under a run that was still going, which then failed on tables that no longer existed. The client now tells the two apart. Reading a run's status is safe to repeat, so an unanswered poll is retried until the run's own deadline. A launch is not (an unanswered launch may still have started the run), so every launch carries a unique `faultline/launch-id` tag, and a launch that got no answer looks its run up by that tag before launching again. Any adapter that drives an orchestrator over HTTP needs the same care.

Design note, deviation from the brief: the brief lists `interrupt` as an optional method. Here an interrupt will be part of the `RunRequest` and implemented as a **failpoint**: the platform exits hard (`os._exit`) at a named point. An asynchronous `interrupt()` call would race with the run (did the write finish before the kill arrived?) and would make results flaky. Failpoints are deterministic, behave the same on Windows and Linux, and are a well known technique (FoundationDB, TiKV and etcd all use them). Black-box adapters that cannot host failpoints can still implement an interrupt point by other means, such as a cancel API or "kill when this file appears".

A capability the adapter does not declare turns the dependent fault or property into `SKIPPED`, never into a failure. The runner checks before anything is generated or run (faults declare what they need in `requires`), so a skipped experiment costs nothing and says which capability was missing.

### 4.2 Fault

Implemented in `faultline.faults` (`base.py`, `source.py`):

```python
class InjectionPoint(StrEnum):
    SOURCE = "source"
    LANDING = "landing"
    EXECUTION = "execution"


class Fault(ABC):
    name: ClassVar[str]
    injection_point: ClassVar[InjectionPoint]
    requires: ClassVar[frozenset[Capability]] = frozenset()

    def prepare_input(self, input_root: Path) -> None: ...  # rewrite the faulted copy of the input
    def apply(self, schedule: Schedule) -> Schedule: ...  # default: unchanged
    def heal(self) -> list[Action]: ...  # default: none
    def affected_partitions(self) -> set[date]: ...  # what an operator would rerun
    def describe(self) -> str: ...  # one line for reports
```

A fault acts only on what faultline controls: its private copy of the generated input, the schedule of `Deliver(day)` and `Run(partition)` actions (`faultline.schedule`), and, later, optional adapter capabilities. It never reaches into the platform, so any adapter that offers what the fault `requires` can be tested with it.

**Built so far: `PosOfflineSync(offline_from, sync_on, store=None)`** (source, point 1). A store's till is offline for some days; everything it recorded reaches the source database on `sync_on`, with its real, older timestamps. faultline moves that store's `pos/{orders,order_items,payments}` extracts for the offline days into the sync day's input; the adapter replays them like any other day. With no store given, the first store (by id) open on every day involved is chosen, deterministically. Replay into the source keeps the latest version per key and never moves a row back in time, as a real till sync would.

**Built: `DuplicateFile(original_day, redelivered_on, datasets=all POS)`** (landing, point 2, requires `landing.files`). A day's extract lands a second time, under a later partition and new names (`redelivered-<name>`), as a retried upload or a supplier resending yesterday's files would. It adds `CopyLandingFiles` actions right before the run of `redelivered_on`; the runner copies byte for byte through the adapter, and an empty source partition is an error rather than a silent no-op (a fault that injected nothing would score the platform on nothing). Recovery reruns `redelivered_on`.

**Built: `LateFile(partition, lands_before, datasets=("pos/order_items",))`** (landing, point 2, requires `landing.files` and `run.stages`). One file of a day's extract arrives after that day was loaded, under its own partition. The day runs as ingest, `HoldLandingFiles` (read, then delete), load; `ReleaseLandingFiles` puts the file back, byte for byte and under its own name, right before the run of `lands_before`. Held files live in the runner for one phase of one sandbox, and releasing what was never held is an error. Recovery reruns the late file's day.

**Built: `TruncatedFile(partition, keep=0.8, datasets=("pos/order_items",))`** (landing, point 2, requires `landing.files` and `run.stages`). A file loses its last rows, cut on a row boundary: the day runs as ingest, `TruncateLandingFiles`, load, and the heal (`ReleaseLandingFiles`) redelivers the full file before recovery reruns the day. The runner keeps the original for that, in one store of held files shared by a sandbox's fault and recovery phases. The cut is made with pyarrow (in the `reference` extra), the library dlt writes with, because the schema must survive exactly: rewriting with DuckDB keeps the values but turns every REQUIRED column into OPTIONAL, which a strict platform would read as schema drift. A fault changes one thing; this one changes the row count and nothing else.

**Built: `ColumnDrift(partition, dataset="pos/payments", old="amount_vnd", new="total_amount_vnd", move_to_end=False)`** (landing, point 2, requires `landing.files` and `run.stages`). A file arrives with a column renamed, values and types untouched (pyarrow again), in place or moved to the end. The day runs as ingest, `RenameLandingColumn`, load; the heal redelivers the file with the agreed schema. Measured before building it: ClickHouse reads a file through the target table's structure (`use_structure_from_insertion_table_in_table_functions`), matching columns by name, so moving the column changes nothing on this stack and one experiment covers both shapes; the toy platform still tests the moved variant, for adapters that load by position.

**Built: `CrashAfterLanding(partition)`** (execution, point 3, requires `interrupt.failpoint`). The day's run dies, hard, at the failpoint `after_landing`: ingestion has landed its files and saved its cursor, nothing has been loaded. It rewrites that day's `Run(D)` into `Snapshot("before_crash")`, `Run(D, failpoint="after_landing")`, `Snapshot("after_crash")`, so `atomicity` can see what readers saw meanwhile; every other run is untouched, and recovery reruns the day. The crash is faultline's, so it is `INTERRUPTED`, never a signal.

**Built: `CrashMidWrite(partition)`** (execution, point 3, requires `interrupt.failpoint`). The day's run dies inside the gold refresh, at the failpoint `gold_mid_write`, between the statements of dbt's incremental strategy. It rewrites `Run(D)` into `Snapshot("before_crash")`, `Run(D, failpoint="gold_mid_write")`, `Snapshot("after_crash")`, so `no_vanishing_rows` and `atomicity` can compare what a reader saw just before and just after. Recovery reruns the day. The built-in experiment crashes day 3, not day 2: a refresh only rewrites the keys its new data touches, and on the `tiny` dataset day 3 is the first extract that touches an older day (refunds of day 1 orders). A crash on a day that only touches itself takes out rows no reader has seen yet, and nothing visible vanishes.

**Built: `RetryAfterPartialCommit(partition)`** (execution, point 3, requires `interrupt.failpoint` and `run.retry`). The day's run dies at `after_bronze_write`, right after bronze wrote the day's order lines and before Dagster recorded the step as done, and asks for one retry: `Run(D, failpoint="after_bronze_write", retries=1)`. Dagster's daemon re-executes the run from the failed step, so the write runs twice: at-least-once delivery, the most common way a pipeline counts something twice. The retry goes green and the crash is faultline's, so the day's run counts as green. A failure of the platform's own stays a signal even when a retry then goes green, since a failure alert would already have fired on it.

MVP faults:

| Fault | Point | What it does | Heal |
|---|---|---|---|
| `duplicate_file` | landing | Delivers a file twice, under the same name (overwrite) or a new name (ingestion retry) | none |
| `late_file` | landing | Delivers a file into partition D after D has been processed | none |
| `column_rename` | landing | Delivers a file whose column is renamed (`amount_vnd` -> `total_amount_vnd`) | redeliver with the agreed schema |
| `truncated_file` | landing | Cuts a file, by default on a line boundary so it still parses | redeliver the full file |
| `crash_mid_write` | execution | Interrupts the gold refresh between its statements (`gold_mid_write`) | none |
| `retry_after_partial_commit` | execution | Interrupts right after a write, before the orchestrator records it (`after_bronze_write`), then lets the orchestrator retry | none |

A line-boundary truncation is the default on purpose. A file cut mid-line usually fails to parse and gets noticed. A file that is short but well formed is the dangerous case, and catching it needs a delivery contract (a manifest with a row count or checksum), which is exactly what the hardened platform demonstrates.

### 4.3 Property

Implemented in `faultline.properties`:

```python
@dataclass(frozen=True)
class Checkpoint:
    name: str  # "F" (end of the fault phase) or "R" (after recovery)
    snapshots: dict[str, Path]
    runs: tuple[tuple[date, RunResult], ...]
    marks: dict[str, dict[str, Path]]  # snapshots taken on the way, by name; R adds F's as "F"
    recovery: Recovery | None  # R only: the rounds recovery took (see section 5)
    fault_runs: tuple[tuple[date, RunResult], ...]  # the fault phase's runs, at F and R alike


@dataclass(frozen=True)
class PropertyContext:
    contracts: tuple[TableContract, ...]
    baseline: dict[str, Path]  # the clean run: the differential answer key
    truth_root: Path  # generated data before any fault
    baseline_days: dict[date, dict[str, Path]]  # the clean run after each day


@dataclass(frozen=True)
class PropertyResult:
    name: str
    checkpoint: str
    holds: bool
    evidence: tuple[str, ...]


class Property(ABC):
    name: ClassVar[str]

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult: ...

    def probe(self) -> Schedule:  # actions of its own; most properties add none
        return ()
```

A property only reads snapshots; it never drives the platform. The runner checks every property at F and at R and the classifier (section 6) turns the pair into one outcome.

**Probes (built).** A property that needs the platform to do something before it can judge (rerun a day, backfill a range) declares those actions as its probe. The runner runs every property's probe right after the fault phase's schedule, in the same sandbox and before checkpoint F, logged as `[probe]`; the baseline never runs one. A probe's runs count as the fault phase's, so a probe run that fails on its own is a signal. Checkpoint R carries the fault phase's snapshots too, so a property built on a probe judges R as it judged F: in an experiment without a fault nothing is rerun in recovery, and the state after the probe is the state readers keep. That keeps the decision table of section 6 as it is: a probe that breaks the promise silently is `SILENT_CORRUPTION`, one that breaks it loudly is `BROKEN` (noticed, never healed).

A probe can need a capability (`Property.requires`, like a fault's): without it the experiment is `SKIPPED` before anything runs. And an experiment can start from another schedule than the steady state (`Experiment.schedule`): `backfill` starts from `deliveries_only`, every day in the source and nothing run, so its probe's backfill is the first load. The baseline always runs the steady state, the right answer.

Most properties judge where a phase ends. Some judge what happened on the way, and for those a fault puts `Snapshot(name)` actions into its schedule: the runner snapshots every consumer table there, as a reader would see it at that moment, and hands them to the checkpoint as `marks`. `no_vanishing_rows` compares the snapshots right before and right after a crash at F, and the end of the fault phase with the end of recovery at R. It needs no baseline: a refresh may change values and add keys, but a key a reader could see should never disappear while the platform works on it. A snapshot takes a second or two, which is why only faults that need one ask for it.

### 4.4 Oracle

The oracle is **hybrid**:

1. **Differential baseline.** faultline runs the clean schedule on a fresh environment and snapshots the consumer tables after every logical step. The faulted run must match those snapshots at the relevant checkpoints. faultline never needs to know how revenue is computed, so this works for any platform.
2. **Key invariants from the generator.** Because faultline generated the source data, it knows the set of keys that must exist per table (for example every `order_id`). It checks that each key appears exactly once and that none is missing. This also validates the baseline itself: a platform that is wrong even on a clean run would otherwise produce a wrong answer key.

Implemented in `faultline.oracle`, all in an embedded DuckDB over Parquet snapshots, whatever warehouse produced them:

```python
snapshot_all(adapter, dest_dir) -> dict[str, Path]  # every declared table to Parquet
fingerprint(path, contract) -> Fingerprint  # rows, distinct keys, row hash, measure sums
diff(expected, actual, contract) -> Diff  # bucket counts, rows off, sample keys, measure deltas, summary()
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

Next to the buckets the diff counts the **rows off**: for every key that differs, its rows when it is missing or extra, the difference in rows when the counts differ, its rows when only the content changed. A key is in one bucket however badly it is off; the rows say how badly. Recovery reports both after every round (section 5).

Totals alone are not enough. A unit test builds a snapshot with one key missing, one duplicated, one changed and one extra, and the revenue total comes out exactly equal to the expected one; only the keyed buckets reveal the damage. On the reference stack, rerunning one day of the naive pipeline yields: `silver_orders` identical, `silver_order_items` 133 duplicated keys, `gold_daily_store_sales` 4 changed keys with net revenue +19.3% and items sold +56.4% (duplicates on both sides of a join multiply instead of adding up).

**Canonicalisation** before hashing or diffing: columns ordered by name, timestamps normalised to UTC, declared `ignore_columns` dropped (load timestamps, run ids), money stored as integer VND. Floats are avoided in the reference data because `0.1 + 0.2` summed in a different order is a different number, and the diff would report noise as corruption.

## 5. Experiment lifecycle

Implemented in `faultline.runner.run_experiment(experiment, adapter_factory, work_dir)`:

```
1. generate     dataset from its seed -> the truth (work_dir/generated);
                  the faults rewrite a copy of it and the schedule
2. baseline     sandbox A: clean schedule, a snapshot after every day, key invariants
                  a failed run or a broken invariant -> ERROR "steady state not established"
3. fault phase  sandbox B (fresh): the faulted schedule on the faulted copy    -> checkpoint F
4. recovery     heal actions, then rerun the affected partitions, round after round until the
                  outputs match the baseline (at most recovery_rounds rounds) -> checkpoint R
5. score        every property at F and R -> classifier -> one verdict per property and for the experiment
```

**Side by side.** The two sandboxes only meet at step 5, so when the adapter declares `CONCURRENT_SANDBOXES`, step 2 runs in one thread and steps 3 and 4 in another, and an experiment takes as long as its longer half: 7 Dagster runs of wall clock instead of 11 for `pos-offline-sync` on `tiny`. The price is ordering: a baseline that turns out broken is only known once the fault phase has run as well. The verdict is still `ERROR` and nothing is scored against that baseline, but the fault phase's runs were spent, because faultline never kills a platform's runs midway to save time. An adapter for a single shared test environment leaves the capability out; the runner then goes one step after the other and stops at a broken baseline before injecting anything. Whichever half fails, the runner waits for the other to finish, so every sandbox is torn down.

Steps 1 and 2 are `run_baseline`, whose result can be passed to several experiments (`run_experiment(..., baseline=...)`) on the same dataset and target, so the clean run is paid for once; a baseline from another dataset spec is refused. Property probes run between steps 3 and F (section 4.3); the first plan put them after step 4, but a probe judged on top of a fault's leftovers says little, so probes live in experiments of their own, without a fault, where the baseline is the right answer.

**Convergence (built).** Recovery is the operator's move after an incident: rerun what the fault touched, and if the numbers still look wrong, rerun again. So recovery goes in rounds. A round reruns every affected partition once; after it the runner snapshots every consumer table into `recovery_round_<n>/` and compares it with the baseline, the same full diff `no_silent_corruption` makes. It stops at the first round that matches, and after `recovery_rounds` rounds at most (default 3); there is always at least one, so R keeps the meaning it had when recovery was one fixed round. R is the state after the last round (copied to `checkpoint_R/`), and the checkpoint carries the rounds: how many runs each took, how many keys and how many rows still differed after it, and in which tables. Rows as well as keys, because a key duplicated once or five times is one duplicated key: a rerun that appends the same day again leaves the key count where it was, and only the rows show the drift. The `convergence` property reads them and holds when the outputs matched. Side by side, the first round may finish before the baseline has: it waits for it. A baseline that turns out broken stops recovery after one round, since there is nothing worth converging to and the verdict is `ERROR` anyway. The count is what the property adds to `no_silent_corruption` at R: one round is a platform the standard move heals, two or three one that needs patience, `>3` one that no amount of rerunning fixes, with the rows off after each round showing whether it held still or got worse (`growing`, `shrinking`).

**The matrix (built).** `run_all(experiments, targets, work_dir, parallel=8)` runs every experiment on every target. The clean run is paid once per dataset and target: the baselines run first, side by side, and every experiment on that dataset reuses its target's (`baseline=`). Then the pairs run, `parallel` at a time; Dagster's run queue decides how many of their runs execute at once. A pair that fails is an `ERROR` result and the others go on; a baseline that could not run at all makes each of its pairs an `ERROR` that says so. On the ten experiments and two editions that is 118 Dagster runs instead of 212 (136 since recovery goes in rounds, section 5). The integration suite does the same across its pytest-xdist workers: the first worker that needs an edition's clean run runs it under a file lock in the session's shared temporary folder, every other worker reads it (`tests/reference/shared_baseline.py`), and every experiment test passes it as `baseline=`. One test, crash-after-landing on naive, still runs whole, so the side-by-side path a single `faultline run` takes keeps being tested on the real stack. Every result, from `run_all` or a single `faultline run`, is written as JSON (`faultline.report.results`).

Notes:

- Never inject faults into a system that is not healthy. That is the first rule of chaos engineering, and step 2 enforces it (side by side, by refusing to score rather than by not starting).
- Recovery reruns the affected partitions because that is the standard operator move after an incident.
- Each phase gets its own sandbox (workspace), so experiments can run in parallel and never contaminate each other. Sandboxes are always torn down; snapshots stay in `work_dir` for inspection.
- Any exception in the harness ends the experiment as `ERROR` with the message, never as a verdict about the platform.
- The whole machinery is tested without Docker on a toy platform written in Python (`tests/runner/fake_platform.py`) with a dlt-like cursor: the same `PosOfflineSync` fault yields `SILENT_CORRUPTION`, `PASS`, `DETECTED` or `BROKEN` depending only on how the toy platform treats rows behind its cursor.

## 6. Outcome classification

A **signal** is an error or alert raised by the platform itself: a run that fails with its own error, or an alert it records. A run that died because faultline killed it is `INTERRUPTED` and is **not** a signal. The process dying says nothing about whether the platform noticed the data problem. On crash-after-landing that rule decides the verdict: the crashed run is red in Dagster, yet naive is `SILENT_CORRUPTION`, because the red says a process died, not that a day is missing, and the days after it run green over the gap.

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
- Implemented in `faultline.outcome.classify`. A violation at R counts as `BROKEN` when a signal was raised in either phase: the platform did raise its hand, it just did not heal.
- `convergence` is judged at R only (at F it holds by definition), so its outcome is `PASS`, `BROKEN` or `SILENT_CORRUPTION`, never `DETECTED`.
- `atomicity` judges one moment, right after the run faultline interrupted, at F and again at R (both carry the fault phase's runs and snapshots), so its outcome follows F: a crash is no signal, so a violation is `SILENT_CORRUPTION`.

## 7. Properties

| Property | Checkpoint | Violated when | Probe |
|---|---|---|---|
| `idempotency` | F, R (after its probe) | outputs change when an already processed day is rerun (built) | snapshot, rerun an old day, snapshot |
| `atomicity` | F, R (the moment right after the interrupted run) | right after a crash a table is half written, or the tables disagree: some show the crashed day, others do not (built) | none (needs a crash fault with `Snapshot` actions around it) |
| `exactly_once` | F, R | duplicated keys, or aggregates inflated after a retry | none |
| `no_data_loss` | F, R | keys missing compared with the generator truth or baseline | none |
| `backfill_equivalence` | F, R (after its probe) | loading a range of days in one backfill gives a different result from running them day by day (built) | one orchestrator backfill of the range |
| `convergence` | R | outputs do not match the baseline within `recovery_rounds` rounds of recovery; the rounds taken are reported (built) | none (recovery itself, section 5) |
| `no_silent_corruption` | F, R | the full snapshot differs from the baseline, including changed values such as revenue turned NULL | none |
| `no_vanishing_rows` | F, R | a key readers could see is gone: at F between the snapshots around a crash, at R between the end of the fault phase and the end of recovery (built) | none (needs `Snapshot` actions in the fault's schedule) |

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

**The POS pipeline.** Two editions of the same pipeline live side by side in one code location: `pos_daily` (naive, described here) and `pos_daily_hardened` (below). They write the same tables, so one table contract and one set of experiments serve both, and an adapter picks the edition by the job it launches. Each edition also has an ingest job (the landing asset alone) and a load job (bronze and dbt), for faults that act in between (see stages in section 4.1). The naive job is partitioned by day; each run executes in a single process (`in_process_executor`, which saves about 15 s of re-imports per step):

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

| Concern | naive | hardened | built |
|---|---|---|---|
| Late rows at the source | dlt's default incremental cursor: a row whose `updated_at` is older than the cursor is never extracted | dlt `incremental(..., lag=...)`: every run reads again the last `lag_days` (op config, default 3) | yes |
| Bronze load | appends every file under `dt=D` with `INSERT ... SELECT FROM s3()` | rebuilds day `D` in a staging table from all its landing files, then swaps it in with `REPLACE PARTITION` | yes |
| Rerun and retry | appends the same rows again (measured: a retry after a crash writes the day's order lines twice) | idempotent: the partition is replaced, and silver collapses copies by key | yes |
| Order updates | gold nets change events (completed adds, refunded subtracts), trusting each event to arrive exactly once | silver keeps the latest row per key (`argMax`); gold aggregates final states (an order counts if its latest status is `completed`) | yes |
| Gold refresh | `delete+insert` incremental: two statements, a reader can see the gap, and a crash between them leaves it open (measured: the older days the run touched vanish from gold) | `insert_overwrite` partitioned by business day: the new rows are built in a side table, then one `ALTER TABLE ... REPLACE PARTITION` statement swaps in all touched days; a crash before it leaves the old rows in place | yes |
| Publishing a day across tables | each model is visible as soon as dbt writes it | the same: each table swaps in on its own, so a crash between two models leaves them disagreeing (measured by `atomicity`); a publish step (a watermark consumers read through) would fix it | no, measured |
| Duplicate files in landing | loaded again: lines, payments and gold counted twice | the copies are stray files no manifest lists: the day is refused, loudly, and the next run's lag window plus keyed silver keep the numbers right | yes |
| Late files in landing | reads only `dt=<run date>`, and dlt's cursor has moved on: the rows never arrive | the manifest lists the file, so the day's load fails on the missing file; the lag window reads the rows again from the source on the next run, so no look back over landing files is needed while the source can be read again | yes |
| Schema drift | ClickHouse matches columns by name and fills the missing `amount_vnd` with zeros (`input_format_parquet_allow_missing_columns`), dropping the renamed one: green runs, payments worth nothing | a schema contract checked before every load: a missing, unexpected or retyped column refuses the day | yes |
| Backfill side by side | runs a backfill's days as Dagster submits them, side by side: their extracts share dlt's cursor and working files, so runs fail on files another moved and early ones each load the whole history (measured: once, twice or three times) | each extract holds a Postgres advisory lock named after the workspace, on the source it reads: extracts of one pipeline take turns, loads and dbt still run side by side | yes |
| Partial file | loads whatever rows are there | reconciles every load against the extract's manifest (rows promised, files written) and refuses the day on a mismatch, a missing file or a stray one | yes |

A **look back over landing files** (reload the last few partitions, or every partition whose files changed) is the textbook answer to late files, and it is not built either: on the late-file experiment hardened passes because the lag window reads the same rows again from the source database. That is a property of this source, not a general cure. A source that only drops files (a delivery partner's exports, say) cannot be read again, and there a late file stays lost unless bronze looks back; it will come with the delivery app's files.

A **ledger of loaded files** (skip a file whose name or checksum was loaded before) is the classic answer to duplicate files, and it is deliberately not built: the duplicate-file experiment passes without it, because every POS table has a key and silver keeps one row per key. A ledger becomes necessary for data without a natural key, an append-only ledger of loyalty points or stock movements, where a copied row is indistinguishable from a real second movement. It will come with a dataset like that.

Each hardening ships together with the fault that tests it. A safeguard no experiment exercises proves nothing, and is exactly the kind of code that drifts into being tuned to pass (risk 1 in section 13).

**The hardened pipeline** (`coffee_platform/pos_hardened.py`, dbt project `reference/dbt_hardened/`). Same tools and table names; Dagster asset keys carry the prefix `hardened`. The dbt project reuses the naive project's workspace guard macro. Internal `stg_*` views keep every copy of every row with `extracted_on`; gold uses them to find the business days a run touched (orders and order lines that came in, wherever their day lies). The consumer silver tables carry business columns only: when the platform happened to read a row is lineage, not a fact about the sale, and it legitimately differs between a clean run and one where the row arrived late.

The three main safeguards only work together. The lag window creates duplicates on purpose, since every run reads the last few days again; idempotent bronze and keyed silver are what make those copies harmless. A lag window on top of naive's append-only bronze and event-netting gold would turn a loss into double counting.

**The load manifest, a delivery note between ingest and load.** Every hardened extract writes, next to its files, one line per table into `pos/_manifests/dt=<partition>/<load_id>.jsonl`: the load id, the rows dlt extracted, and the files that load wrote (dlt names them `<load_id>.<file_id>.parquet`). Bronze reconciles a day's files against those notes before it swaps anything in, and refuses the day on any of three things: a stray file no manifest lists, a listed file that is missing, or a load whose files hold another number of rows than it promised. The run fails with the problems spelled out, and the day stays as it was until the files are right. The manifest is written and read through ClickHouse's `s3()` with the `landing_s3` named collection, so no credential reaches the code, and it sits outside the datasets the adapter exposes, so a fault can cut a file but never edit its delivery note. That separation is the point: a count only proves something if it comes from the side that sent the data.

**The schema contract, the consumer's side of the deal.** The manifest is written by the producer, who may well mean a new schema, so it cannot guard the schema. Hardened bronze also holds every file against `CONTRACTS`, the columns and ClickHouse types the platform expects for each table (dlt's `_dlt_*` columns and the `dt` partition column taken from the path are left out), and refuses the day on a missing column, an unexpected one, or one of another type, naming the file. The check is cheap (a `DESCRIBE` per file) and it is the only thing between a renamed column and a day of zeros, because nothing else complains: the file parses, the row count matches the manifest, every key is unique.

The manifest changes what hardened does with the earlier landing faults, and the verdicts stay `PASS`: the resent copies of duplicate-file are stray files, and the held back file of late-file is a missing one, so those days fail, loudly, and the next day's lag window reads the rows again from the source. A red run with the right numbers is the behaviour wanted: noticed, and harmless.

**Lateness is a promise with a limit.** The lag window is how late a row may reach the source and still be counted. It is a business decision as much as a technical one: a wider window tolerates longer outages but re-reads more data on every run (with `lag_days = 3`, each run extracts about four days of changes instead of one). A row later than the window is lost on hardened too, silently, exactly as on naive. faultline makes that limit measurable: `faultline run pos-offline-sync --target reference-hardened-lag1` shrinks the window below the two-day outage, and the verdict should turn into `SILENT_CORRUPTION`. The runner's unit tests show the same on a toy platform.

Lessons worth stating plainly:

- **ClickHouse has no multi-statement transactions.** Atomicity comes from swapping whole partitions or tables (`REPLACE PARTITION`, `EXCHANGE TABLES`), and safety from making every step idempotent so that a replay is harmless. That is the situation of most real warehouses and lakes, which is why the hardened patterns here transfer. It buys atomicity per table only: dbt builds one model after another, and silver is views over bronze, so a run that dies between two models leaves tables that are each whole and together contradict each other (measured on crash-mid-write: the payments report shows day 3, the sales report does not). Keeping the tables consistent with each other takes publishing them together on purpose, for instance a watermark of the last complete day that every consumer table or view reads through, moved once the whole run is done. Hardened does not do it, and the scorecard says so.
- **Deduplication can be eventual.** `ReplacingMergeTree` removes duplicates when ClickHouse merges parts in the background, at a time nobody controls. A reader that forgets `FINAL` sees duplicates sometimes and not others; the platform must define one stable way for consumers to read.
- **Cursor and data live in different systems.** dlt stores its cursor with the load in the landing bucket, while bronze lives in ClickHouse. A crash after landing but before bronze leaves the cursor ahead of the warehouse: the next extract finds nothing new, and unless bronze picks up unprocessed landing files the day is silently missing. This is the classic window faultline's execution faults aim at.

**Interrupts (built).** A run launched with the tag `faultline/failpoint=<name>` dies at the failpoint of that name (`coffee_platform/failpoints.py`): it logs `faultline failpoint reached: <name>`, then calls `os._exit`, so no exception handler, `finally` block or connection close runs, as with an out-of-memory kill. Dagster's code server sees its run process vanish and marks the run failed within seconds (measured: about two; no run monitoring needed with `DefaultRunLauncher`). The adapter then reads the event log: a failed run that logged its failpoint is `INTERRUPTED`, faultline's own crash; a failed run without that line failed by itself, before the failpoint, and stays `FAILED`, so a genuine failure is never hidden behind an injected one. Three failpoints exist. `after_landing` sits at the end of both editions' landing assets (after the manifest, on hardened). `after_bronze_write` sits at the end of the order lines' bronze asset, after the INSERT on naive and after the `REPLACE PARTITION` on hardened. `gold_mid_write` sits inside dbt, in copies of dbt-clickhouse's own `delete_insert` and `insert_overwrite` strategy macros (`reference/dbt/macros/failpoint_mid_write.sql`; a root project macro overrides the adapter's): after the DELETE and before the INSERT for naive, after the side table is built and before the `REPLACE PARTITION` for hardened, armed for `gold_daily_store_sales` only. dbt runs in a subprocess, so the macro cannot kill the run: it raises with the same marker, passed in as a dbt var, and the Dagster asset that sees the marker in dbt's error dies at the failpoint of that name. Copying a library's macro is a maintenance cost, so a unit test fails as soon as the lock file pins another dbt-clickhouse than the one copied.

**Retries (built).** The Dagster instance enables run retries with a default of zero (`run_retries` in `dagster.yaml`), so a run is only retried when it asks: faultline tags one run with `dagster/max_retries` and `dagster/retry_strategy=FROM_FAILURE`, and every other experiment still sees a crash stay a crash. Measured: the daemon launches the retry 4 to 12 seconds after the failure, tagged with its parent (`dagster/parent_run_id`), and re-executes the step that did not finish plus the steps that never started; landing, which had finished, is not run again. A retry inherits every tag, the failpoint among them, so a failpoint never fires in a re-execution (a run with a parent); otherwise the retry would die at the same place, every time. The adapter waits for each retry it asked for (up to two minutes for one to appear, then an error, never a guess) and reports the run once: the last attempt's status, `FAILED` if any attempt failed on its own, with every attempt's status in the result's metadata.

## 9. Experiment spec, targets and the safety guard

Experiments and targets are YAML files at the root of the repository (`experiments/`, `targets/`), read by `faultline.spec`. They are data, so a team can review a new experiment in a pull request, and the test suite loads the same files the CLI runs: there is no second copy in Python to drift from them.

**Target** (`targets/reference-hardened-lag1.yaml`):

```yaml
name: reference-hardened-lag1
adapter: reference
environment: test          # required
allow_destructive: true    # required
options:
  edition: hardened        # naive | hardened
  lag_days: 1              # hardened only; leave it out for the platform's own
  env_file: ../reference/.env   # relative to this file
```

Three targets ship with the repository: `reference-naive`, `reference-hardened` and `reference-hardened-lag1`. The target's name is the name results carry. A target is configuration, not a flag: the lag window used to be a CLI option, and now every setting that changes what is tested goes through a file the guard checks.

**Experiment** (`experiments/late-file.yaml`):

```yaml
name: late-file
description: >
  One file of a day's extract (the order lines) arrives after that day was
  loaded, under its own partition, two days late.
dataset:
  profile: tiny            # tiny | demo | large; any DatasetSpec field can be set too
schedule: daily            # daily (the steady state) | deliveries_only
faults:
  - type: late_file
    partition: 2           # day 2 of the dataset
    lands_before: 4
properties: [no_data_loss, no_silent_corruption, convergence]
recovery_rounds: 3         # at most: recovery stops once the outputs match the baseline
```

- **Days are counted within the dataset**: `1` is its first day, `last` its final one, resolved when the spec is loaded. The same experiment on a longer profile still means the same thing (`truncated-file` cuts the last day of whatever dataset it runs on), and a day outside the dataset is an error naming it ("day 4 is outside a 3-day dataset").
- **Faults and properties are named by their class's `name`** (`late_file`, `crash_mid_write`, `idempotency`, ...), with their parameters next to the type: `{type: idempotency, rerun: 2}`, `{type: backfill_equivalence, start: 1, end: last}`. A property without parameters can be a bare name.
- **Parameters are checked against the class they build** (pydantic `TypeAdapter` on each constructor argument): an unknown key, an unknown type, a missing argument or a value of the wrong kind is an error that names the file and the field, never a silent default. So are unknown top-level keys: `steady_state` and `max_runs` from the first draft of this section are refused (`convergence` became a property, and the limit `max_runs` was to set is `recovery_rounds`, now an upper bound).
- The file's `name` must match its file name, so a copied spec cannot run under the old name.

**Safety guard.** faultline deliberately destroys data, so it must be hard to point at the wrong place. Three layers, each enough to stop a mistake on its own:

1. **The target spec.** Its pydantic model rejects a target unless it declares `environment: test` and `allow_destructive: true`. There is no CLI flag to override this.
2. **The runner.** `run_experiment` and `run_baseline` take a `Target` (name, environment, allow_destructive, how to build a sandbox), not a bare adapter factory, and check it before anything runs, again before entering each sandbox and again before every action of every schedule. A `Target` built in code meets the same check when it is constructed, and one changed after that is stopped at its next action. An unsafe target is refused with `UnsafeTargetError`, never turned into a verdict.
3. **The adapter's own scope.** The reference adapter only touches workspaces named `fl_` + 8 hex characters, and it marks every workspace it creates, in all three stores: a comment on the Postgres schema and on the ClickHouse database (`faultline sandbox`), and a `.faultline-workspace` object at the root of the S3 prefix. Dropping a schema, a database or a prefix that has the name but not the mark is refused (`UnmarkedWorkspaceError`), and `faultline clean` reports such leftovers and leaves them alone. A database someone happened to name `fl_0123abcd` is never faultline's to drop.

The README carries this warning prominently. The guard protects against mistakes, not against a spec that says `environment: test` while pointing at production, which is why layer 3 exists for every adapter.

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
faultline run <experiment> [--target NAME ...] [--profile tiny|demo|large]
faultline run --all [--target NAME ...] [--profile ...] [--parallel N]
faultline report [RUN_ID] [--format md|json] [--expect scorecard/expected.yaml]
faultline clean [--force]
```

`run` takes an experiment by name (looked up in `./experiments`) or by path, and any number of `--target`s the same way (in `./targets`; default `reference-naive`), and runs the experiment on each in turn. `--profile` overrides the spec's dataset profile. `--all` runs every experiment in `./experiments` through `run_all`, writes each pair's log to its own file, prints one line per finished pair and the scorecard at the end.

`generate` refuses a non-empty output directory rather than deleting anything, and prints plain ASCII so the summary works on any console.

Results go to `.faultline/results/<run_id>/<experiment>__<target>.json` (the verdict, every property at F and R with its evidence, every run, the dataset, how long it took, where the snapshots are), next to `run.json` (what the run was asked to do, in order) and `logs/`. The run id is the start time, so runs sort in time order and `report` without an id reads the latest.

**Scorecard** (`faultline.report.scorecard`). A matrix of experiments by targets, one verdict per cell, a count of verdicts per target, and one line of evidence for every cell that is not a `PASS`: the first evidence of the property that decided the cell, or the reason for an `ERROR` or `SKIPPED`. Markdown for people and pull requests, JSON for CI. Checked against a file of expected verdicts (`--expect`, one verdict or a list of allowed ones per cell), it is a regression test: a cell off its expectation, or expected and missing, exits 1; a cell no expectation covers is only noted. A cell of an experiment that measures convergence also shows the rounds recovery took: `PASS (1)`, or `SILENT_CORRUPTION (>3)` when three were not enough. The JSON scorecard carries them under `rounds`, every result file under `recovery` (each round's runs, keys and rows off, and differences), and `--expect` still compares verdicts only.

## 11. Directory structure

```
faultline/
  pyproject.toml  uv.lock  Makefile  README.md  LICENSE
  .github/workflows/ci.yml
  docs/
    design.md
  src/faultline/
    cli.py  spec.py  safety.py   (YAML specs; the safety guard's Target)
    adapters/      base.py  reference.py
    oracle/        fingerprint.py  diff.py  invariants.py  snapshot.py
    schedule.py    Deliver / Run actions, clean_schedule
    faults/        base.py  source.py  landing.py  execution.py  files.py
    properties/    base.py  core.py     (no_data_loss, no_silent_corruption, no_vanishing_rows,
                                         idempotency, backfill_equivalence)
    outcome.py  runner.py
    datagen/       spec.py  catalog.py  dimensions.py  pos.py  writer.py  truth.py  generate.py
    report/        results.py  scorecard.py   (result files, the scorecard and its expectations)
    connectors/    dagster.py  clickhouse.py  postgres.py  s3.py  workspace.py
    sources/       pos.py   (fake POS source: tables and day-by-day replay)
  reference/       the platform under test, never imported by faultline
    docker-compose.yml  .env.example   (credentials; the real .env is gitignored)
    clickhouse/    entrypoint.sh  config.d/
    dagster/       Dockerfile  pyproject.toml  requirements.lock  dagster.yaml  workspace.yaml
                   coffee_platform/   Dagster definitions, dlt and ClickHouse assets
    dbt/           dbt project: silver and gold models, ClickHouse profile
  experiments/     *.yaml   one experiment per file
  targets/         *.yaml   one target per file, read through the safety guard
  scorecard/       expected.yaml   the verdicts the reference platform should get
  tests/           one folder per package area (datagen/, connectors/, reference/, ...);
                   tests that need the stack are marked `integration`
```

Runtime dependencies: typer, faker, duckdb (1.x, the oracle's engine), pydantic and pyyaml (the specs). The optional extra `faultline[reference]` adds psycopg for the source database connector. Development: pytest, ruff. Nothing else unless a concrete need proves it. The reference platform has its own fully pinned dependency set in `reference/dagster/requirements.lock`, installed only in its image.

## 12. Expected results (MVP definition of done)

`make demo` runs every experiment against both reference targets, prints the scorecard and checks it against `scorecard/expected.yaml`. Measured (run 20261127-085025, profile `tiny`, 27 minutes including `make ref-up`):

```
| experiment | reference-naive | reference-hardened |
|---|---|---|
| backfill | BROKEN | PASS |
| column-rename | SILENT_CORRUPTION (>3) | DETECTED (1) |
| crash-after-landing | SILENT_CORRUPTION (1) | PASS (1) |
| crash-mid-write | SILENT_CORRUPTION (>3) | SILENT_CORRUPTION (1) |
| duplicate-file | SILENT_CORRUPTION (>3) | PASS (1) |
| idempotency | SILENT_CORRUPTION | PASS |
| late-file | SILENT_CORRUPTION (>3) | PASS (1) |
| pos-offline-sync | SILENT_CORRUPTION (>3) | PASS (1) |
| retry-after-partial-commit | SILENT_CORRUPTION (>3) | PASS (1) |
| truncated-file | SILENT_CORRUPTION (>3) | DETECTED (1) |

(n): rounds of recovery until the outputs matched the baseline; (>n): not within n.

- reference-naive: 1 BROKEN, 9 SILENT_CORRUPTION
- reference-hardened: 7 PASS, 2 DETECTED, 1 SILENT_CORRUPTION

As expected: 20 of 20 cells checked against scorecard/expected.yaml
```

Naive is silently wrong on nine of ten and loudly, unrecoverably wrong on the tenth; hardened survives seven, refuses two, loudly, until the right file arrives, and on the tenth contradicts itself silently for as long as a crashed run is not rerun: right after crash-mid-write its tables are each whole and disagree with each other (`atomicity`). That is the definition of done the project set itself: every cell measured, every cell explained below, and the whole table a regression test. A cell that moves (a hardened `PASS` turning `SILENT_CORRUPTION` after a change to the platform, say) fails `make demo`; the fix is either in the platform or in `expected.yaml`, in the same pull request and with the reason. Backfill on naive may take either `BROKEN` or `SILENT_CORRUPTION` from one run to the next (whether one of its racing runs fails), and the file says so.

**Convergence, measured** (the same run). Hardened heals in the first round of recovery on all eight fault experiments, so the rounds cost it nothing: one rerun, three on pos-offline-sync, as before. Naive heals in one round on exactly one, crash-after-landing, whose crashed day still has its files waiting in the landing zone. On the other seven, three rounds are not enough, and every round makes it worse: each naive rerun appends the day's landing files to bronze once more. The keys off the baseline mostly hold still (595, 595, 595 on duplicate-file) while the rows behind them pile up (864, 1,429, 1,994) and gold's net revenue climbs by the same step every round:

| experiment (naive) | keys off, rounds 1 to 3 | rows off | net revenue in gold |
|---|---|---|---|
| column-rename | 230, 230, 230 | 230, 447, 664 | +19.3%, +38.7%, +58.0% |
| crash-mid-write | 310, 310, 310 | 310, 606, 902 | +24.9%, +49.9%, +74.8% |
| duplicate-file | 595, 595, 595 | 864, 1,429, 1,994 | +67.7%, +114.0%, +160.4% |
| late-file | 122, 285, 285 | 122, 391, 660 | +21.4%, +42.8%, +64.1% |
| pos-offline-sync (3 runs a round) | 886, 886, 886 | 886, 1,494, 2,102 | +35.5%, +86.1%, +136.6% |
| retry-after-partial-commit | 285, 285, 285 | 448, 717, 986 | +21.4%, +42.8%, +64.1% |
| truncated-file | 203, 230, 230 | 203, 420, 637 | +19.3%, +38.7%, +58.0% |

late-file and truncated-file get worse in keys too after the first round: the rerun that brings the missing lines back duplicates what was already there. The first round is what the notes on each experiment below describe; the other two only add copies. The matrix costs 136 Dagster runs instead of 118 (18 more reruns, all on naive) and 22 minutes instead of 17. Atomicity adds no run, only a snapshot of the baseline after every day; the run above took 136 Dagster runs. For a team the count turns a habit into a finding: on naive, "rerun it" is not a recovery procedure but a second incident, and the scorecard now says so in every cell instead of in a post-mortem.

The first draft of this table also listed a `late-row-at-source` experiment (a row whose `updated_at` is older than the dlt cursor). It was never a separate experiment: `pos-offline-sync` is exactly that, for a whole store at once.

At least one experiment must show `BROKEN` (noisy and unrecoverable): backfill on naive is the first, and it took no fault at all, only an ordinary operation the pipeline was never built for.

**First measured result** (`faultline run pos-offline-sync`, profile `tiny`, naive, about 8 minutes on Docker Desktop): every one of the 11 runs green, verdict `SILENT_CORRUPTION`.

- At F: all 62 orders of store HCM-001 from the two offline days missing from `silver_orders` (with 105 order lines and 69 payments); net revenue in gold -15.1%. dlt's cursor had passed their timestamps, so they were never extracted.
- At R, after the operator reran the three affected days: the 62 orders still missing, and every other store now double counted because naive bronze reloaded the same landing files. Net revenue +35.5% against the baseline, items sold +136.7%. The standard fix did not recover the data; it made the numbers wrong in the other direction.

**The same experiment on hardened** (`--target reference-hardened`, default lag window of 3 days): 11 of 11 runs green, verdict `PASS`. Every consumer table equals its clean baseline at F, as soon as the till synced, and again at R: the three reruns changed nothing.

**And with the lag window shrunk below the outage** (`--target reference-hardened-lag1`): `SILENT_CORRUPTION`, precisely bounded. On the sync day the window reached back one day, so the 40 orders of day 3 were counted and only the 22 of day 2 were lost (net revenue -4.6% instead of naive's -15.1%). At R the figures stayed at -4.6%: the reruns could not bring day 2 back, but idempotent bronze kept them from double counting the rest. The safeguards worked; the promise was simply shorter than the outage.

**duplicate-file** (`faultline run duplicate-file`, profile `tiny`): every run green on both editions (4 baseline, 4 fault phase, 1 recovery).

- Naive at F: `silver_orders` looks clean, since it keeps the latest state per order, so a uniqueness check on orders passes. Underneath, 163 order lines and 106 payments are there twice; net revenue in gold +21.4% and items sold +65.9% (copies on both sides of the join multiply). `no_data_loss` holds: nothing is missing, which is exactly why a completeness check would not catch it either.
- Naive at R, after rerunning day 3: bronze appends every file of day 3 once more, and net revenue reaches +67.7% against the baseline. The standard fix makes it worse.
- Hardened: `PASS` at F and R. Since the load manifest, the copies are stray files: day 3's load fails, loudly, and day 4's lag window reads the rows again, so consumers never see the copies. The recovery rerun of day 3 fails again, for the same stray files: refusing them is safe, but a real operator would still have to quarantine them by hand before that day turns green. The experiment makes that chore visible.

**late-file** (`faultline run late-file`, profile `tiny`): every run green on both editions (4 baseline, 5 fault phase with day 2 in two halves, 1 recovery).

- Naive at F: all 163 order lines of day 2 missing, items sold -22.0%, and revenue to the dong unchanged: orders and payments arrived on time, so a revenue dashboard shows nothing wrong. The file did land, two days late, but naive only reads the partition it runs, and dlt's cursor had already moved past those rows.
- Naive at R, after rerunning day 2: the lines are back (`no_data_loss` holds), but the rerun loaded the day's orders and payments a second time, so net revenue is now +21.4%. One gap closed, another opened.
- Hardened: `PASS` at F and R, through the lag window (see section 8 on why a file-only source would need a look back instead). Since the load manifest, day 2's load fails on the missing file first: noticed, then filled.

**truncated-file** (`faultline run truncated-file`, profile `tiny`): the last day's order lines file cut to 80% of its rows, on a row boundary. The first `DETECTED` of the scorecard.

- Naive at F: 27 order lines missing, items sold -3.9%, every run green; nothing in the file was invalid. At R, with the full file redelivered and day 4 rerun, bronze appended it on top of the cut one: 106 lines and 84 payments twice, net revenue +19.3%.
- Hardened at F: the load of day 4 fails (`load ... promised 424 rows, its files hold 339`), and the whole day is missing from gold, net revenue -19.3%. Bigger in numbers than naive's gap, and that is the design: a refused day with an alert is easier to handle than a short day nobody knows about. At R the full file reconciles, and every consumer table equals the baseline: `DETECTED`.
- The last day on purpose. Cut an earlier day and the next run's lag window reads its rows again from the source before checkpoint F: hardened then shows `PASS` with a red run, which is right but scores the source's re-readability, not the manifest.

**column-rename** (`faultline run column-rename`, profile `tiny`): the last day's payments file arrives with `amount_vnd` renamed `total_amount_vnd`.

- Naive at F: every run green, every payment there (`no_data_loss` holds), and 84 of them worth zero: net payments -19.3% in `gold_daily_payments`. The sales report, built from orders, has not moved, so two gold tables now disagree by a fifth of a day's revenue and only someone comparing them would notice. At R, with the agreed schema redelivered and day 4 rerun, naive appends the day once more: amounts back, everything else of day 4 twice.
- Hardened at F: the load of day 4 is refused (`missing amount_vnd; unexpected total_amount_vnd`) and the whole day is missing from gold. At R the file with the agreed schema loads and every consumer table equals the baseline: `DETECTED`.

**crash-after-landing** (`faultline run crash-after-landing`, profile `tiny`): the run of day 2 dies right after landing its files.

- Naive at F: the whole of day 2 is missing (99 orders, 163 lines, 106 payments, net revenue -21.4%), while days 3 and 4 ran green: dlt's cursor had moved past day 2 with the crashed run, and each later run loads only its own partition. The one red run is faultline's crash, so the verdict is `SILENT_CORRUPTION`. At R, rerunning day 2 loads the files that had been waiting in the landing zone all along, exactly once, and the numbers are right: the data was never lost, only stranded between two systems.
- Hardened: `PASS`. Day 3's lag window reads day 2's rows again from the source. The day 2 files and manifest in the landing zone are left unloaded until the rerun, harmless either way.

**crash-mid-write** (`faultline run crash-mid-write`, profile `tiny`): the run of day 3 dies inside the refresh of `gold_daily_store_sales`. Day 3's extract carries refunds of day 1 orders, so the refresh rewrites day 1 rows of three stores along with day 3.

- Naive at F: those three keys (day 1 of DN-001, HCM-002 and HN-001) were deleted and never inserted again; right after the crash `atomicity` finds the sales table half written (its days 1 and 2 down from 233 orders to 95). `no_vanishing_rows` names them between the snapshots around the crash, and they are still gone at F, after day 4 ran green: day 4 does not touch day 1, so nothing writes them back. The sales table lost a third of its orders (409 -> 273, net revenue -34.1%), and the only red run is faultline's crash: `SILENT_CORRUPTION`. Nothing upstream is wrong (silver and the payments table are right), so a reconciliation between the two gold tables is the only thing that would notice. At R, the rerun of day 3 puts the keys back, and naive bronze loads day 3's files a second time: every key is there, net revenue is +24.9%.
- Hardened: no row lost. The crash came after the side table was built and before the swap, so readers kept the old day 1 rows, and day 4 and the rerun rebuilt from final state. But silver (views over bronze, which the run had already rebuilt) and `gold_daily_payments` (built before the sales table) show day 3 at the moment of the crash, and `gold_daily_store_sales` does not: the payments report has 9,465,000 VND net for 3 November, the sales report no row for that day at all. Until the next run two official reports disagree, with only faultline's crash in red: `atomicity` violated, `SILENT_CORRUPTION`, the one hardened cell that is. Every other property holds at F and R. The side table itself is left behind (`gold_daily_store_sales__dbt_new_data_<invocation>`), a harmless leftover the next run of dbt never reads; a sandbox's teardown drops it with the database.

**retry-after-partial-commit** (`faultline run retry-after-partial-commit`, profile `tiny`): the run of day 2 dies right after bronze wrote the day's order lines; Dagster retries it from that step.

- Naive at F: every run green, the crashed one included (two attempts, the second green). The 163 order lines of day 2 are in bronze and silver twice, items sold +22.0%, and revenue is right to the dong: orders and payments were written once, so the sales dashboard shows nothing and only someone counting cups or stock would notice. At R, rerunning day 2 appends the whole day once more (net revenue +21.4%, lines three times).
- Hardened: `PASS`. The retry replaced the day's partition with the same rows, and the staging table the crash left behind was dropped by the retry before it rebuilt the day.
- The retry policy is what turned a crash into corruption. Without it, the crash would have been a red run and an operator's rerun, which on naive does the same damage; with it, nobody even sees red. Retries only help a step that can safely run twice, which makes idempotency the price of turning them on.

**idempotency** (`faultline run idempotency`, profile `tiny`): no fault. The four days run as usual, then day 2 is run again, with a snapshot on either side.

- Naive: the rerun is green and counts day 2 twice. Bronze appends every landing file of the day once more, so its 163 order lines and 106 payments are there twice; gold nets the day's events twice, so net revenue is +21.4%, orders +23.2% and items sold +65.9% (lines on both sides of the join multiply). Nothing was broken and nobody did anything unusual: an operator reran a day. Every earlier experiment's recovery showed the same thing as a side effect; this one makes it the question.
- Hardened: `PASS`. The rerun extracts the lag window again, bronze rebuilds the day's partition from its files, silver keeps one row per key and gold is built from final state, so the second snapshot equals the first, table by table.
- Why it matters to whoever runs the platform: on naive, "rerun the day" is not a fix but a fault of its own, which makes every incident runbook that says it unsafe. Idempotency is what lets an operator rerun without first proving that rerunning is harmless.

**backfill** (`faultline run backfill`, profile `tiny`): no fault. The four days reach the source before anything runs, then Dagster backfills them in one request: one run per day, submitted together, as many side by side as the run queue allows.

- Naive, measured five times: wrong every time, never the same way. The four runs extract with one dlt pipeline, whose cursor and working files belong to the pipeline, not to a day. Runs fail on files another run moved (`FileNotFoundError`, `LoadPackageNotFound`, a directory not empty), and every run that started before any cursor was saved extracted the whole history into its own day: once, twice (every table doubled, net revenue +99.0%) or three times. Four times out of five a run failed, so the verdict is `BROKEN`: the platform raised its hand, but nothing heals a history loaded twice. Once all four runs were green and the history came in once, yet gold was still off (3 orders, net revenue -0.5%): naive gold nets change events, and a history load only sees each order's final state, so a refund is subtracted from a sale it never saw. That run was `SILENT_CORRUPTION`.
- Hardened, without a safeguard: not safe either. In both tries a run failed on dlt's files; in the first the data still came out right, in the second two runs' loads interleaved under one day, the manifest refused it and a third of the orders were missing. Keyed silver could not save it: the damage was in the extract. With the extract lock (a Postgres advisory lock per workspace on the source database, held while dlt extracts and the manifest is written), every run green and every table equal to the daily answer, three tries out of three: `PASS`.
- The lineage column `extracted_on` (which landing partition a row came in) is left out of comparisons (`TableContract.ignore`): a history load brings every row in on its first day, which is true, and not a difference a reader should be warned about.
- What it means for a team: a backfill is an everyday operation, and an orchestrator runs it its own way, side by side by default. A pipeline whose extract keeps one cursor cannot be backfilled that way, however correct its daily runs are; the fix is not in the warehouse but in making the extract take turns, or keeping a cursor per partition.

## 13. Design risks

1. **A self-fulfilling demo.** We write both the bugs and the tests, so faultline could end up catching only bugs designed for it. Mitigations: faults are defined against generic contracts (files, runs, keys), never against naive internals; hardened uses established patterns rather than tricks tuned to pass; the reference stack is built from real tools (dlt, Dagster, dbt, ClickHouse), and the first adapter for a team's own platform is the real check.
2. **Non-deterministic baselines.** Volatile columns, floats and row order can make two clean runs differ. Mitigations: canonicalisation, `ignore_columns`, integer money, and a self-check that runs the baseline twice in tests and expects identical fingerprints.
3. **Failpoints need platform cooperation.** Real tools do not expose them. Mitigation: interrupt points are generic, and each adapter chooses how to realise them (failpoint, cancel API, or kill on an observed event).
4. **Blurry outcome boundaries.** `DETECTED` versus `BROKEN` versus `SILENT_CORRUPTION` invites argument. Mitigations: the fixed decision table in section 6, injected crashes never count as signals, and every verdict carries evidence.
5. **Run time.** One `pos_daily` run takes about 27 s on Docker Desktop (measured; about 40 s from launch, since the run worker spends some 13 s importing code first). Most of it is fixed cost: importing dbt and its ClickHouse adapter alone takes about 10 s, while the six models execute in under 2 s. An experiment with dozens of runs therefore takes minutes. Mitigations: the `tiny` profile or short windows for the demo, reusing a baseline across experiments, and above all running sandboxes side by side on one stack, since they share nothing. Measured on Docker Desktop with 16 CPUs, concurrent runs barely slow each other: one run alone takes 42 s from launch, two at once 44 s each, four 50 s, six 59 s, nine 74 s (1.75 times as long as alone). So `faultline run pos-offline-sync` went from about 8 minutes to 5 (baseline side by side with the fault phase: 7 runs of wall clock instead of 11), and the integration suite from 21.5 minutes run back to back to 6.5 on seven pytest-xdist workers. With the duplicate-file experiment on both editions it held 22 tests and 57 `pos_daily` runs, and took 8 minutes on ten workers (one per test unit): about twelve runs at a time, 51 s each on average. With late-file too: 25 tests, 79 runs, 11.5 minutes on twelve workers, some sixteen runs at a time and 71 s each. With truncated-file: 27 tests, 12 minutes on fourteen workers; the extra experiments mostly add width, not length, since each is no longer than the longest one already there. With column-rename the width stopped being free: 30 tests on sixteen workers, some twenty runs at a time, 117 s per run on average and 18.5 minutes in all. Past about a dozen concurrent runs on 16 CPUs every run slows down faster than the parallelism helps, so the next step looked like a cap on concurrent sandboxes. Measured across the suites, it is not: from about eight concurrent runs on, the stack completes some seven runs a minute however many run at once (41 runs in 386 s, 57 in 485 s, 79 in 690 s, 127 in 1108 s), so a cap would trade slower runs for longer queues and save nothing. The suite grows with its work, the number of Dagster runs; sharing one baseline per edition across experiments, or cutting each run's fixed cost (imports, dbt's re-parse), are the levers left. With crash-after-landing: 33 tests in 17.5 minutes on eighteen workers; with crash-mid-write, 35 tests in 16.5 minutes on twenty (the gap is run to run noise, not a gain). With retry-after-partial-commit the suite crossed a line the earlier measurements had not shown: on twenty-two workers some two dozen runs were launched within seconds, the code server had to spawn a run worker for each (about 13 s of imports apiece), its answer to a launch took longer than Dagster's 60 s gRPC timeout, and Dagster marked those runs failed before they began (`DagsterUserCodeUnreachableError`, then "marked as failed from outside the execution context"): 11 tests failed on runs that never started. The stack now queues runs (`QueuedRunCoordinator`, at most 12 at a time). A queue alone made it worse: the daemon launched queued runs one by one, each launch waiting for its worker to spawn, so only about seven slots were ever busy, runs waited up to eight minutes, throughput fell to 4.5 runs a minute and the suite took 37 minutes. Launching on four threads (`dequeue_use_threads`, `dequeue_num_workers: 4`) fixed that: 37 tests and 178 runs in 19.5 minutes, 9.1 runs a minute (more than the seven without a queue, since runs no longer slow each other down: 46 s each), queue waits of 2 to 4 minutes. Waits on a run now include its time in the queue, so a test's run timeout is the adapter's (`run_timeout_seconds`, 900 s), never a shorter one of its own. The cap that looked useless when measured by throughput turned out to be about correctness: without it, a busy stack does not slow down, it drops runs. With idempotency: 39 tests and 196 runs in 25 minutes on twenty-four workers, 7.8 runs a minute (9.1 the time before: from run to run the rate moves by that much). With the queue full the suite grows by about a minute for every eight runs an experiment adds; the lever left is the baselines: every experiment class still runs its own four clean days, 72 of the 196 runs, where one baseline per edition would do. With backfill: 41 tests and 212 runs in 22 minutes on twenty-six workers, 9.5 runs a minute; a backfill's four runs share the queue like any others. With the specs and the guard, the same 41 tests and 212 runs: 35 minutes on the first suite after a fresh `make ref-up` (every run 76 s, the stack still cold), 27 minutes on the next one (48 s a run, 7.8 runs a minute). The guard's checks and the sandbox marks cost nothing measurable; a cold stack costs a third. `make demo` takes 17 minutes for the whole matrix (118 runs, 35 s each, 7 a minute): one baseline per edition instead of one per test. The same suite on the same machine has now taken 22, 25, 27, 35 and 38 minutes for the same 212 runs: what moves is how long each run takes (48 to 83 s), not how many faultline launches. Not yet explained: whether Dagster's growing run storage (every run's event log is kept across `make ref-up`) is part of it. Then the suite stopped paying for baselines it did not need: one clean run per edition for the whole session, shared by every worker, and the pipeline checks reading the same one. 41 tests, 136 runs instead of 212 (88 baseline runs down to 12), 16 minutes on twenty-six workers, the first suite after a fresh `make ref-up` at that; queue waits fell from three to six minutes to under one. Convergence then gave naive's recovery up to three rounds: 41 tests and 154 runs (18 more reruns, all on naive, since hardened stops after its first round), 23.5 minutes on twenty-six workers; `make demo` 22 minutes for 136 runs. Atomicity's snapshots of the baseline after every day add no run: the same 154 runs took 26.5 minutes (the first suite after a restarted stack), `make demo` 27 minutes for the same 136 runs, on the same restarted stack. That is where the gain from running side by side flattens on 16 CPUs, and where Dagster's webserver started to answer slower than the client's timeout (see section 4.1). The suite spends more compute to get there (every experiment runs its own baseline, the determinism check its own two clean runs) and less wall clock, which is the trade worth making while runs mostly wait on imports rather than on CPU. Hardened runs take as long as naive ones; the lag window costs nothing visible at `tiny` scale. Not yet done: avoiding dbt's full re-parse on every run (changing `--vars` invalidates partial parsing, about 3 to 4 s). `make demo` target to be re-set once the first experiment runs end to end. CI runs the integration suite only on pushes to main, split by edition into two parallel jobs that each start their own stack (`make test-integration EDITION=naive|hardened WORKERS=n`, one worker per test unit), with the platform image built through `docker buildx bake` and its layers cached in GitHub's cache. GitHub's 4-vCPU runners execute a run in about 16 to 19 s alone, faster than Docker Desktop; how far they gain from running several at once is measured on the first push. Pull requests get lint and unit tests (under a minute), and integration is run locally with `make test-integration` before a PR is opened.
6. **Windows versus Linux.** Integration tests need Linux containers, which GitHub's Windows runners cannot host, so they run on ubuntu only (on pushes to main) while unit tests still run on Windows for every pull request. Locally, Git Bash rewrites absolute paths passed to `docker exec` (`/usr/bin/x` becomes `C:/Program Files/Git/usr/bin/x`); set `MSYS_NO_PATHCONV=1` for such commands.
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
