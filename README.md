# faultline

Chaos and invariant testing for data platforms.

faultline injects faults into ingestion and pipelines (duplicate files, late data, schema drift, crashes, retries) and checks that your platform stays idempotent, atomic, backfill-safe and recoverable. Soda tests your data; faultline tests your platform.

> **Warning:** faultline deliberately corrupts and deletes data. Point it only at disposable test or CI environments. A target must declare `environment: test` and `allow_destructive: true`, or faultline refuses to run.

**Status:** early development. Six experiments, one at the source, four in the landing zone and one during execution, run end to end against both editions of the bundled reference platform. See [docs/design.md](docs/design.md) for the design.

## Try it

With Docker running:

```sh
make install
make ref-up                                              # Postgres, dlt, S3, ClickHouse, dbt, Dagster
uv run faultline run pos-offline-sync                    # naive edition, about 5 minutes on a laptop
uv run faultline run pos-offline-sync --target reference-hardened  # hardened edition
uv run faultline run duplicate-file                      # a resent extract, about 4 minutes
uv run faultline run late-file                           # a file that lands two days late
uv run faultline run truncated-file                      # a file cut short, still well formed
uv run faultline run column-rename                       # a file with a column renamed
uv run faultline run crash-after-landing                 # a run killed between landing and loading
uv run faultline run crash-mid-write                     # a run killed inside a gold table refresh
uv run faultline run retry-after-partial-commit          # a crashed run retried by the orchestrator
uv run faultline run idempotency                         # no fault: an old day is simply run again
uv run faultline run backfill                            # no fault: the whole history in one backfill
uv run faultline run late-file --target reference-naive --target reference-hardened
make demo                                                # all ten on both editions, then the scorecard
uv run faultline report --format json                    # the latest run's scorecard, for CI
```

`make demo` runs all ten on both editions, one clean run per edition shared by every experiment, the experiments side by side, about 22 minutes on a laptop. It prints the scorecard and checks it against the verdicts the platform should get:

```
| experiment | reference-naive | reference-hardened |
|---|---|---|
| backfill | BROKEN | PASS |
| column-rename | SILENT_CORRUPTION (>3) | DETECTED (1) |
| crash-after-landing | SILENT_CORRUPTION (1) | PASS (1) |
| crash-mid-write | SILENT_CORRUPTION (>3) | PASS (1) |
| duplicate-file | SILENT_CORRUPTION (>3) | PASS (1) |
| idempotency | SILENT_CORRUPTION | PASS |
| late-file | SILENT_CORRUPTION (>3) | PASS (1) |
| pos-offline-sync | SILENT_CORRUPTION (>3) | PASS (1) |
| retry-after-partial-commit | SILENT_CORRUPTION (>3) | PASS (1) |
| truncated-file | SILENT_CORRUPTION (>3) | DETECTED (1) |

(n): rounds of recovery until the outputs matched the baseline; (>n): not within n.

- reference-naive: 1 BROKEN, 9 SILENT_CORRUPTION
- reference-hardened: 8 PASS, 2 DETECTED

As expected: 20 of 20 cells checked against scorecard/expected.yaml
```

Read it as a manager would: the naive pipeline is wrong on every scenario and says so only once, on the one where it cannot recover. Hardened survives eight and refuses the other two until the right file arrives, which is the failure you want: loud and fixable. Every cell that is not a `PASS` comes with one line of evidence (`silver_order_items: 163 duplicated keys, qty +22.0%`), every result is saved as JSON (`faultline report --format json` for CI), and because the table is checked against `scorecard/expected.yaml`, a change that makes the platform worse at surviving a fault fails the build like any other regression.

The number in brackets is how many rounds of recovery it took: faultline reruns the days a fault touched, compares every table with the clean run, and goes round again until they match, three times at most. Hardened always heals in the first round. Naive heals in one round once, and on the other seven no amount of rerunning helps, it makes things worse: every rerun appends the same files again, so on `duplicate-file` the rows off the clean run go 864, 1,429, 1,994 and gold's revenue +68%, +114%, +160%. The runbook's first move is a second incident on this pipeline, and the scorecard shows it.

Every experiment is a YAML file in `experiments/` and every platform a target in `targets/`, so adding a scenario is a reviewed pull request rather than new code. Days in a spec are counted within the dataset (`partition: 2`, `end: last`), so a longer profile keeps their meaning, and anything a spec gets wrong (an unknown fault, a misspelt parameter, day 9 of a four-day dataset) is an error that names the file and the field. A target must declare `environment: test` and `allow_destructive: true`; the runner checks that again before every action, and the reference adapter only ever drops a sandbox it marked as its own.

`pos-offline-sync` takes a store's till offline for two days. When it reconnects, its orders reach the source database with their real, older timestamps, behind the incremental cursor of the ingestion tool. On the reference platform's naive pipeline the result looks like this:

```
Runs:       baseline 4/4 green, fault phase 4/4 green, recovery 9/9 green
  property                F         R         outcome
  no_data_loss            violated  violated  SILENT_CORRUPTION
  no_silent_corruption    violated  violated  SILENT_CORRUPTION
  convergence             holds     violated  SILENT_CORRUPTION
Recovery:   not converged within 3 rounds (9 runs): 886, 886, 886 keys and 886, 1,494, 2,102 rows off the baseline, growing
Verdict:    SILENT_CORRUPTION
Evidence at F:
  - silver_orders: complete_keys violated by 62 keys, e.g. HCM-001-20261102-00001, ...
  - gold_daily_store_sales: 2 missing keys, ..., net_vnd 37,937,000 -> 32,214,000 (-15.1%)
```

Every run was green, so no alert fired and nobody would have been paged, yet two days of one store's sales are gone. Rerunning the days, the usual fix, does not bring them back and double counts the other stores instead (net revenue +35.5% after one round of reruns, +136.6% after three). That is the gap faultline measures. At checkpoint F, the usual data quality checks (unique keys, no nulls, valid values) all pass: nothing in the tables is malformed, rows are simply absent. The platform failed, not the data. Snapshots of every checkpoint stay in `.faultline/runs/` for inspection.

The hardened edition runs the same tools with established patterns: a lag window in the extractor, idempotent bronze loads, one row per key in silver, and gold built from final states. Same fault, same runs, verdict `PASS` at both checkpoints. Its lag window is a promise with a limit, and faultline can measure that too: with a one-day window (`--target reference-hardened-lag1`), shorter than the two-day outage, hardened loses exactly the oldest day (22 orders) and the verdict is `SILENT_CORRUPTION` again. Choosing that window is a business decision (how late may a till sync and still count?) that now comes with a number attached.

`duplicate-file` works one layer down, in the landing zone: day 2's extract lands a second time under day 3, byte for byte, as a retried upload would leave it. On naive, orders still look clean (one row per order), so a uniqueness check on them passes, while 163 order lines and 106 payments are there twice and gold reports 21.4% more revenue; rerunning day 3 takes it to +67.7%. Hardened passes without a ledger of loaded files, because every table has a key and silver keeps one row per key. When the data has no natural key (an append-only points ledger, say), that is no longer enough, and the experiment is where a team would find out.

`late-file` holds one file back instead: day 2 runs as ingest and load, and between the two the order lines file is taken out of the landing zone; it lands, in its own partition, two days later. Naive only reads the partition it runs, so the 163 lines never arrive, and the damage hides well: items sold drop 22% while revenue is right to the dong, because orders and payments came on time. Rerunning day 2 brings the lines back and loads the day's orders and payments a second time (+21.4% revenue). Hardened passes, through its lag window: it reads the rows again from the source database. A source that only drops files cannot be read again, and would need bronze to look back over recent landing partitions instead.

`truncated-file` is the quietest of the four: the last day's order lines file loses a fifth of its rows, cut on a row boundary, so it parses and every value in it is valid. Naive loads it and every run stays green; 27 lines are simply not there. Hardened now keeps a delivery note: each extract writes a small manifest next to its files (rows promised, files written), and bronze refuses a day whose files do not match it. Day 4's load fails with `load ... promised 424 rows, its files hold 339`, the full file is redelivered, the rerun reconciles, and the numbers are right: `DETECTED`, noticed and healed. The same note also catches the earlier faults on hardened, a stray resent copy or a missing late file, as red runs with the right numbers. A count only proves something when it comes from the side that sent the data, which is why the manifest sits where faults cannot reach it.

`column-rename` renames one column of the last day's payments file, `amount_vnd` to `total_amount_vnd`. Naive's warehouse reads files by column name, finds `amount_vnd` missing and quietly fills it with zeros: every run green, every payment present, 84 of them worth nothing. The sales report does not move, since it is built from orders, so two official numbers for the same day now disagree by a fifth. The manifest cannot help here, because the producer writes it and may mean the new schema; hardened keeps its own schema contract instead, refuses the day on `missing amount_vnd; unexpected total_amount_vnd`, and loads it once the file comes back with the agreed schema: `DETECTED`. Who owns a schema change, and who has to be told, is exactly what that red run forces into the open.

`crash-after-landing` is the first execution fault: day 2's run is killed, hard (`os._exit`, nothing cleaned up), right after ingestion landed its files and saved its cursor, before anything was loaded. The platform dies at a named failpoint and logs the fact first, so faultline can tell its own crash from a real failure. On naive the whole of day 2 goes missing while days 3 and 4 run green: the cursor lives with the landing files, the data was meant for the warehouse, and the crash left the first ahead of the second. The red run in Dagster says a process died, not that a day of sales is gone, so the verdict is `SILENT_CORRUPTION`. Rerunning the day fixes it, if someone knows to. Hardened passes: its lag window reads the day again on the next run.

`crash-mid-write` kills day 3's run in the middle of refreshing the gold sales table. Naive refreshes it the way dbt's `delete+insert` strategy does: delete the rows of every day the new data touches, then insert the new ones. Day 3 carries refunds of day 1 orders, so the delete takes out day 1 for three stores, and the crash lands before the insert. faultline snapshots the tables just before and just after the crash, and a new property, `no_vanishing_rows`, names the keys a reader could see a moment earlier and no longer can. Day 4 runs green and never touches day 1, so the rows stay gone: a third of the table's orders, with only faultline's own crash in red. Hardened builds the new rows in a side table and swaps them in with one statement, so a crash before the swap leaves readers with the old numbers: `PASS`. In dbt the choice is one config line plus a partition key (and a model that rebuilds whole days), and the difference only shows on a day when something dies halfway, which is why it is worth deciding on purpose rather than by default.

`retry-after-partial-commit` kills day 2's run right after bronze wrote the day's order lines, before Dagster recorded the step as done, and lets Dagster retry it, as a retry policy would. The write was committed, the record of it was not, so the retry writes it again. On naive, which appends, the 163 lines are there twice: items sold +22%, revenue unchanged to the dong, every run green. Hardened replaces the day's partition, so writing it twice changes nothing: `PASS`. Turning on retries is a one-line setting that makes every crash disappear from the run history; whether that is a good idea depends entirely on whether each step can safely run twice, and this experiment is how a team finds out before the retries do.

`idempotency` breaks nothing at all. The four days run as usual, then day 2 is run again, as an operator does after an incident or a fix, and faultline compares the tables just before and just after. On naive the rerun is green and counts the day twice: net revenue +21.4%, items sold +65.9%. That is the move every runbook reaches for first, and on this pipeline it is a fault of its own. Hardened gives back exactly the same tables. The check is a probe: a property may add actions of its own (here a snapshot, the rerun, a snapshot), and faultline runs them in their own experiment so the answer is not muddied by an earlier fault.

`backfill` breaks nothing either. All four days reach the source first, then Dagster backfills them in one request, as a team loading its history would, and the tables must equal those of running the days one by one. Dagster runs a backfill's days side by side, and naive's four extracts share one dlt cursor and one set of working files: runs fail on each other's files, and the history lands once, twice or three times depending on who started first. It is the first `BROKEN` of the scorecard (a run failed, nothing heals it), and on the one try where every run was green the numbers were still off. Hardened was wrong too until its extracts took turns: a lock on the source database per pipeline, while loads and dbt still run side by side. The bug was never in the daily runs, so no daily test would have found it.

## Development

Requires [uv](https://docs.astral.sh/uv/) and GNU Make. The reference platform (Postgres, dlt, an S3 landing bucket, ClickHouse, dbt and Dagster) also needs Docker, for example Docker Desktop.

```sh
make install            # create .venv and install dependencies
make check              # lint, format check and the tests that need no Docker

make ref-up             # build and start the reference platform (first build takes a few minutes)
make test-integration   # run the tests that drive the real platform (about 18 min on 18 parallel workers;
                        #   EDITION=naive|hardened for one half, WORKERS=0 for one process)
make ref-clean          # delete leftover sandboxes (fl_*) once nothing runs; FORCE=1 to insist
make ref-down           # stop it and delete its data
```

Pull requests on GitHub run only lint and the Docker-free tests; the integration suite runs on every push to `main`, as two parallel jobs, one per edition. Run `make test-integration` locally before opening a pull request that touches `src/` or `reference/`.

While the reference platform is up, the Dagster UI is at <http://localhost:3000> and the S3 console at <http://localhost:9001/rustfs/console/> (log in with `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `reference/.env`).

Credentials live in `reference/.env`, which `make ref-up` creates from [`reference/.env.example`](reference/.env.example) the first time and never overwrites. They are throwaway test values and every port is bound to localhost; to change one, edit `reference/.env` and run `make ref-down` first, since the databases keep the credentials they were created with.

## License

[MIT](LICENSE)
