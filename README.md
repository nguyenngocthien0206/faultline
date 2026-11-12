# faultline

Chaos and invariant testing for data platforms.

faultline injects faults into ingestion and pipelines (duplicate files, late data, schema drift, crashes, retries) and checks that your platform stays idempotent, atomic, backfill-safe and recoverable. Soda tests your data; faultline tests your platform.

> **Warning:** faultline deliberately corrupts and deletes data. Point it only at disposable test or CI environments. A target must declare `environment: test` and `allow_destructive: true`, or faultline refuses to run.

**Status:** early development. Two experiments, one at the source and one in the landing zone, run end to end against both editions of the bundled reference platform. See [docs/design.md](docs/design.md) for the design.

## Try it

With Docker running:

```sh
make install
make ref-up                                              # Postgres, dlt, S3, ClickHouse, dbt, Dagster
uv run faultline run pos-offline-sync                    # naive edition, about 5 minutes on a laptop
uv run faultline run pos-offline-sync --target hardened  # hardened edition
uv run faultline run duplicate-file                      # a resent extract, about 4 minutes
```

`pos-offline-sync` takes a store's till offline for two days. When it reconnects, its orders reach the source database with their real, older timestamps, behind the incremental cursor of the ingestion tool. On the reference platform's naive pipeline the result looks like this:

```
Runs:       baseline 4/4 green, fault phase 4/4 green, recovery 3/3 green
  property                F         R         outcome
  no_data_loss            violated  violated  SILENT_CORRUPTION
  no_silent_corruption    violated  violated  SILENT_CORRUPTION
Verdict:    SILENT_CORRUPTION
Evidence at F:
  - silver_orders: complete_keys violated by 62 keys, e.g. HCM-001-20261102-00001, ...
  - gold_daily_store_sales: 2 missing keys, ..., net_vnd 37,937,000 -> 32,214,000 (-15.1%)
```

Every run was green, so no alert fired and nobody would have been paged, yet two days of one store's sales are gone. Rerunning the days, the usual fix, does not bring them back and double counts the other stores instead (net revenue +35.5% at checkpoint R). That is the gap faultline measures. At checkpoint F, the usual data quality checks (unique keys, no nulls, valid values) all pass: nothing in the tables is malformed, rows are simply absent. The platform failed, not the data. Snapshots of every checkpoint stay in `.faultline/runs/` for inspection.

The hardened edition runs the same tools with established patterns: a lag window in the extractor, idempotent bronze loads, one row per key in silver, and gold built from final states. Same fault, same runs, verdict `PASS` at both checkpoints. Its lag window is a promise with a limit, and faultline can measure that too: with `--lag-days 1`, shorter than the two-day outage, hardened loses exactly the oldest day (22 orders) and the verdict is `SILENT_CORRUPTION` again. Choosing that window is a business decision (how late may a till sync and still count?) that now comes with a number attached.

`duplicate-file` works one layer down, in the landing zone: day 2's extract lands a second time under day 3, byte for byte, as a retried upload would leave it. On naive, orders still look clean (one row per order), so a uniqueness check on them passes, while 163 order lines and 106 payments are there twice and gold reports 21.4% more revenue; rerunning day 3 takes it to +67.7%. Hardened passes without a ledger of loaded files, because every table has a key and silver keeps one row per key. When the data has no natural key (an append-only points ledger, say), that is no longer enough, and the experiment is where a team would find out.

## Development

Requires [uv](https://docs.astral.sh/uv/) and GNU Make. The reference platform (Postgres, dlt, an S3 landing bucket, ClickHouse, dbt and Dagster) also needs Docker, for example Docker Desktop.

```sh
make install            # create .venv and install dependencies
make check              # lint, format check and the tests that need no Docker

make ref-up             # build and start the reference platform (first build takes a few minutes)
make test-integration   # run the tests that drive the real platform (about 8 min on 10 parallel workers;
                        #   EDITION=naive|hardened for one half, WORKERS=0 for one process)
make ref-down           # stop it and delete its data
```

Pull requests on GitHub run only lint and the Docker-free tests; the integration suite runs on every push to `main`, as two parallel jobs, one per edition. Run `make test-integration` locally before opening a pull request that touches `src/` or `reference/`.

While the reference platform is up, the Dagster UI is at <http://localhost:3000> and the S3 console at <http://localhost:9001/rustfs/console/> (log in with `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `reference/.env`).

Credentials live in `reference/.env`, which `make ref-up` creates from [`reference/.env.example`](reference/.env.example) the first time and never overwrites. They are throwaway test values and every port is bound to localhost; to change one, edit `reference/.env` and run `make ref-down` first, since the databases keep the credentials they were created with.

## License

[MIT](LICENSE)
