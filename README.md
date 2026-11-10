# faultline

Chaos and invariant testing for data platforms.

faultline injects faults into ingestion and pipelines (duplicate files, late data, schema drift, crashes, retries) and checks that your platform stays idempotent, atomic, backfill-safe and recoverable. Soda tests your data; faultline tests your platform.

> **Warning:** faultline deliberately corrupts and deletes data. Point it only at disposable test or CI environments. A target must declare `environment: test` and `allow_destructive: true`, or faultline refuses to run.

**Status:** early development, nothing usable yet. See [docs/design.md](docs/design.md) for the design.

## Development

Requires [uv](https://docs.astral.sh/uv/) and GNU Make. The reference platform (Postgres, dlt, an S3 landing bucket, ClickHouse, dbt and Dagster) also needs Docker, for example Docker Desktop.

```sh
make install            # create .venv and install dependencies
make check              # lint, format check and the tests that need no Docker

make ref-up             # build and start the reference platform (first build takes a few minutes)
make test-integration   # run the tests that drive the real platform
make ref-down           # stop it and delete its data
```

While the reference platform is up, the Dagster UI is at <http://localhost:3000> and the S3 console at <http://localhost:9001/rustfs/console/> (log in with `S3_ACCESS_KEY` / `S3_SECRET_KEY` from `reference/.env`).

Credentials live in `reference/.env`, which `make ref-up` creates from [`reference/.env.example`](reference/.env.example) the first time and never overwrites. They are throwaway test values and every port is bound to localhost; to change one, edit `reference/.env` and run `make ref-down` first, since the databases keep the credentials they were created with.

## License

[MIT](LICENSE)
