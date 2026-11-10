# faultline

Chaos and invariant testing for data platforms.

faultline injects faults into ingestion and pipelines (duplicate files, late data, schema drift, crashes, retries) and checks that your platform stays idempotent, atomic, backfill-safe and recoverable. Soda tests your data; faultline tests your platform.

> **Warning:** faultline deliberately corrupts and deletes data. Point it only at disposable test or CI environments. A target must declare `environment: test` and `allow_destructive: true`, or faultline refuses to run.

**Status:** early development, nothing usable yet. See [docs/design.md](docs/design.md) for the design.

## Development

Requires [uv](https://docs.astral.sh/uv/) and GNU Make.

```sh
make install   # create .venv and install dependencies
make check     # lint, format check and tests
```

## License

[MIT](LICENSE)
