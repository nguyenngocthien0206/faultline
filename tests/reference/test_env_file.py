"""The env file parser and the example file every fresh checkout starts from."""

from pathlib import Path

from reference_stack import REFERENCE_DIR

from faultline.adapters.reference import config_from_env_file, parse_env_file

REQUIRED = {
    "SOURCE_DB_USER",
    "SOURCE_DB_PASSWORD",
    "SOURCE_DB_NAME",
    "DAGSTER_DB_USER",
    "DAGSTER_DB_PASSWORD",
    "DAGSTER_DB_NAME",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
    "S3_ACCESS_KEY",
    "S3_SECRET_KEY",
}


def test_parser_handles_comments_blanks_quotes_and_equals_in_values(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text(
        "# comment\n\nA=1\nB = two \nC=\"quoted value\"\nD='single'\nE=x=y\nnot a pair\n",
        encoding="utf-8",
    )

    assert parse_env_file(env) == {
        "A": "1",
        "B": "two",
        "C": "quoted value",
        "D": "single",
        "E": "x=y",
    }


def test_example_defines_every_variable_the_stack_needs() -> None:
    example = parse_env_file(REFERENCE_DIR / ".env.example")

    assert set(example) == REQUIRED
    assert all(example.values())


def test_compose_references_only_variables_from_the_example() -> None:
    compose = (REFERENCE_DIR / "docker-compose.yml").read_text(encoding="utf-8")
    example = parse_env_file(REFERENCE_DIR / ".env.example")

    for name in REQUIRED:
        assert f"${{{name}:?" in compose, name
    for value in ("faultline-secret", "dagster:dagster"):
        assert value not in compose, f"hard-coded credential {value!r} left in compose"
    assert example["S3_SECRET_KEY"] not in compose


def test_config_comes_from_the_env_file_and_the_environment_wins(tmp_path: Path) -> None:
    env = tmp_path / ".env"
    env.write_text((REFERENCE_DIR / ".env.example").read_text(encoding="utf-8"), encoding="utf-8")

    plain = config_from_env_file(env, environ={})
    overridden = config_from_env_file(env, environ={"CLICKHOUSE_PASSWORD": "rotated"})

    assert plain.source_dsn == "postgresql://faultline:faultline@127.0.0.1:5433/coffee"
    assert plain.clickhouse_user == "faultline"
    assert overridden.clickhouse_password == "rotated"
    assert plain.s3_access_key == "faultline"  # the landing bucket, for landing faults
    assert plain.s3_secret_key == "faultline-secret"
