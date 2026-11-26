from pathlib import Path

import pytest
from typer.testing import CliRunner

from faultline import __version__
from faultline.adapters.reference import CleanReport, StackBusyError
from faultline.cli import app
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult

runner = CliRunner()


def test_version_flag_prints_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout.strip() == f"faultline {__version__}"


def test_no_arguments_shows_help() -> None:
    result = runner.invoke(app, [])

    assert "Usage" in result.output


def test_generate_writes_landing_truth_and_metadata(tmp_path: Path) -> None:
    out = tmp_path / "sample"
    result = runner.invoke(
        app, ["generate", "--profile", "tiny", "--days", "2", "--workers", "1", "--out", str(out)]
    )

    assert result.exit_code == 0, result.output
    assert "pos/orders" in result.output
    assert (out / "generation.json").is_file()
    assert (out / "truth" / "control_totals.csv").is_file()
    assert sorted(p.name for p in (out / "landing" / "pos" / "orders").iterdir()) == [
        "dt=2026-11-01",
        "dt=2026-11-02",
    ]
    result.output.encode("ascii")  # the summary must print on any console


def test_generate_refuses_a_non_empty_directory(tmp_path: Path) -> None:
    (tmp_path / "keep.txt").write_text("existing", encoding="utf-8")

    result = runner.invoke(app, ["generate", "--profile", "tiny", "--out", str(tmp_path)])

    assert result.exit_code == 2
    assert "not empty" in result.output
    assert sorted(p.name for p in tmp_path.iterdir()) == ["keep.txt"]


def test_generate_rejects_an_invalid_date(tmp_path: Path) -> None:
    result = runner.invoke(
        app, ["generate", "--profile", "tiny", "--start-date", "01/11/2026", "--out", str(tmp_path)]
    )

    assert result.exit_code != 0


def _target_file(folder: Path, *, environment: str = "test", env_file: str = "missing.env") -> Path:
    path = folder / "sandbox.yaml"
    path.write_text(
        "name: sandbox\n"
        "adapter: reference\n"
        f"environment: {environment}\n"
        "allow_destructive: true\n"
        "options:\n"
        f"  env_file: {env_file}\n",
        encoding="utf-8",
    )
    return path


def test_run_rejects_an_unknown_experiment() -> None:
    result = runner.invoke(app, ["run", "no-such-thing"])

    assert result.exit_code == 2
    assert "no experiment named 'no-such-thing'" in result.output
    assert "pos-offline-sync" in result.output  # the known ones are listed


def test_run_rejects_an_unknown_target() -> None:
    result = runner.invoke(app, ["run", "late-file", "--target", "bulletproof"])

    assert result.exit_code == 2
    assert "no target named 'bulletproof'" in result.output
    assert "reference-hardened-lag1" in result.output


def test_run_refuses_a_target_that_is_not_a_test_environment(tmp_path: Path) -> None:
    target = _target_file(tmp_path, environment="production")

    result = runner.invoke(app, ["run", "late-file", "--target", str(target)])

    assert result.exit_code == 2
    assert "faultline only runs against environment 'test'" in result.output


def test_run_explains_a_missing_stack(tmp_path: Path) -> None:
    # By path this time: the spec parses, the target too, then the stack is missing.
    target = _target_file(tmp_path)

    result = runner.invoke(app, ["run", "experiments/late-file.yaml", "--target", str(target)])

    assert result.exit_code == 2
    assert "make ref-up" in result.output


def test_run_runs_the_experiment_on_every_target_in_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = tmp_path / "stack.env"
    env.write_text(
        "SOURCE_DB_USER=u\nSOURCE_DB_PASSWORD=p\nSOURCE_DB_NAME=db\nCLICKHOUSE_USER=u\n"
        "CLICKHOUSE_PASSWORD=p\nS3_ACCESS_KEY=k\nS3_SECRET_KEY=s\n",
        encoding="utf-8",
    )
    first = _target_file(tmp_path, env_file=env.name)
    second = tmp_path / "other" / "sandbox.yaml"
    second.parent.mkdir()
    second.write_text(
        first.read_text(encoding="utf-8").replace(env.name, f"../{env.name}"), encoding="utf-8"
    )
    seen: list[tuple[str, str, int]] = []

    def fake_run(experiment, target, work_dir, *, log):  # type: ignore[no-untyped-def]
        seen.append((experiment.name, target.name, experiment.spec.days))
        return ExperimentResult(experiment.name, target.name, Outcome.PASS, work_dir=work_dir)

    monkeypatch.setattr("faultline.runner.run_experiment", fake_run)

    result = runner.invoke(
        app,
        ["run", "late-file", "--target", str(first), "--target", str(second), "--profile", "demo"],
    )

    assert result.exit_code == 0, result.output
    assert [(e, t) for e, t, _ in seen] == [("late-file", "sandbox")] * 2
    assert seen[0][2] == DatasetSpec.from_profile("demo").days  # --profile won over the spec
    assert result.output.count("Verdict:    PASS") == 2


def test_clean_explains_a_missing_stack(tmp_path: Path) -> None:
    result = runner.invoke(app, ["clean", "--stack-env", str(tmp_path / ".env")])

    assert result.exit_code == 2
    assert "make ref-up" in result.output


def _env_file(tmp_path: Path) -> Path:
    env = tmp_path / ".env"
    example = Path(__file__).resolve().parents[1] / "reference" / ".env.example"
    env.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
    return env


def test_clean_refuses_a_busy_stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def busy(config: object, *, force: bool) -> None:
        assert not force
        raise StackBusyError("a Dagster run is in progress")

    monkeypatch.setattr("faultline.adapters.reference.clean_reference_stack", busy)

    result = runner.invoke(app, ["clean", "--stack-env", str(_env_file(tmp_path))])

    assert result.exit_code == 1
    assert "Not cleaning: a Dagster run is in progress" in result.output
    assert "--force" in result.output


def test_clean_reports_what_it_removed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[bool] = []

    def cleaned(config: object, *, force: bool) -> CleanReport:
        seen.append(force)
        return CleanReport(landing=("fl_00000001", "fl_00000002"), source=("fl_00000001",))

    monkeypatch.setattr("faultline.adapters.reference.clean_reference_stack", cleaned)

    result = runner.invoke(app, ["clean", "--force", "--stack-env", str(_env_file(tmp_path))])

    assert result.exit_code == 0, result.output
    assert seen == [True]
    assert result.output.strip() == (
        "Removed 2 leftover sandboxes: 2 landing prefixes, 1 source schemas, 0 warehouse databases"
    )
