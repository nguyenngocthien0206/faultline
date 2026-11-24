from pathlib import Path

import pytest
from typer.testing import CliRunner

from faultline import __version__
from faultline.adapters.reference import CleanReport, StackBusyError
from faultline.cli import app

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


def test_run_rejects_an_unknown_experiment(tmp_path: Path) -> None:
    result = runner.invoke(app, ["run", "no-such-thing", "--stack-env", str(tmp_path / ".env")])

    assert result.exit_code == 2
    assert "unknown experiment 'no-such-thing'" in result.output
    assert "pos-offline-sync" in result.output


def test_run_explains_a_missing_stack(tmp_path: Path) -> None:
    result = runner.invoke(app, ["run", "pos-offline-sync", "--stack-env", str(tmp_path / ".env")])

    assert result.exit_code == 2
    assert "make ref-up" in result.output


def test_run_refuses_a_lag_window_on_naive(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["run", "pos-offline-sync", "--lag-days", "3", "--stack-env", str(tmp_path / ".env")],
    )

    assert result.exit_code == 2
    assert "--target hardened only" in result.output


def test_run_rejects_an_unknown_target(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        ["run", "pos-offline-sync", "--target", "bulletproof", "--stack-env", str(tmp_path)],
    )

    assert result.exit_code != 0


def test_run_accepts_a_lag_window_on_hardened(tmp_path: Path) -> None:
    # Gets as far as looking for the stack, so the options themselves were accepted.
    result = runner.invoke(
        app,
        [
            "run",
            "pos-offline-sync",
            "--target",
            "hardened",
            "--lag-days",
            "1",
            "--stack-env",
            str(tmp_path / ".env"),
        ],
    )

    assert result.exit_code == 2
    assert "make ref-up" in result.output


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
