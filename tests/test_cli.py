from pathlib import Path

from typer.testing import CliRunner

from faultline import __version__
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
