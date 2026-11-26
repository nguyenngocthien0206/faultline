"""Command line entry point.

Output is plain ASCII on purpose: the default Windows console code page cannot
print Vietnamese, and a crash while printing a summary helps nobody.
"""

import threading
import time
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from faultline import __version__

app = typer.Typer(
    name="faultline",
    help="Chaos and invariant testing for data platforms.",
    no_args_is_help=True,
    add_completion=False,
)


class ProfileName(StrEnum):
    tiny = "tiny"
    demo = "demo"
    large = "large"


def _print_version(value: bool) -> None:
    if value:
        typer.echo(f"faultline {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the faultline version and exit.",
            callback=_print_version,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Inject faults around a data platform and check that it keeps its promises."""


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    raise AssertionError("unreachable")


@app.command()
def generate(
    out: Annotated[Path, typer.Option("--out", help="Output directory (must be empty or absent).")],
    profile: Annotated[ProfileName, typer.Option(help="Volume profile.")] = ProfileName.demo,
    seed: Annotated[int | None, typer.Option(help="Random seed.")] = None,
    start_date: Annotated[
        datetime | None, typer.Option(formats=["%Y-%m-%d"], help="First day (YYYY-MM-DD).")
    ] = None,
    days: Annotated[int | None, typer.Option(min=1, help="Number of days.")] = None,
    stores: Annotated[int | None, typer.Option(min=1, help="Number of stores.")] = None,
    orders_per_store_day: Annotated[
        int | None, typer.Option(min=1, help="Average orders per store per day.")
    ] = None,
    members: Annotated[int | None, typer.Option(min=1, help="Member base size.")] = None,
    workers: Annotated[
        int | None, typer.Option(min=1, help="Worker processes (default: CPU count).")
    ] = None,
) -> None:
    """Generate a seeded coffee chain dataset as landing files plus truth."""
    from faultline.datagen import (
        DatasetSpec,
        OutputNotEmptyError,
        estimate_bytes,
        estimate_orders,
    )
    from faultline.datagen import generate as run_generation

    spec = DatasetSpec.from_profile(
        profile.value,
        seed=seed,
        start_date=start_date.date() if start_date else None,
        days=days,
        stores=stores,
        orders_per_store_day=orders_per_store_day,
        members=members,
    )
    typer.echo(
        f"Generating profile={spec.profile} seed={spec.seed} "
        f"{spec.start_date}..{spec.end_date} ({spec.days} days, {spec.stores} stores)"
    )
    typer.echo(
        f"Estimate: ~{estimate_orders(spec):,} orders, ~{_human_bytes(estimate_bytes(spec))}"
    )
    try:
        report = run_generation(spec, out, workers=workers)
    except OutputNotEmptyError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    width = max(len(name) for name in report.row_counts)
    for name, count in report.row_counts.items():
        typer.echo(f"  {name:<{width}}  {count:>12,}")
    typer.echo(f"Wrote {report.files:,} files to {out} in {report.elapsed_seconds:.1f}s")


class ReportFormat(StrEnum):
    md = "md"
    json = "json"


@app.command()
def run(
    experiment: Annotated[
        str | None,
        typer.Argument(help="Experiment spec: a name in experiments/, or a path."),
    ] = None,
    all_experiments: Annotated[
        bool, typer.Option("--all", help="Every experiment in experiments/.")
    ] = False,
    target: Annotated[
        list[str] | None,
        typer.Option(
            "--target",
            help="Target spec: a name in targets/, or a path. Repeat to run on several.",
        ),
    ] = None,
    profile: Annotated[
        ProfileName | None, typer.Option(help="Dataset volume profile (default: the spec's).")
    ] = None,
    parallel: Annotated[
        int, typer.Option(min=1, help="With --all: experiments running at the same time.")
    ] = 8,
    work_dir: Annotated[
        Path, typer.Option(help="Parent folder for this run's data and snapshots.")
    ] = Path(".faultline/runs"),
    results_dir: Annotated[
        Path, typer.Option(help="Parent folder for this run's result files.")
    ] = Path(".faultline/results"),
) -> None:
    """Run an experiment (or --all of them) on one or more targets and print the verdicts.

    Targets must declare `environment: test` and `allow_destructive: true`;
    nothing overrides that. The default target is reference-naive. Every
    result is saved as JSON under the results folder, for `faultline report`.
    With --all, each target's clean run is shared by every experiment and the
    experiments run side by side; the scorecard is printed at the end.
    """
    from faultline.outcome import Outcome
    from faultline.report.results import load_results, new_run_id, save_result, save_run
    from faultline.report.scorecard import Scorecard
    from faultline.runner import pair_name, run_all, run_experiment
    from faultline.safety import UnsafeTargetError
    from faultline.spec import (
        EXPERIMENTS_DIR,
        TARGETS_DIR,
        SpecError,
        find_spec,
        load_experiment,
        load_target,
    )

    if all_experiments == (experiment is not None):
        typer.echo("Error: give one experiment, or --all", err=True)
        raise typer.Exit(code=2)
    dataset = {"profile": profile.value if profile else None}
    try:
        if all_experiments:
            paths = sorted(EXPERIMENTS_DIR.glob("*.yaml"))
            if not paths:
                raise SpecError(f"no experiments in {EXPERIMENTS_DIR}/ (run from the repository)")
        else:
            paths = [find_spec(str(experiment), EXPERIMENTS_DIR, "experiment")]
        experiments = [load_experiment(path, dataset=dataset) for path in paths]
        targets = [
            load_target(find_spec(name, TARGETS_DIR, "target")).build()
            for name in (target or ["reference-naive"])
        ]
    except (SpecError, UnsafeTargetError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    run_id = new_run_id()
    results, runs = results_dir / run_id, work_dir / run_id
    errors = 0
    names = [t.name for t in targets]
    save_run(results, experiments=[e.name for e in experiments], targets=names)
    if all_experiments:
        lock = threading.Lock()
        logs = results / "logs"
        logs.mkdir(parents=True, exist_ok=True)

        def log_for(name: str):  # type: ignore[no-untyped-def]
            path = logs / f"{name}.log"

            def write(line: str) -> None:
                with path.open("a", encoding="utf-8") as f:
                    f.write(line + "\n")

            return write

        def on_result(result, seconds: float) -> None:  # type: ignore[no-untyped-def]
            spec = next(e.spec for e in experiments if e.name == result.experiment)
            save_result(result, results, dataset=spec, seconds=seconds)
            with lock:
                minutes, rest = divmod(int(seconds), 60)
                typer.echo(
                    f"{result.experiment} on {result.target}: {result.outcome} "
                    f"({minutes}m{rest:02d}s)"
                )

        n = len(experiments) * len(targets)
        typer.echo(f"run {run_id}: {len(experiments)} experiments x {len(targets)} targets ({n})")
        typer.echo(f"logs in {logs}")
        done = run_all(
            experiments, targets, runs, parallel=parallel, log_for=log_for, on_result=on_result
        )
        errors = sum(r.outcome is Outcome.ERROR for r in done)
        typer.echo("")
        card = Scorecard.from_records(run_id, load_results(results), targets=names)
        typer.echo(card.to_markdown())
    else:
        (spec,) = experiments
        for chosen in targets:
            started = time.monotonic()
            result = run_experiment(
                spec, chosen, runs / pair_name(spec.name, chosen.name), log=typer.echo
            )
            saved = save_result(
                result, results, dataset=spec.spec, seconds=time.monotonic() - started
            )
            typer.echo("")
            typer.echo(result.render())
            typer.echo(f"Result:     {saved}")
            errors += result.outcome is Outcome.ERROR
    typer.echo(f"Results:    {results} (faultline report {run_id})")
    if errors:
        raise typer.Exit(code=1)


@app.command()
def report(
    run_id: Annotated[
        str | None, typer.Argument(help="A run in the results folder (default: the latest).")
    ] = None,
    output: Annotated[ReportFormat, typer.Option("--format", help="md or json.")] = (
        ReportFormat.md
    ),
    expect: Annotated[
        Path | None,
        typer.Option(help="Expected verdicts (scorecard/expected.yaml); exit 1 on a mismatch."),
    ] = None,
    results_dir: Annotated[
        Path, typer.Option(help="Parent folder of the runs' result files.")
    ] = Path(".faultline/results"),
) -> None:
    """Print a run's scorecard, and check it against expected verdicts."""
    import json

    from faultline.report.results import latest_run, load_results, load_run
    from faultline.report.scorecard import ExpectationError, Scorecard, check, load_expected

    folder = results_dir / run_id if run_id else latest_run(results_dir)
    records = load_results(folder) if folder is not None and folder.is_dir() else []
    if not records:
        where = folder if folder is not None else results_dir
        typer.echo(f"Error: no results in {where}; run `faultline run` first", err=True)
        raise typer.Exit(code=2)
    assert folder is not None  # records came from it
    run_info = load_run(folder) or {}
    card = Scorecard.from_records(folder.name, records, targets=run_info.get("targets", ()))
    if output is ReportFormat.json:
        typer.echo(json.dumps(card.to_json(), indent=2))
    else:
        typer.echo(card.to_markdown(), nl=False)
    if expect is None:
        return
    try:
        outcome = check(card, load_expected(expect))
    except ExpectationError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc
    for line in outcome.unexpected:
        typer.echo(f"note: {line}", err=True)
    if not outcome.passed:
        typer.echo(f"Not as expected ({expect}):", err=True)
        for line in outcome.mismatches:
            typer.echo(f"  - {line}", err=True)
        raise typer.Exit(code=1)
    covered = len(card.cells) - len(outcome.unexpected)
    typer.echo(
        f"As expected: {covered} of {len(card.cells)} cells checked against {expect}", err=True
    )


@app.command()
def clean(
    stack_env: Annotated[
        Path, typer.Option(help="Env file of the reference stack (created by `make ref-up`).")
    ] = Path("reference/.env"),
    force: Annotated[
        bool, typer.Option(help="Clean even if the stack was used in the last ten minutes.")
    ] = False,
) -> None:
    """Delete leftover sandboxes (fl_*) from the local reference stack.

    A sandbox removes itself when an experiment ends; this sweeps up the ones
    a crash or an interrupted test left in the bucket, the source or the warehouse.
    """
    from faultline.adapters.reference import (
        StackBusyError,
        clean_reference_stack,
        config_from_env_file,
    )

    if not stack_env.exists():
        typer.echo(f"Error: {stack_env} not found; start the stack with `make ref-up`", err=True)
        raise typer.Exit(code=2)
    try:
        report = clean_reference_stack(config_from_env_file(stack_env), force=force)
    except StackBusyError as exc:
        typer.echo(f"Not cleaning: {exc}. Wait until nothing runs, or pass --force.", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(
        f"Removed {len(report.workspaces)} leftover sandboxes: "
        f"{len(report.landing)} landing prefixes, {len(report.source)} source schemas, "
        f"{len(report.warehouse)} warehouse databases"
    )
    if report.unmarked:
        typer.echo(f"Left alone, not marked as faultline sandboxes: {', '.join(report.unmarked)}")
