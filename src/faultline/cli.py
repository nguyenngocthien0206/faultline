"""Command line entry point.

Output is plain ASCII on purpose: the default Windows console code page cannot
print Vietnamese, and a crash while printing a summary helps nobody.
"""

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
