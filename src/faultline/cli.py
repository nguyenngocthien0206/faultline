"""Command line entry point.

Only `--version` exists for now. The `run` and `report` commands are added
once experiments can be executed.
"""

from typing import Annotated

import typer

from faultline import __version__

app = typer.Typer(
    name="faultline",
    help="Chaos and invariant testing for data platforms.",
    no_args_is_help=True,
    add_completion=False,
)


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
