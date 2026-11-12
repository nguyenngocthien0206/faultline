"""The adapter interface and the value types it speaks.

An adapter declares what it can do (`Capabilities`) instead of failing on what
it cannot: a fault or property that needs a missing capability is reported as
SKIPPED, never as a platform failure.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from pathlib import Path
from types import TracebackType


class AdapterError(Exception):
    """The adapter could not perform an operation (as opposed to the platform failing a run)."""


class Capability(StrEnum):
    DELIVER_DAY = "input.deliver_day"  # make one day of input available to the platform
    RUN_PARTITION = "run.partition"  # run the pipeline for one partition
    SNAPSHOT = "output.snapshot"  # export a consumer-visible table to Parquet
    # Several sandboxes of this adapter can run at the same time without touching
    # each other. The runner then runs the baseline and the fault phase side by
    # side; without it, one after the other (a single shared test environment).
    CONCURRENT_SANDBOXES = "sandbox.concurrent"
    # List, read and write the files ingestion leaves for the warehouse (the
    # landing zone), so landing faults can deliver a file twice, late or broken.
    LANDING_FILES = "landing.files"


@dataclass(frozen=True)
class TableContract:
    """What faultline needs to know about one consumer-visible table."""

    name: str
    key: tuple[str, ...]
    measures: tuple[str, ...] = ()  # numeric columns whose totals are worth reporting
    ignore: tuple[str, ...] = ()  # volatile columns left out of comparisons
    # Generated dataset ("source/dataset") whose keys must all appear in this table.
    truth_dataset: str | None = None


@dataclass(frozen=True)
class Capabilities:
    features: frozenset[Capability]
    tables: tuple[TableContract, ...] = ()

    def supports(self, capability: Capability) -> bool:
        return capability in self.features

    def table(self, name: str) -> TableContract:
        for contract in self.tables:
            if contract.name == name:
                return contract
        raise KeyError(f"no consumer table named {name!r}")


class RunStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"  # the platform's own failure: a signal
    INTERRUPTED = "INTERRUPTED"  # killed by faultline on purpose: not a signal


@dataclass(frozen=True)
class RunRequest:
    partition: date


@dataclass(frozen=True)
class RunResult:
    status: RunStatus
    errors: tuple[str, ...] = ()
    run_id: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return self.status is RunStatus.SUCCEEDED


class Adapter(ABC):
    """One isolated sandbox on one platform. Use as a context manager."""

    @abstractmethod
    def capabilities(self) -> Capabilities: ...

    @abstractmethod
    def setup(self) -> None:
        """Create a fresh, isolated sandbox for one experiment."""

    @abstractmethod
    def deliver(self, day: date) -> None:
        """Make day `day`'s input available, the way the platform normally receives it."""

    @abstractmethod
    def run(self, request: RunRequest) -> RunResult:
        """Run the pipeline for one partition and wait for the outcome."""

    @abstractmethod
    def snapshot(self, table: str, dest: Path) -> Path:
        """Export a consumer-visible table to a Parquet file at `dest`."""

    @abstractmethod
    def teardown(self) -> None:
        """Remove everything `setup` created. Safe to call more than once."""

    # Optional, with Capability.LANDING_FILES. A file is addressed by dataset
    # (named as in the generated data, "pos/order_items"), partition and bare
    # file name; where that lives is the adapter's business, so a fault never
    # needs to know the platform's layout.

    def landing_files(self, dataset: str, partition: date) -> list[str]:
        """File names (no directories) of `dataset`'s landing files for one partition, sorted."""
        raise self._lacks(Capability.LANDING_FILES)

    def read_landing_file(self, dataset: str, partition: date, file: str) -> bytes:
        raise self._lacks(Capability.LANDING_FILES)

    def write_landing_file(self, dataset: str, partition: date, file: str, data: bytes) -> None:
        raise self._lacks(Capability.LANDING_FILES)

    def _lacks(self, capability: Capability) -> AdapterError:
        return AdapterError(f"{type(self).__name__} does not offer {capability}")

    def __enter__(self) -> Adapter:
        self.setup()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.teardown()
