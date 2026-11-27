"""Experiment and target specs: YAML a team can read, review in a pull request and change.

An experiment (experiments/<name>.yaml) is plain data: a dataset, a starting
schedule, faults and properties, each named by the `name` of its class and
given its parameters. Days are counted within the dataset (1 is its first day,
`last` its final one), so the same experiment still means the same thing on a
longer profile. Parameters are checked against the class they build: an
unknown key, an unknown type or a value of the wrong kind is an error naming
the file, never silently dropped.

A target (targets/<name>.yaml) names a platform and how to reach it, and must
pass the first layer of the safety guard (faultline.safety) to be read at all.
"""

from __future__ import annotations

import dataclasses
import inspect
import typing
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from faultline.datagen.spec import DatasetSpec
from faultline.faults.base import Fault
from faultline.faults.execution import CrashAfterLanding, CrashMidWrite, RetryAfterPartialCommit
from faultline.faults.landing import ColumnDrift, DuplicateFile, LateFile, TruncatedFile
from faultline.faults.source import PosOfflineSync
from faultline.properties.base import Property
from faultline.properties.core import (
    Atomicity,
    BackfillEquivalence,
    Convergence,
    Idempotency,
    NoDataLoss,
    NoSilentCorruption,
    NoVanishingRows,
)
from faultline.runner import Experiment
from faultline.safety import SAFE_ENVIRONMENT, Target
from faultline.schedule import Schedule, clean_schedule, deliveries_only

if TYPE_CHECKING:
    from faultline.adapters.reference import ReferenceStackConfig

EXPERIMENTS_DIR = Path("experiments")
TARGETS_DIR = Path("targets")

FAULTS: dict[str, type[Fault]] = {
    cls.name: cls
    for cls in (
        PosOfflineSync,
        DuplicateFile,
        LateFile,
        TruncatedFile,
        ColumnDrift,
        CrashAfterLanding,
        CrashMidWrite,
        RetryAfterPartialCommit,
    )
}
PROPERTIES: dict[str, type[Property]] = {
    cls.name: cls
    for cls in (
        NoDataLoss,
        NoSilentCorruption,
        NoVanishingRows,
        Idempotency,
        BackfillEquivalence,
        Convergence,
        Atomicity,
    )
}
SCHEDULES: dict[str, Callable[[DatasetSpec], Schedule]] = {
    "daily": clean_schedule,  # each day arrives, then runs: the steady state
    "deliveries_only": deliveries_only,  # every day arrives, nothing runs (a history to load)
}


class SpecError(ValueError):
    """A spec that cannot be used, with the file it came from and why."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# -------------------------------------------------------------------- targets


class ReferenceOptions(_Strict):
    edition: Literal["naive", "hardened"] = "naive"
    lag_days: int | None = Field(default=None, ge=0)  # hardened only; None: the platform's own
    env_file: Path = Path("../reference/.env")  # relative to the target file

    @model_validator(mode="after")
    def _lag_is_hardened_only(self) -> ReferenceOptions:
        if self.lag_days is not None and self.edition != "hardened":
            raise ValueError("lag_days applies to the hardened edition only")
        return self


class TargetSpec(_Strict):
    name: str
    adapter: Literal["reference"]
    environment: str
    allow_destructive: bool
    options: ReferenceOptions = ReferenceOptions()

    @field_validator("environment")
    @classmethod
    def _test_only(cls, value: str) -> str:
        if value != SAFE_ENVIRONMENT:
            raise ValueError(
                f"faultline only runs against environment {SAFE_ENVIRONMENT!r}, got {value!r}"
            )
        return value

    @field_validator("allow_destructive")
    @classmethod
    def _destructive_allowed(cls, value: bool) -> bool:
        if value is not True:
            raise ValueError("must be true: faultline deletes and rewrites data by design")
        return value

    def build(self, *, config: ReferenceStackConfig | None = None) -> Target:
        """The Target the runner takes; `config` replaces the env file (tests use it)."""
        from faultline.adapters.reference import ReferenceAdapter, config_from_env_file

        if config is None:
            if not self.options.env_file.exists():
                raise SpecError(
                    f"target {self.name!r}: {self.options.env_file} not found; "
                    "start the stack with `make ref-up`"
                )
            config = config_from_env_file(self.options.env_file)
        edition, lag_days = self.options.edition, self.options.lag_days
        return Target(
            name=self.name,
            environment=self.environment,
            allow_destructive=self.allow_destructive,
            sandbox=lambda root: ReferenceAdapter(config, root, edition=edition, lag_days=lag_days),
        )


def load_target(path: Path) -> TargetSpec:
    path = Path(path)
    spec = _validated(TargetSpec, path)
    _same_name(spec.name, path)
    env_file = spec.options.env_file
    if not env_file.is_absolute():  # a target file means paths next to it, wherever faultline runs
        options = spec.options.model_copy(update={"env_file": path.parent / env_file})
        spec = spec.model_copy(update={"options": options})
    return spec


# ---------------------------------------------------------------- experiments


class DatasetSection(_Strict):
    profile: Literal["tiny", "demo", "large"] = "tiny"
    seed: int | None = None
    start_date: date | None = None
    days: int | None = Field(default=None, ge=1)
    stores: int | None = Field(default=None, ge=1)
    orders_per_store_day: int | None = Field(default=None, ge=1)
    members: int | None = Field(default=None, ge=1)


class ExperimentSpec(_Strict):
    name: str
    description: str
    dataset: DatasetSection = DatasetSection()
    schedule: Literal["daily", "deliveries_only"] = "daily"
    faults: list[str | dict[str, Any]] = []
    properties: list[str | dict[str, Any]] = Field(min_length=1)
    recovery_rounds: int = Field(default=3, ge=1)  # at most; recovery stops once it matches


def load_experiment(path: Path, *, dataset: Mapping[str, Any] | None = None) -> Experiment:
    """Read an experiment spec; `dataset` overrides its dataset section (the CLI's --profile)."""
    path = Path(path)
    spec = _validated(ExperimentSpec, path)
    _same_name(spec.name, path)
    section = spec.dataset.model_dump()
    section.update({k: v for k, v in (dataset or {}).items() if v is not None})
    try:
        data = DatasetSpec.from_profile(section.pop("profile"), **section)
    except (TypeError, ValueError) as exc:
        raise SpecError(f"{path}: dataset: {exc}") from exc
    faults = tuple(
        _build("fault", FAULTS, entry, data, f"{path}: faults[{i}]")
        for i, entry in enumerate(spec.faults)
    )
    properties = tuple(
        _build("property", PROPERTIES, entry, data, f"{path}: properties[{i}]")
        for i, entry in enumerate(spec.properties)
    )
    return Experiment(
        name=spec.name,
        description=" ".join(spec.description.split()),
        spec=data,
        faults=faults,  # type: ignore[arg-type]
        properties=properties,  # type: ignore[arg-type]
        recovery_rounds=spec.recovery_rounds,
        schedule=SCHEDULES[spec.schedule],
    )


def find_spec(name_or_path: str, folder: Path, kind: str) -> Path:
    """A spec file given by path, or by name in `folder` (experiments/, targets/)."""
    given = Path(name_or_path)
    if given.suffix in (".yaml", ".yml") or given.parent != Path("."):
        if not given.is_file():
            raise SpecError(f"{kind} spec {given} not found")
        return given
    candidate = folder / f"{name_or_path}.yaml"
    if candidate.is_file():
        return candidate
    known = sorted(f.stem for f in folder.glob("*.yaml")) if folder.is_dir() else []
    listing = ", ".join(known) if known else f"none found in {folder}/ (run from the repository)"
    raise SpecError(f"no {kind} named {name_or_path!r}; known: {listing}")


# -------------------------------------------------------------------- helpers


def day(value: object, dataset: DatasetSpec, where: str) -> date:
    """A day counted within the dataset: 1 is its first day, `last` its final one."""
    if value == "last":
        number = dataset.days
    elif isinstance(value, int) and not isinstance(value, bool):
        number = value
    else:
        raise SpecError(f"{where}: a day is a number counted from 1, or 'last'; got {value!r}")
    if not 1 <= number <= dataset.days:
        raise SpecError(f"{where}: day {number} is outside a {dataset.days}-day dataset")
    return dataset.start_date + timedelta(days=number - 1)


def _build(
    kind: str,
    registry: Mapping[str, type],
    entry: str | dict[str, Any],
    dataset: DatasetSpec,
    where: str,
) -> Any:
    params = {"type": entry} if isinstance(entry, str) else dict(entry)
    name = params.pop("type", None)
    if name not in registry:
        known = ", ".join(sorted(registry))
        raise SpecError(f"{where}: unknown {kind} type {name!r}; known: {known}")
    cls = registry[name]
    accepted = _parameters(cls)
    unknown = sorted(set(params) - set(accepted))
    if unknown:
        raise SpecError(
            f"{where}: {name} has no parameter {', '.join(unknown)}; "
            f"it takes {', '.join(accepted) or 'none'}"
        )
    missing = [p for p, (_, required) in accepted.items() if required and p not in params]
    if missing:
        raise SpecError(f"{where}: {name} needs {', '.join(missing)}")
    kwargs = {}
    for param, value in params.items():
        hint = accepted[param][0]
        if hint is date:
            kwargs[param] = day(value, dataset, f"{where}.{param}")
            continue
        try:
            kwargs[param] = TypeAdapter(hint).validate_python(value)
        except ValidationError as exc:
            reason = exc.errors()[0]["msg"]
            raise SpecError(f"{where}.{param}: {reason} (got {value!r})") from exc
    return cls(**kwargs)


def _parameters(cls: type) -> dict[str, tuple[Any, bool]]:
    """Each constructor parameter: its type and whether it is required."""
    hints = typing.get_type_hints(cls if dataclasses.is_dataclass(cls) else cls.__init__)
    return {
        name: (hints[name], p.default is inspect.Parameter.empty)
        for name, p in inspect.signature(cls).parameters.items()
    }


def _validated(model: type[_Strict], path: Path) -> Any:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise SpecError(f"{path}: cannot read: {exc}") from exc
    except yaml.YAMLError as exc:
        raise SpecError(f"{path}: not valid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: expected a mapping at the top level")
    try:
        return model.model_validate(raw)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in e['loc']) or 'spec'}: {e['msg']}" for e in exc.errors()
        )
        raise SpecError(f"{path}: {problems}") from exc


def _same_name(name: str, path: Path) -> None:
    if name != path.stem:
        raise SpecError(f"{path}: name {name!r} does not match the file name {path.stem!r}")
