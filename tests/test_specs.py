"""Experiment and target specs: what they accept, what they refuse, and why."""

from datetime import date
from pathlib import Path

import pytest

from faultline.adapters.reference import ReferenceAdapter, ReferenceStackConfig
from faultline.datagen import DatasetSpec
from faultline.faults.landing import LateFile, TruncatedFile
from faultline.properties.core import BackfillEquivalence, Idempotency, NoDataLoss
from faultline.safety import Target, UnsafeTargetError
from faultline.schedule import clean_schedule, deliveries_only
from faultline.spec import (
    FAULTS,
    PROPERTIES,
    SpecError,
    day,
    find_spec,
    load_experiment,
    load_target,
)

ROOT = Path(__file__).resolve().parents[1]
EXPERIMENTS, TARGETS = ROOT / "experiments", ROOT / "targets"
TINY = DatasetSpec.from_profile("tiny")
CONFIG = ReferenceStackConfig(
    source_dsn="postgresql://u:p@127.0.0.1:5433/db", clickhouse_user="u", clickhouse_password="p"
)


def _spec(tmp_path: Path, text: str, name: str = "trial") -> Path:
    path = tmp_path / f"{name}.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def _experiment(tmp_path: Path, body: str) -> Path:
    return _spec(tmp_path, f"name: trial\ndescription: a trial\n{body}")


# ----------------------------------------------------------------- experiments


def test_every_experiment_in_the_repository_loads() -> None:
    names = sorted(p.stem for p in EXPERIMENTS.glob("*.yaml"))

    assert names == [
        "backfill",
        "column-rename",
        "crash-after-landing",
        "crash-mid-write",
        "duplicate-file",
        "idempotency",
        "late-file",
        "pos-offline-sync",
        "retry-after-partial-commit",
        "truncated-file",
    ]
    for name in names:
        experiment = load_experiment(EXPERIMENTS / f"{name}.yaml")
        assert experiment.name == name
        assert experiment.spec == TINY
        assert "  " not in experiment.description  # folded into one line


def test_days_are_counted_in_the_dataset_so_a_longer_profile_keeps_their_meaning() -> None:
    tiny = load_experiment(EXPERIMENTS / "truncated-file.yaml")
    longer = load_experiment(EXPERIMENTS / "truncated-file.yaml", dataset={"days": 7})

    (cut,) = tiny.faults
    (cut_later,) = longer.faults
    assert isinstance(cut, TruncatedFile)
    assert cut.partition == date(2026, 11, 4)  # last, of four days
    assert cut_later.partition == date(2026, 11, 7)  # last, of seven


def test_a_spec_builds_its_faults_properties_and_schedule(tmp_path: Path) -> None:
    path = _experiment(
        tmp_path,
        """\
schedule: deliveries_only
faults:
  - type: late_file
    partition: 2
    lands_before: 4
    datasets: [pos/payments]
properties:
  - no_data_loss
  - {type: backfill_equivalence, start: 1, end: last}
recovery_rounds: 2
""",
    )

    experiment = load_experiment(path)

    assert experiment.faults == (
        LateFile(date(2026, 11, 2), date(2026, 11, 4), datasets=("pos/payments",)),
    )
    first, second = experiment.properties
    assert isinstance(first, NoDataLoss)
    assert isinstance(second, BackfillEquivalence)
    assert (second.start, second.end) == (date(2026, 11, 1), date(2026, 11, 4))
    assert experiment.schedule is deliveries_only
    assert experiment.recovery_rounds == 2


def test_the_steady_state_is_the_default_schedule(tmp_path: Path) -> None:
    experiment = load_experiment(_experiment(tmp_path, "properties: [no_data_loss]\n"))

    assert experiment.schedule is clean_schedule
    assert experiment.faults == ()


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (5, "day 5 is outside a 4-day dataset"),
        (0, "day 0 is outside a 4-day dataset"),
        ("2", "a day is a number counted from 1, or 'last'"),
        (True, "a day is a number counted from 1, or 'last'"),
        ("2026-11-02", "a day is a number counted from 1, or 'last'"),
    ],
)
def test_a_day_outside_the_dataset_or_of_another_kind_is_refused(
    value: object, message: str
) -> None:
    with pytest.raises(SpecError, match=message):
        day(value, TINY, "faults[0].partition")


def test_last_and_first_days() -> None:
    assert day(1, TINY, "x") == date(2026, 11, 1)
    assert day("last", TINY, "x") == date(2026, 11, 4)


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            "faults: [{type: meteor_strike}]\nproperties: [no_data_loss]\n",
            "unknown fault type 'meteor_strike'",
        ),
        ("properties: [convergence]\n", "unknown property type 'convergence'"),
        (
            "faults: [{type: truncated_file, partition: 2, keep: 0.5, speed: 3}]\n"
            "properties: [no_data_loss]\n",
            "truncated_file has no parameter speed; it takes partition, keep, datasets",
        ),
        (
            "faults: [{type: late_file, partition: 2}]\nproperties: [no_data_loss]\n",
            "late_file needs lands_before",
        ),
        (
            "faults: [{type: truncated_file, partition: 2, keep: lots}]\n"
            "properties: [no_data_loss]\n",
            r"faults\[0\]\.keep: Input should be a valid number",
        ),
        ("properties: [{type: idempotency}]\n", "idempotency needs rerun"),
        ("properties: []\n", "properties"),
        ("properties: [no_data_loss]\nsteady_state: {tables: [x]}\n", "steady_state: Extra inputs"),
        ("properties: [no_data_loss]\ndataset: {profile: huge}\n", "dataset.profile"),
        ("properties: [no_data_loss]\ndataset: {days: 0}\n", "dataset.days"),
        ("properties: [no_data_loss]\nschedule: weekly\n", "schedule"),
    ],
)
def test_a_spec_it_cannot_use_is_refused_with_the_reason(
    body: str, message: str, tmp_path: Path
) -> None:
    with pytest.raises(SpecError, match=message):
        load_experiment(_experiment(tmp_path, body))


def test_an_error_names_the_file_it_came_from(tmp_path: Path) -> None:
    path = _experiment(
        tmp_path,
        "faults: [{type: late_file, partition: 9, lands_before: 4}]\nproperties: [no_data_loss]\n",
    )

    with pytest.raises(SpecError) as caught:
        load_experiment(path)

    assert str(caught.value).startswith(f"{path}: faults[0].partition: day 9 is outside")


def test_the_name_must_match_the_file(tmp_path: Path) -> None:
    path = _spec(tmp_path, "name: other\ndescription: x\nproperties: [no_data_loss]\n")

    with pytest.raises(SpecError, match="name 'other' does not match the file name 'trial'"):
        load_experiment(path)


def test_a_file_that_is_not_yaml_or_not_a_mapping_is_refused(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="not valid YAML"):
        load_experiment(_spec(tmp_path, "name: [unclosed\n"))
    with pytest.raises(SpecError, match="expected a mapping"):
        load_experiment(_spec(tmp_path, "- just\n- a list\n"))


def test_every_fault_and_property_class_is_registered_under_its_own_name() -> None:
    assert all(cls.name == name for name, cls in FAULTS.items())
    assert all(cls.name == name for name, cls in PROPERTIES.items())
    assert {"crash_mid_write", "retry_after_partial_commit"} <= set(FAULTS)
    assert PROPERTIES["idempotency"] is Idempotency


# --------------------------------------------------------------------- targets


def test_every_target_in_the_repository_loads_and_builds() -> None:
    names = sorted(p.stem for p in TARGETS.glob("*.yaml"))

    assert names == ["reference-hardened", "reference-hardened-lag1", "reference-naive"]
    for name in names:
        target = load_target(TARGETS / f"{name}.yaml").build(config=CONFIG)
        assert isinstance(target, Target)
        assert target.name == name


def test_a_target_builds_sandboxes_of_its_edition_and_lag_window(tmp_path: Path) -> None:
    target = load_target(TARGETS / "reference-hardened-lag1.yaml").build(config=CONFIG)

    sandbox = target.sandbox(tmp_path)

    assert isinstance(sandbox, ReferenceAdapter)
    assert (sandbox.edition, sandbox.lag_days) == ("hardened", 1)


def test_a_targets_env_file_is_found_next_to_it_wherever_faultline_runs() -> None:
    spec = load_target(TARGETS / "reference-naive.yaml")

    assert spec.options.env_file == TARGETS / "../reference/.env"


TARGET = "name: trial\nadapter: reference\n"


@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            "environment: production\nallow_destructive: true\n",
            "faultline only runs against environment 'test', got 'production'",
        ),
        ("allow_destructive: true\n", "environment: Field required"),
        (
            "environment: test\nallow_destructive: false\n",
            "allow_destructive: Value error, must be true",
        ),
        ("environment: test\n", "allow_destructive: Field required"),
        (
            "environment: test\nallow_destructive: true\noptions: {edition: naive, lag_days: 2}\n",
            "lag_days applies to the hardened edition only",
        ),
        ("environment: test\nallow_destructive: true\noptions: {mode: naive}\n", "options.mode"),
    ],
)
def test_the_first_layer_of_the_guard_refuses_an_unsafe_or_unclear_target(
    body: str, message: str, tmp_path: Path
) -> None:
    with pytest.raises(SpecError, match=message):
        load_target(_spec(tmp_path, TARGET + body))


def test_a_target_without_its_stack_explains_how_to_start_it(tmp_path: Path) -> None:
    spec = load_target(_spec(tmp_path, TARGET + "environment: test\nallow_destructive: true\n"))

    with pytest.raises(SpecError, match="start the stack with `make ref-up`"):
        spec.build()


def test_a_target_built_in_code_meets_the_same_guard() -> None:
    with pytest.raises(UnsafeTargetError, match="environment 'staging'"):
        Target("prod-ish", environment="staging", allow_destructive=True, sandbox=print)  # type: ignore[arg-type]
    with pytest.raises(UnsafeTargetError, match="allow_destructive: true"):
        Target("careful", environment="test", allow_destructive=False, sandbox=print)  # type: ignore[arg-type]


# ----------------------------------------------------------------- find_spec


def test_a_spec_is_found_by_name_or_by_path(tmp_path: Path) -> None:
    by_name = find_spec("late-file", EXPERIMENTS, "experiment")
    by_path = find_spec(str(EXPERIMENTS / "late-file.yaml"), EXPERIMENTS, "experiment")

    assert by_name == by_path == EXPERIMENTS / "late-file.yaml"
    with pytest.raises(SpecError, match=r"experiment spec .*nowhere\.yaml not found"):
        find_spec(str(tmp_path / "nowhere.yaml"), EXPERIMENTS, "experiment")
    with pytest.raises(SpecError, match="none found in"):
        find_spec("late-file", tmp_path / "empty", "experiment")
