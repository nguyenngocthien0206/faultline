"""The reference adapter's choice of job and run config, without a stack.

Building an adapter opens no connection, so these need no Docker.
"""

from datetime import date
from pathlib import Path

import pytest

from faultline.adapters.base import AdapterError, Capability, Stage
from faultline.adapters.reference import (
    HARDENED_EXTRACT_OP,
    Edition,
    ReferenceAdapter,
    ReferenceStackConfig,
    target_name,
)

DAY = date(2026, 11, 2)
CONFIG = ReferenceStackConfig(
    source_dsn="postgresql://user:secret@127.0.0.1:5433/db",
    clickhouse_user="user",
    clickhouse_password="secret",
)


def _adapter(**kwargs: object) -> ReferenceAdapter:
    return ReferenceAdapter(CONFIG, Path("generated"), workspace="fl_0123abcd", **kwargs)


def test_naive_is_the_default_edition() -> None:
    adapter = _adapter()

    assert adapter.edition is Edition.NAIVE
    assert adapter.job == "pos_daily"
    assert adapter.target == "reference-naive"
    assert adapter.run_config() == {"resources": {"workspace": {"config": {"name": "fl_0123abcd"}}}}


def test_hardened_runs_its_own_job_with_the_platform_default_lag() -> None:
    adapter = _adapter(edition="hardened")

    assert adapter.job == "pos_daily_hardened"
    assert adapter.target == "reference-hardened"
    assert "ops" not in adapter.run_config()  # the platform's own default applies


def test_a_lag_set_by_faultline_goes_into_every_run_and_into_the_target_name() -> None:
    adapter = _adapter(edition=Edition.HARDENED, lag_days=1)

    assert adapter.run_config()["ops"] == {HARDENED_EXTRACT_OP: {"config": {"lag_days": 1}}}
    assert adapter.target == "reference-hardened(lag=1d)"


def test_naive_has_no_lag_window_to_set() -> None:
    with pytest.raises(ValueError, match="only the hardened edition"):
        _adapter(edition="naive", lag_days=3)


def test_a_negative_lag_is_refused() -> None:
    with pytest.raises(ValueError, match="0 or more"):
        _adapter(edition="hardened", lag_days=-1)


def test_an_unknown_edition_is_refused() -> None:
    with pytest.raises(ValueError, match="bulletproof"):
        _adapter(edition="bulletproof")


def test_target_names() -> None:
    assert target_name("naive") == "reference-naive"
    assert target_name(Edition.HARDENED, 0) == "reference-hardened(lag=0d)"


def test_sandboxes_are_isolated_enough_to_run_side_by_side() -> None:
    assert _adapter().capabilities().supports(Capability.CONCURRENT_SANDBOXES)


def test_landing_files_are_offered() -> None:
    assert _adapter().capabilities().supports(Capability.LANDING_FILES)


def test_landing_files_map_to_keys_inside_the_workspace() -> None:
    adapter = _adapter()

    assert adapter.landing_key("pos/orders", DAY, "a.parquet") == (
        "fl_0123abcd/pos/orders/dt=2026-11-02/a.parquet"
    )
    assert adapter.landing_key("pos/payments", DAY) == "fl_0123abcd/pos/payments/dt=2026-11-02/"


@pytest.mark.parametrize(
    "file",
    ["..", ".", "../../fl_99999999/pos/orders/x.parquet", "dt=2026-11-03/x.parquet", "a\\b"],
)
def test_a_landing_file_name_cannot_leave_its_folder(file: str) -> None:
    with pytest.raises(AdapterError, match="refusing landing file name"):
        _adapter().landing_key("pos/orders", DAY, file)


def test_an_undeclared_dataset_has_no_landing_files() -> None:
    adapter = _adapter()

    with pytest.raises(AdapterError, match="no landing files for 'crm/members'"):
        adapter.landing_files("crm/members", DAY)
    with pytest.raises(AdapterError, match="no landing files for '_dlt_pipeline_state'"):
        adapter.write_landing_file("_dlt_pipeline_state", DAY, "x.parquet", b"")


@pytest.mark.parametrize(
    ("edition", "stage", "job"),
    [
        ("naive", None, "pos_daily"),
        ("naive", Stage.INGEST, "pos_ingest"),
        ("naive", Stage.LOAD, "pos_load"),
        ("hardened", None, "pos_daily_hardened"),
        ("hardened", Stage.INGEST, "pos_ingest_hardened"),
        ("hardened", Stage.LOAD, "pos_load_hardened"),
    ],
)
def test_each_stage_runs_its_own_job(edition: str, stage: Stage | None, job: str) -> None:
    adapter = _adapter(edition=edition)

    assert adapter.capabilities().supports(Capability.RUN_STAGES)
    assert adapter.job_for(stage) == job


def test_the_lag_window_only_goes_to_jobs_that_extract() -> None:
    adapter = _adapter(edition="hardened", lag_days=2)

    assert "ops" in adapter.run_config(None)
    assert "ops" in adapter.run_config(Stage.INGEST)
    assert "ops" not in adapter.run_config(Stage.LOAD)  # Dagster refuses config for absent ops


def test_deleting_a_landing_file_needs_its_name() -> None:
    with pytest.raises(AdapterError, match="name the landing file"):
        _adapter().delete_landing_file("pos/orders", DAY, "")
