"""The reference adapter's choice of job and run config, without a stack.

Building an adapter opens no connection, so these need no Docker.
"""

from pathlib import Path

import pytest

from faultline.adapters.reference import (
    HARDENED_EXTRACT_OP,
    Edition,
    ReferenceAdapter,
    ReferenceStackConfig,
    target_name,
)

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
