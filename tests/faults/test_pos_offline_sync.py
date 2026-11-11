from datetime import date
from pathlib import Path

import pytest

from faultline.datagen import DatasetSpec, generate
from faultline.faults.source import FaultSetupError, PosOfflineSync
from faultline.schedule import Deliver, Run, clean_schedule
from faultline.sources.pos import day_files

TINY = DatasetSpec.from_profile("tiny")
D2, D3, D4 = date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)


@pytest.fixture
def input_root(tmp_path: Path) -> Path:
    generate(TINY, tmp_path, workers=1)
    return tmp_path


def _stores(root: Path, table: str, day: date) -> set[str]:
    return {p.parent.name for p in day_files(root, table, day)}


def test_clean_schedule_delivers_then_runs_every_day() -> None:
    schedule = clean_schedule(TINY)

    assert schedule[:4] == (Deliver(TINY.start_date), Run(TINY.start_date), Deliver(D2), Run(D2))
    assert len(schedule) == 2 * TINY.days


def test_offline_days_move_to_the_sync_day(input_root: Path) -> None:
    fault = PosOfflineSync(offline_from=D2, sync_on=D4)
    before = {d: _stores(input_root, "orders", d) for d in (D2, D3, D4)}

    fault.prepare_input(input_root)

    store = fault.store
    assert store is not None
    for table in ("orders", "order_items", "payments"):
        assert f"store={store}" not in _stores(input_root, table, D2)
        assert f"store={store}" not in _stores(input_root, table, D3)
        assert {f"store={store}-offline-2026-11-02", f"store={store}-offline-2026-11-03"} <= (
            _stores(input_root, table, D4)
        )
    assert len(_stores(input_root, "orders", D4)) == len(before[D4]) + 2


def test_store_choice_is_deterministic(
    input_root: Path, tmp_path_factory: pytest.TempPathFactory
) -> None:
    other = tmp_path_factory.mktemp("again")
    generate(TINY, other, workers=1)
    a, b = PosOfflineSync(D2, D4), PosOfflineSync(D2, D4)

    a.prepare_input(input_root)
    b.prepare_input(other)

    assert a.store == b.store
    assert (
        a.store
        == sorted(
            {
                p.name.removeprefix("store=")
                for p in (input_root / "landing/pos/orders/dt=2026-11-04").iterdir()
            }
            - {f"{a.store}-offline-2026-11-02", f"{a.store}-offline-2026-11-03"}
        )[0]
    )


def test_affected_partitions_and_description() -> None:
    fault = PosOfflineSync(D2, D4, store="HN-001")

    assert fault.affected_partitions() == {D2, D3, D4}
    assert fault.describe() == (
        "POS of HN-001 offline on 2026-11-02, 2026-11-03; its rows reach the source on 2026-11-04"
    )
    assert fault.apply(clean_schedule(TINY)) == clean_schedule(TINY)
    assert fault.heal() == []


def test_unknown_store_is_a_setup_error(input_root: Path) -> None:
    with pytest.raises(FaultSetupError, match="NOPE-999"):
        PosOfflineSync(D2, D4, store="NOPE-999").prepare_input(input_root)


def test_sync_must_come_after_going_offline() -> None:
    with pytest.raises(FaultSetupError, match="after"):
        PosOfflineSync(D4, D2)
