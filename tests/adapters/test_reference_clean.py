"""When the reference stack may be cleaned, without a stack."""

from faultline.adapters.reference import QUIET_SECONDS, CleanReport, busy_reason

NOW = 1_795_500_000.0


def test_a_stack_with_a_run_in_progress_is_busy() -> None:
    assert busy_reason(True, NOW, NOW) == "a Dagster run is in progress"


def test_a_stack_whose_last_run_ended_minutes_ago_is_still_busy() -> None:
    # Between two runs a sandbox has nothing running, yet it is alive.
    reason = busy_reason(False, NOW - 120, NOW)

    assert reason is not None
    assert "2 min ago" in reason


def test_a_stack_quiet_for_the_whole_period_may_be_cleaned() -> None:
    assert busy_reason(False, NOW - QUIET_SECONDS - 1, NOW) is None
    assert busy_reason(False, None, NOW) is None  # no run ever


def test_the_report_counts_each_sandbox_once() -> None:
    report = CleanReport(
        landing=("fl_00000001", "fl_00000002"), source=("fl_00000001",), warehouse=()
    )

    assert report.workspaces == {"fl_00000001", "fl_00000002"}
