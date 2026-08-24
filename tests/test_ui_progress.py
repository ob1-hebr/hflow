"""Pace, ETA, and stall arithmetic for per-stage episode progress.

The catalog read is exercised through the server (tests/test_ui_server.py);
these are the judgment calls the numbers rest on.
"""

from datetime import UTC, datetime, timedelta

from hflow.steps import Stage
from hflow.ui._progress import (
    StageCounts,
    StageWindow,
    stage_progress,
    stage_windows_from_instances,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)


def running_window(elapsed_s: float) -> StageWindow:
    return StageWindow(start=NOW - timedelta(seconds=elapsed_s), end=None)


def finished_window(elapsed_s: float, *, ended_s_ago: float = 0.0) -> StageWindow:
    end = NOW - timedelta(seconds=ended_s_ago)
    return StageWindow(start=end - timedelta(seconds=elapsed_s), end=end)


def progress(counts: StageCounts, *, total: int, window: StageWindow, running: bool = True) -> dict:
    return stage_progress(counts, total=total, window=window, running=running, now=NOW)


class TestPaceAndEta:
    def test_a_stage_that_has_finished_nothing_reports_no_pace_or_estimate(self) -> None:
        # 100 episodes to do and two minutes gone by is not yet evidence of
        # anything: an honest "no estimate" beats a fabricated one.
        facts = progress(StageCounts(), total=100, window=running_window(120))
        assert facts["done"] == 0
        assert facts["throughput_eps_per_min"] is None
        assert facts["eta_s"] is None
        assert facts["stalled"] is False

    def test_pace_and_estimate_come_from_the_stage_start(self) -> None:
        # 50 done in 2 minutes: 25/min, and 150 left is 6 more minutes.
        facts = progress(StageCounts(done=50), total=200, window=running_window(120))
        assert facts["throughput_eps_per_min"] == 25.0
        assert facts["eta_s"] == 360.0

    def test_a_finished_stage_reports_its_pace_but_no_estimate(self) -> None:
        facts = progress(StageCounts(done=60), total=60, window=finished_window(120), running=False)
        assert facts["throughput_eps_per_min"] == 30.0
        assert facts["eta_s"] is None

    def test_nothing_left_to_do_means_no_estimate(self) -> None:
        facts = progress(StageCounts(done=10), total=10, window=running_window(60))
        assert facts["eta_s"] is None

    def test_counts_carry_quarantine_and_error_tallies(self) -> None:
        facts = progress(
            StageCounts(done=10, quarantined=2, errors=1), total=10, window=running_window(60)
        )
        assert (facts["quarantined"], facts["errors"]) == (2, 1)


class TestStallDetection:
    def test_a_fast_stage_going_quiet_for_a_minute_is_stalled(self) -> None:
        # 100 episodes in 100s is one per second; a 90s silence is a stall.
        counts = StageCounts(done=100, last_completed_at=NOW - timedelta(seconds=90))
        assert progress(counts, total=200, window=running_window(100))["stalled"] is True

    def test_a_short_quiet_spell_is_not_a_stall(self) -> None:
        counts = StageCounts(done=100, last_completed_at=NOW - timedelta(seconds=30))
        assert progress(counts, total=200, window=running_window(100))["stalled"] is False

    def test_a_slow_stage_is_judged_against_its_own_pace(self) -> None:
        # 2 episodes in 10 minutes: five minutes of quiet is normal here, even
        # though it would be alarming for the fast stage above.
        counts = StageCounts(done=2, last_completed_at=NOW - timedelta(seconds=300))
        assert progress(counts, total=20, window=running_window(600))["stalled"] is False
        long_quiet = StageCounts(done=2, last_completed_at=NOW - timedelta(seconds=1800))
        assert progress(long_quiet, total=20, window=running_window(600))["stalled"] is True

    def test_a_finished_stage_is_never_stalled(self) -> None:
        counts = StageCounts(done=60, last_completed_at=NOW - timedelta(days=2))
        facts = progress(counts, total=60, window=finished_window(120), running=False)
        assert facts["stalled"] is False

    def test_a_stage_with_no_completions_is_never_stalled(self) -> None:
        # An exact replay appends nothing (the pipeline dedupes), so silence
        # here means "no baseline", not "stuck".
        assert progress(StageCounts(), total=20, window=running_window(3600))["stalled"] is False


class TestStageWindows:
    def test_a_running_stage_has_an_open_window(self) -> None:
        windows = stage_windows_from_instances(
            [
                {
                    "task_id": "trigger_sync",
                    "start_date": "2026-08-24T11:58:00+00:00",
                    "end_date": "2026-08-24T11:59:00+00:00",
                },
                {
                    "task_id": "trigger_meta",
                    "start_date": "2026-08-24T11:59:00+00:00",
                    "end_date": None,
                },
            ]
        )
        assert windows[Stage.SYNC].end == datetime(2026, 8, 24, 11, 59, tzinfo=UTC)
        assert windows[Stage.META].end is None
        assert windows[Stage.META].start == datetime(2026, 8, 24, 11, 59, tzinfo=UTC)

    def test_stages_that_never_started_have_no_window(self) -> None:
        windows = stage_windows_from_instances(
            [
                {"task_id": "trigger_labels", "state": "queued", "start_date": None},
                {"task_id": "enabled_labels", "start_date": "2026-08-24T11:59:00+00:00"},
                {"task_id": "resolve_profile", "start_date": "2026-08-24T11:58:00+00:00"},
            ]
        )
        assert windows == {}
