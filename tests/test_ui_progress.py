"""Pace, ETA, and stall arithmetic for per-stage episode progress.

The catalog read is exercised through the server (tests/test_ui_server.py);
these are the judgment calls the numbers rest on.
"""

from datetime import UTC, datetime, timedelta

from hflow.steps import Stage
from hflow.ui._progress import (
    StageCounts,
    StageWindow,
    batch_progress,
    prefer_recorded,
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


class TestBatchProgress:
    """The fallback for a replayed run, where the catalog stays silent."""

    def batch(self, map_index: int, state: str, *, end: str | None = None) -> dict:
        return {
            "task_id": "process_batch",
            "state": state,
            "map_index": map_index,
            "end_date": end,
        }

    def test_episodes_are_apportioned_over_the_finished_batches(self) -> None:
        counts = batch_progress(
            60,
            [
                {"task_id": "plan", "state": "success", "map_index": -1},
                self.batch(0, "success", end="2026-08-24T12:00:00+00:00"),
                self.batch(1, "running"),
                self.batch(2, "queued"),
                self.batch(3, "queued"),
            ],
        )
        assert counts is not None
        # One batch of four finished, so a quarter of the run's episodes.
        assert counts.done == 15
        assert counts.last_completed_at == datetime(2026, 8, 24, 12, 0, tzinfo=UTC)

    def test_a_failed_batch_finished_an_unknown_amount_so_counts_for_nothing(self) -> None:
        # Per-episode errors are caught inside the batch loop, so a batch task
        # that failed died before or during its work -- claiming its whole
        # slice is done would overstate the run badly.
        counts = batch_progress(60, [self.batch(0, "success"), self.batch(1, "failed")])
        assert counts is not None
        assert counts.done == 30

    def test_an_unexpanded_placeholder_is_not_a_finished_batch(self) -> None:
        # A failed plan leaves one map_index -1 instance; reading it as a
        # finished batch would show a stage where nothing ran as complete.
        assert batch_progress(60, [self.batch(-1, "upstream_failed")]) is None

    def test_nothing_finished_yet_is_zero_not_unknown(self) -> None:
        assert batch_progress(60, [self.batch(0, "running")]) == StageCounts()

    def test_before_the_fan_out_exists_there_is_no_answer(self) -> None:
        assert batch_progress(60, [{"task_id": "plan", "state": "running"}]) is None

    def test_an_unknown_episode_count_is_no_answer(self) -> None:
        assert batch_progress(None, [self.batch(0, "success")]) is None


class TestPreferRecorded:
    def test_the_estimate_speaks_only_in_the_catalog_s_silence(self) -> None:
        # The replay case: no appends landed, but batches are through.
        merged = prefer_recorded(StageCounts(), StageCounts(done=6))
        assert merged is not None
        assert merged.done == 6

    def test_a_real_count_is_never_displaced_by_an_estimate(self) -> None:
        # The estimate can overshoot (batches are packed by bytes), so it must
        # not win just by being larger.
        recorded = StageCounts(done=3, quarantined=1, errors=1)
        assert prefer_recorded(recorded, StageCounts(done=6)) == recorded

    def test_the_silent_catalog_still_contributes_what_it_saw(self) -> None:
        recorded = StageCounts(done=0, quarantined=1, errors=1)
        merged = prefer_recorded(recorded, StageCounts(done=6))
        assert merged is not None
        assert (merged.done, merged.quarantined, merged.errors) == (6, 1, 1)

    def test_a_missing_source_is_not_an_answer(self) -> None:
        assert prefer_recorded(None, StageCounts(done=2)) == StageCounts(done=2)
        assert prefer_recorded(StageCounts(done=2), None) == StageCounts(done=2)
        assert prefer_recorded(None, None) is None


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
