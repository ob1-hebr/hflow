"""Per-stage episode progress: how far a run's data has actually moved.

Airflow answers "which task is running"; the operator asks "how many of my
episodes are through, and what is holding up the rest". The pipeline already
records that answer -- every episode a stage finishes appends a catalog row --
so progress here is a read of the Parquet the run itself wrote, with no extra
instrumentation anywhere in the pipeline.

Attribution is (uri set, time window). A run's conf names its source URIs, and
the master triggers stages strictly one after another (each trigger waits for
its sub-DAG), so a stage's trigger task instance bounds exactly the appends
that stage produced. That is exact for one runtime observing one run at a
time; concurrent runs over overlapping URIs would cross-attribute, which the
local dashboard accepts (docs/UI.md records the caveat).

Every timestamp crosses the SQL boundary as epoch seconds: fetching a
TIMESTAMPTZ into Python needs pytz, which is not a dependency.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from hflow.format import CATALOG_FORMAT_VERSION
from hflow.steps import CheckStatus, Stage

# A stage is stalled when nothing has completed for this many times its own
# mean pace, floored so that a fast stage's first quiet second is not an alarm.
_STALL_FLOOR_S = 60.0
_STALL_FACTOR = 5.0


@dataclass(frozen=True)
class StageWindow:
    """When a stage ran: the span its catalog appends must fall inside.

    ``end`` is None while the stage is still running, and the caller closes
    the window at "now" for attribution.
    """

    start: datetime
    end: datetime | None


@dataclass(frozen=True)
class StageCounts:
    """What a stage's catalog appends add up to."""

    done: int = 0
    quarantined: int = 0
    errors: int = 0
    last_completed_at: datetime | None = None


def parse_timestamp(value: object) -> datetime | None:
    """Airflow's ISO timestamps, as aware datetimes; None when unusable."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def iso_timestamp(value: datetime | None) -> str | None:
    return None if value is None else value.astimezone(UTC).isoformat()


def stage_windows_from_instances(instances: list[dict[str, Any]]) -> dict[Stage, StageWindow]:
    """Each stage's run window, from the master's own trigger task instances."""
    starts = {f"trigger_{stage.value}": stage for stage in Stage}
    windows: dict[Stage, StageWindow] = {}
    for instance in instances:
        stage = starts.get(str(instance.get("task_id")))
        if stage is None:
            continue
        start = parse_timestamp(instance.get("start_date"))
        if start is None:
            continue  # queued but not started: nothing could have been appended
        windows[stage] = StageWindow(start=start, end=parse_timestamp(instance.get("end_date")))
    return windows


def _parquet_glob(directory: Path) -> str | None:
    if not directory.is_dir() or not any(directory.glob("*.parquet")):
        return None
    return str(directory / "*.parquet").replace("'", "''")


def _catalog_is_readable(catalog_root: Path) -> bool:
    try:
        found = (catalog_root / "format_version").read_text().strip()
    except OSError:
        return False
    return found == CATALOG_FORMAT_VERSION


def query_stage_counts(
    catalog_root: Path,
    uris: list[str],
    windows: dict[Stage, StageWindow],
    *,
    now: datetime,
) -> dict[Stage, StageCounts] | None:
    """Per-stage append counts for ``uris``, or None when the catalog can't be read.

    Progress is best-effort telemetry: an absent, foreign-version, or corrupt
    catalog means the run page renders without counts, never that it fails.

    Only ``episodes`` and ``check_runs`` are scanned -- deliberately not
    :func:`hflow.curation.open_catalog_connection`, whose measurement pivot
    would be a whole-corpus scan on every poll. If the per-poll cost of the
    Parquet footers ever bites, the fix is one materialized temp table per
    connection, not a narrower question.
    """
    if not _catalog_is_readable(catalog_root):
        return None
    if not uris or not windows:
        return {}
    episodes_glob = _parquet_glob(catalog_root / "episodes")
    if episodes_glob is None:
        return {}  # a catalog exists, but this run has appended nothing yet
    bounds = {
        stage: (window.start.timestamp(), (window.end or now).timestamp())
        for stage, window in windows.items()
    }

    import duckdb

    checks_glob = _parquet_glob(catalog_root / "check_runs")
    error_column = "false"
    join_clause = ""
    if checks_glob is not None:
        error_column = "coalesce(bool_or(c.status = 'error'), false)"
        join_clause = (
            f"LEFT JOIN read_parquet('{checks_glob}', union_by_name=true) c "
            "USING (episode_id, run_fingerprint)"
        )
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"""
            SELECT epoch(e.recorded_at) AS recorded_at,
                   e.quarantined,
                   {error_column} AS had_error
            FROM read_parquet('{episodes_glob}', union_by_name=true) e
            {join_clause}
            WHERE e.source_uri IN (SELECT unnest(?::VARCHAR[]))
              AND epoch(e.recorded_at) >= ?
            GROUP BY e.episode_id, e.run_fingerprint, e.recorded_at, e.quarantined
            """,
            [uris, min(start for start, _ in bounds.values())],
        ).fetchall()
    except (duckdb.Error, OSError):
        return None
    finally:
        connection.close()

    tallies: dict[Stage, StageCounts] = {}
    for recorded_at, quarantined, had_error in rows:
        stage = _stage_for(float(recorded_at), bounds)
        if stage is None:
            continue
        counts = tallies.get(stage, StageCounts())
        completed_at = datetime.fromtimestamp(float(recorded_at), UTC)
        latest = counts.last_completed_at
        tallies[stage] = StageCounts(
            done=counts.done + 1,
            # Error wins over quarantine, exactly as process_batch tallies it.
            quarantined=counts.quarantined + (1 if quarantined and not had_error else 0),
            errors=counts.errors + (1 if had_error else 0),
            last_completed_at=max(latest, completed_at) if latest else completed_at,
        )
    return tallies


def batch_counts(plan: list[dict[str, Any]], instances: list[dict[str, Any]]) -> StageCounts | None:
    """Episodes finished, counted by whole batches, or None when unknowable.

    The catalog goes quiet when a run replays work it already recorded (the
    append is idempotent, so nothing new lands), and a stage in that state
    would otherwise read as making no progress at all. A batch task that has
    ended did finish every episode in its slice either way, so the plan's own
    batch composition gives an exact lower bound that survives the replay.

    Both this and the catalog undercount rather than over, so the caller takes
    whichever is further along.
    """
    if not plan:
        return None
    sizes = [len(entry.get("items") or ()) for entry in plan]
    ended = [
        instance
        for instance in instances
        if str(instance.get("task_id")) == "process_batch"
        and str(instance.get("state") or "") in _ENDED_TASK_STATES
    ]
    if not ended:
        return StageCounts()
    done = 0
    for instance in ended:
        index = instance.get("map_index")
        if isinstance(index, int) and 0 <= index < len(sizes):
            done += sizes[index]
    ends = [parse_timestamp(instance.get("end_date")) for instance in ended]
    finished_at = [end for end in ends if end is not None]
    return StageCounts(done=done, last_completed_at=max(finished_at) if finished_at else None)


# A batch task in any of these states has stopped working on its episodes.
_ENDED_TASK_STATES = frozenset({"success", "failed", "skipped", "upstream_failed"})


def further_along(first: StageCounts | None, second: StageCounts | None) -> StageCounts | None:
    """Whichever of two undercounts got further, keeping the richer detail."""
    if first is None:
        return second
    if second is None or second.done <= first.done:
        return first
    # The batch count knows nothing of quarantine or errors; the catalog does.
    return StageCounts(
        done=second.done,
        quarantined=first.quarantined,
        errors=first.errors,
        last_completed_at=max(
            (stamp for stamp in (first.last_completed_at, second.last_completed_at) if stamp),
            default=None,
        ),
    )


def query_check_breakdown(
    catalog_root: Path,
    uris: list[str],
    window: StageWindow,
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    """Per-check outcomes for the episodes one stage finished in ``window``.

    What the stage's verification actually found, check by check: how many
    episodes passed, failed, were skipped, or crashed it, and what it cost.
    An unreadable catalog or a stage that records no checks (sync writes
    episode rows only) is an empty list, not an error.
    """
    if not _catalog_is_readable(catalog_root) or not uris:
        return []
    episodes_glob = _parquet_glob(catalog_root / "episodes")
    checks_glob = _parquet_glob(catalog_root / "check_runs")
    if episodes_glob is None or checks_glob is None:
        return []

    import duckdb

    counted = ", ".join(
        f"count(*) FILTER (c.status = '{status.value}') AS {status.value}_count"
        for status in CheckStatus
    )
    connection = duckdb.connect()
    try:
        cursor = connection.execute(
            f"""
            SELECT c.check_name AS name,
                   coalesce(bool_or(c.critical), false) AS critical,
                   {counted},
                   count(DISTINCT c.episode_id) AS episodes,
                   avg(c.duration_s) FILTER (c.status != 'skipped') AS avg_duration_s
            FROM read_parquet('{checks_glob}', union_by_name=true) c
            JOIN read_parquet('{episodes_glob}', union_by_name=true) e
              USING (episode_id, run_fingerprint)
            WHERE e.source_uri IN (SELECT unnest(?::VARCHAR[]))
              AND epoch(e.recorded_at) BETWEEN ? AND ?
            GROUP BY c.check_name
            ORDER BY c.check_name
            """,
            [uris, window.start.timestamp(), (window.end or now).timestamp()],
        )
        columns = [description[0] for description in cursor.description or []]
        rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    except (duckdb.Error, OSError):
        return []
    finally:
        connection.close()

    return [
        {
            "name": str(row["name"]),
            "critical": bool(row["critical"]),
            "statuses": {status.value: int(row[f"{status.value}_count"]) for status in CheckStatus},
            "episodes": int(row["episodes"]),
            "avg_duration_s": (
                round(float(row["avg_duration_s"]), 3)
                if row["avg_duration_s"] is not None
                else None
            ),
        }
        for row in rows
    ]


def _stage_for(recorded_at: float, bounds: dict[Stage, tuple[float, float]]) -> Stage | None:
    for stage, (start, end) in bounds.items():
        if start <= recorded_at <= end:
            return stage
    return None


def stage_progress(
    counts: StageCounts,
    *,
    total: int | None,
    window: StageWindow,
    running: bool,
    now: datetime,
) -> dict[str, Any]:
    """One stage's progress payload: counts, pace, ETA, and whether it stalled.

    Pace is measured from the stage's start, not from its first completion, so
    a slow first episode is visible as a slow stage. Before anything completes
    there is no pace to report and no honest ETA to offer, so both stay null
    rather than being guessed.
    """
    elapsed_s = ((window.end or now) - window.start).total_seconds()
    done = counts.done
    measurable = done > 0 and elapsed_s > 0
    remaining = max(0, total - done) if total is not None else None
    return {
        "done": done,
        "quarantined": counts.quarantined,
        "errors": counts.errors,
        "throughput_eps_per_min": round(60.0 * done / elapsed_s, 1) if measurable else None,
        "eta_s": (
            round(remaining * elapsed_s / done, 1) if running and measurable and remaining else None
        ),
        "stalled": _is_stalled(counts, elapsed_s=elapsed_s, running=running, now=now),
        "last_completed_at": iso_timestamp(counts.last_completed_at),
    }


def _is_stalled(counts: StageCounts, *, elapsed_s: float, running: bool, now: datetime) -> bool:
    if not running or counts.done <= 0 or counts.last_completed_at is None or elapsed_s <= 0:
        return False  # no pace baseline yet: quiet is not evidence of a stall
    quiet_s = (now - counts.last_completed_at).total_seconds()
    return quiet_s > max(_STALL_FLOOR_S, _STALL_FACTOR * elapsed_s / counts.done)
