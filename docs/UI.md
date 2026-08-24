# The dashboard: `hflow ui`

`hflow ui` serves a local dashboard for operating pipelines: live runs with
per-stage status, storage roots with catalog stats and browsing, and secrets
for task containers. It is a thin viewer over the same standard surfaces you
can query yourself -- the Airflow REST API and the Parquet catalog -- and it
works without a running runtime (tabs degrade rather than fail).

```bash
uv run hflow ui                # serves http://127.0.0.1:4400 and opens a browser
uv run hflow ui --port 5000 --bundle-dir data/ego100k/runtime --data-root data/ego100k
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--port` | `4400` | Port on `127.0.0.1`. The server never binds a public interface and refuses requests whose `Host` header names a foreign host. |
| `--bundle-dir` | `./data/runtime`, else `./runtime` | The rendered runtime bundle to observe (same probing as `status`/`down`). |
| `--data-root` | `./data` | Local data root the Storage tab lists implicitly. |
| `--no-browser` | off | Do not open the dashboard in a browser. |

## Pipelines

Runs of the master ingest DAG, newest first, polling every few seconds: state,
episode count and profile from the run conf, elapsed/duration, and a
four-segment stage strip (sync, meta, labels, media) derived from the master
run's own task instances. A stage disabled by the run profile shows as
skipped; a stage that never ran because an earlier one failed shows as
pending, not failed.

Opening a run shows its pipeline as stages, numbered in the order they run.
Each card carries the stage's title and purpose, which verification family
vouches for its output (automated checks, model verification, human review),
its state, and how many of the run's episodes it has finished. While a stage
is running the card adds its pace in episodes per minute, an estimate of the
time left, and -- when nothing has finished for well past the stage's own pace
-- a stalled warning, which is where a run bottlenecks in practice. A stage
the profile disabled shows as skipped, one waiting its turn names the stage
ahead of it, and once a run has ended the stages it never reached say so
rather than claiming to be waiting.

**Where the counts come from.** Every episode a stage finishes appends a row
to the catalog the pipeline already writes, so progress is read from that
Parquet -- no extra instrumentation, and no state of the dashboard's own.
Appends are attributed to a stage by the run's URI set inside that stage's own
window, which is exact for one runtime watching one run: concurrent runs over
overlapping episodes, or a `hflow test --record` in the same minutes, would
cross-attribute. Two honest limits follow. A run that replays work already
recorded appends nothing (the append is idempotent), so a running stage falls
back to counting whole finished batches -- coarser, and its pace estimate runs
pessimistic, but it does not read as stuck; and once a stage ends its own
budget gate's tally is preferred, which is why a replayed stage's count jumps
to complete when it finishes. Serving the dashboard without `--data-root`, or
against a bucket data root, leaves the counts unavailable and the rest of the
card intact.

Clicking a stage opens that stage: the same card, then what its checks found
-- per check, how many episodes passed, failed, were skipped or crashed it,
whether it is critical, and its average cost -- and then the tasks that did
the work (`plan`, the mapped `process_batch` instances, the budget gate).
Clicking a task opens a details panel with its state, timings, attempt count,
the task's doc line, and a link to that task's logs in Airflow; a mapped task
is one node with a per-batch breakdown in the panel. The sync stage records
episodes rather than checks, so its breakdown is empty by design.

The master DAG's own tasks are still drawable, a level down: the **Task graph**
link on the run page (`#/pipelines/run?id=<run>&graph=tasks`) lays them out top
to bottom with live state, for when the orchestration itself is the suspect
rather than the data. The dashboard reads that shape from Airflow, so it
follows whatever your bundle rendered.

Run pages poll every few seconds and stop once the run is finished.

Logs, retries, re-runs, and everything else task-level stay Airflow's. The
toolbar's credentials popover serves the admin username and password from the
bundle `.env`, so nothing needs to be fished out of terminal scrollback. The
dashboard triggers nothing; runs start from `hflow ingest` or the REST API as
before.

## Storage

Data roots and their catalogs. The served `--data-root` appears implicitly;
more roots -- local paths or `gs://`/`s3://`/`az://` URLs -- can be registered,
and registration is bookkeeping only (removing an entry never touches data).
Registered roots persist in `storage_roots.json` in the config directory (see
below). Each root shows episode and quarantine counts plus last activity from
its Parquet catalog, and can be browsed one directory level at a time.
Browsing a bucket root needs the `hflow[bucket]` extra; the error says so.

## Secrets

Key-value pairs that become environment variables in task containers, so a
step reads `os.environ["OPENAI_API_KEY"]` the same way in `app.test()` (from
your shell) and under Airflow (from the secrets store). Semantics:

- Stored in `secrets.env` in the config directory, owner-only (`0600`),
  never inside a bundle and never committed. Values are stored single-quoted
  so Compose reads them literally (`$` survives); a value therefore cannot
  contain single quotes or newlines.
- The rendered compose file references the store by absolute path; values
  enter container environments at `hflow up`, so **changes apply on the next
  `hflow up`** (the tab says so). Bundles rendered before this feature gain
  the reference on their next `hflow up`; the tab flags un-wired bundles.
- Values never leave the machine or the server: the dashboard lists names
  with a constant-width mask and has no reveal. Edit by overwriting.
- Locally the values are visible to you via `docker inspect`, as with any
  Compose environment -- the store is per-user, not a multi-tenant vault.

## The config directory

User-level state lives in one directory: `HFLOW_CONFIG_DIR` if set, else
`$XDG_CONFIG_HOME/hflow`, else `~/.config/hflow`. It holds `secrets.env` and
`storage_roots.json`. Both survive `hflow down --volumes` and bundle
re-renders.
