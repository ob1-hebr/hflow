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

The dashboard deliberately does not replace step-level observability: each
run deep-links into Airflow's own UI for tasks, logs, retries, and re-runs.
The toolbar's credentials popover serves the admin username and password from
the bundle `.env`, so nothing needs to be fished out of terminal scrollback.
The dashboard triggers nothing; runs start from `hflow ingest` or the REST
API as before.

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
  never inside a bundle and never committed.
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
