# Usage

## Installation

QueueWorks requires Python 3.11 or newer. FastAPI, Uvicorn, HTTPX, and Pillow are runtime dependencies; pytest and Ruff are development dependencies. SQLite is included with Python. Install with the checked-in constraint file to use the resolved dependency versions from the recorded verification environment.

On Linux or macOS, from the repository root:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -c requirements-lock.txt -e '.[dev]'
```

### Windows

From PowerShell, with Python 3.11 installed:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -c requirements-lock.txt -e ".[dev]"
```

If activation is blocked by execution policy, use `.\.venv\Scripts\python.exe` and `.\.venv\Scripts\queueworks.exe` directly. The dependency set was verified locally on Python 3.12.14/Windows and in the [recorded CI run](https://github.com/ChenJY06/queueworks/actions/runs/38368363662) on Python 3.11 and 3.12/Linux.

## Manual operation

Generate the fixture data once, then start the coordinator:

```sh
queueworks fixtures
queueworks serve --host 127.0.0.1 --port 8000
```

Start each worker in a separate terminal:

```sh
queueworks worker --url http://127.0.0.1:8000
```

The coordinator and workers must use the same data directory. Only the coordinator opens SQLite. Keep fixtures unchanged while jobs reference them.

```sh
queueworks submit --kind checksum --payload '{"path":"fixtures/sample.bin"}' --max-attempts 3 --idempotency-key checksum-example
queueworks submit --kind thumbnail --payload '{"path":"fixtures/sample.png","width":160,"height":120}'
queueworks submit --kind synthetic --payload '{"duration_ms":500,"iterations":0,"fail_until_attempt":2}'
queueworks inspect JOB_ID
queueworks list --status succeeded --limit 20 --offset 0
```

Replace `JOB_ID` with the submitted ID. Client commands default to `http://127.0.0.1:8000`; use `--url` to select another coordinator. Payload examples use shell-quoted JSON. PowerShell 7 preserves these arguments; older shells may require different escaping. The HTTP API is also available directly.

`queueworks --help` and each subcommand's `--help` list options. `demo` and `benchmark` manage their own coordinator and worker subprocesses. Benchmark workload options select `realistic`, `synthetic`, or `all`; `--iterations` adds bounded Python CPU work to synthetic jobs. The [README](../README.md#measured-results) gives the recorded benchmark command and links to its methodology.

## HTTP API

Interactive API documentation is available at `http://127.0.0.1:8000/docs` while the coordinator runs.

| Method and path | Request and response |
| --- | --- |
| `POST /jobs` | Submit `{kind, payload, max_attempts}`; optional `Idempotency-Key` header. Returns the accepted job. |
| `GET /jobs/{id}` | Status, attempt count, timestamps, result/error, attempts, and transition events. |
| `GET /jobs` | Optional `status`, `limit`, and `offset`; returns `{items, total, limit, offset}`. |
| `GET /health` | Coordinator/database readiness; HTTP 503 when the database is unavailable. |
| `POST /internal/claim` | `{worker_id}`; returns `{job: null}` or a job including `attempt_token`, `lease_expires_at`, and `lease_seconds`. |
| `POST /internal/jobs/{id}/heartbeat` | `{token}`; returns the renewed lease deadline and duration. |
| `POST /internal/jobs/{id}/complete` | `{token, result, execution_ms}`; returns the resulting status. |
| `POST /internal/jobs/{id}/fail` | `{token, error, execution_ms}`; returns the resulting status. |

The supported job payloads are:

| Kind | Fields and bounds |
| --- | --- |
| `checksum` | `path`: an existing relative file under `fixtures/`. |
| `thumbnail` | `path`, `width`, `height`; dimensions default to 128 and range from 1 to 2048. They bound the image while preserving aspect ratio. |
| `synthetic` | `duration_ms` defaults to 100, range 0–30,000; `iterations` defaults to 0, range 0–5,000,000; `fail_until_attempt` defaults to 0, range 0–10. |

`max_attempts` defaults to 3 and ranges from 1 to 10. Pagination limits are 1–100, with offsets from 0 to 1,000,000. Unknown payload fields are rejected. Absolute paths, traversal, drive letters, and control characters are rejected; coordinator and worker checks also resolve symlinks within the data directory.

New submissions return HTTP 201. Matching idempotent submissions return the existing job with HTTP 200; conflicting content returns HTTP 409. Stale worker updates return HTTP 409, unknown jobs HTTP 404, and invalid requests HTTP 422. Public inspection omits attempt tokens. Timestamps are UTC Unix seconds.

## Configuration

Set coordinator environment variables before `queueworks serve`. Workers use `QUEUEWORKS_DATA_DIR` and `QUEUEWORKS_WORKER_KEY`, or their corresponding CLI options.

| Variable | Default | Purpose |
| --- | --- | --- |
| `QUEUEWORKS_DB_PATH` | `runtime/queueworks.sqlite3` | Coordinator database file. |
| `QUEUEWORKS_DATA_DIR` | `data` | Fixture and output directory. |
| `QUEUEWORKS_LEASE_SECONDS` | `10` | Lease duration renewed by heartbeats. |
| `QUEUEWORKS_RETRY_BASE_SECONDS` | `0.5` | First retry delay. |
| `QUEUEWORKS_RETRY_CAP_SECONDS` | `30` | Maximum retry delay. |
| `QUEUEWORKS_REAPER_INTERVAL` | `0.25` | Background expiry-check interval. |
| `QUEUEWORKS_SQLITE_BUSY_TIMEOUT_MS` | `5000` | Bounded SQLite lock wait. |
| `QUEUEWORKS_SQLITE_SYNCHRONOUS` | `FULL` | `FULL` or `NORMAL`; see [durability trade-offs](design.md#sqlite-and-durability). |
| `QUEUEWORKS_WORKER_KEY` | unset | Shared key for internal endpoints, sent as `X-Worker-Key`. |

All workers must use the coordinator's configured worker key. Keep the public API on loopback for local operation; this shared key does not add public API authorization or TLS.

## Docker Compose

Container execution remains unverified because Docker was unavailable during verification.

```sh
docker compose up --build
```

Compose starts one coordinator and three workers. The coordinator generates fixtures before serving. Named volumes persist the database and shared data; only the coordinator mounts the database volume. The API binds to host loopback at `http://127.0.0.1:8000`.

```sh
docker compose exec coordinator queueworks submit --kind checksum --payload '{"path":"fixtures/sample.bin"}'
docker compose exec coordinator queueworks list --limit 20
docker compose down
```

Inspect submitted IDs until they reach terminal states. `docker compose down` preserves the named volumes; `docker compose down --volumes` deletes them. Coordinator replication is not supported.

## Development checks

```sh
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m pip check
```

`make check`, `make demo`, and `make benchmark` wrap the corresponding commands. Runtime files and raw benchmark runs are ignored by Git; small measurement summaries are retained in `benchmarks/results/`.
