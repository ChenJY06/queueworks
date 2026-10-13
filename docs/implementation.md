# Implementation

QueueWorks separates durable scheduling from task execution. The coordinator owns SQLite and exposes the HTTP API; workers execute supported handlers and report outcomes through a token-fenced protocol.

## Source modules

| Module | Responsibility |
| --- | --- |
| `src/queueworks/models.py` | Submission validation and worker protocol messages. |
| `src/queueworks/config.py` | Coordinator settings and environment configuration. |
| `src/queueworks/store.py` | Schema, transactions, atomic claims, retries, and attempt history. |
| `src/queueworks/api.py` | HTTP endpoints, readiness, and background lease recovery. |
| `src/queueworks/worker.py` | Claim loop, heartbeat renewal, execution, and bounded shutdown. |
| `src/queueworks/handlers.py` | File checksums, image thumbnails, synthetic work, and fixtures. |
| `src/queueworks/cli.py` | Coordinator, worker, submission, inspection, and experiment commands. |
| `src/queueworks/experiments.py` | Process lifecycle, crash injection, benchmarks, and reports. |

## Submission and claiming

`POST /jobs` validates the handler kind, payload, and attempt limit before accepting a job. File inputs must exist beneath the configured fixture directory. The coordinator stores the normalized submission and its initial queued state in one transaction.

An optional idempotency key identifies repeated submissions. Matching normalized content returns the existing job; changed content produces HTTP 409. The key check and insertion are serialized in the same SQLite write transaction.

A claim uses `BEGIN IMMEDIATE` before selecting an eligible job. It increments the attempt count, assigns a unique token and lease deadline, creates the attempt record, and changes the job to running. The transaction commits before the HTTP response reaches the worker. Competing claims therefore observe committed ownership rather than independently selecting the same queued state.

## Execution and lease renewal

Handlers run outside database transactions. The worker uses separate execution and heartbeat threads, with independent HTTP clients, so long-running tasks can retain their leases. The coordinator owns scheduling time; the worker uses monotonic deadlines for its local lease budget and `perf_counter` for execution-duration measurements.

Every heartbeat, completion, and failure checks running state, the current attempt token, and an unexpired deadline. An obsolete attempt receives HTTP 409 even if the periodic reaper has not yet processed its expiry.

## Failure recovery

A failed or expired attempt returns to queued with capped exponential backoff when attempts remain. Otherwise, the job reaches the terminal failed state. The coordinator checks expiry at startup, periodically, and during worker protocol operations, allowing the same database to recover after a restart.

Graceful worker shutdown stops claims and cooperatively cancels active execution within a bounded cleanup budget. A hard process kill bypasses cleanup; its lease must expire before reassignment. Durable attempt records distinguish interrupted execution from a later successful transition.

## Repeatable output

Checksum jobs return deterministic SHA-256 digests. Thumbnail attempts write unique temporary files and atomically replace a stable output path. Repeatability assumes immutable fixture bytes and unchanged payloads. File publication and SQLite completion are separate operations, so retries may repeat work after an acknowledgement is lost.

## Verification and measurement

API tests exercise real SQLite with a controllable scheduling clock. Process integration tests run the coordinator and independent workers over HTTP, including simultaneous claims, heartbeats, retries, restart, graceful shutdown, and hard termination.

Benchmarks separate submission-to-completion latency, queue residence, and handler execution time. Raw records preserve attempt history, failures, retries, and missing execution measurements from killed processes. The direct sequential reference uses the same handlers without HTTP or persistence; its reliability guarantees differ from the queue. Full methodology and observations are in the [benchmark report](../benchmarks/results/report.md).

The [design document](design.md) defines the invariants and storage trade-offs. The [verification record](verification.md) records executed checks and recovery evidence.
