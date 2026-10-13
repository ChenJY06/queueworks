# Implementation

## Source map

| Module | Main responsibilities |
| --- | --- |
| `src/queueworks/models.py` | Payload validation and worker request models. |
| `src/queueworks/config.py` | Settings and environment parsing. |
| `src/queueworks/store.py` | Schema, transactions, claims, lease checks, retries, and audit history. |
| `src/queueworks/api.py` | HTTP endpoints, readiness, and lease-reaper lifecycle. |
| `src/queueworks/worker.py` | Worker loop, handler/heartbeat threads, acknowledgement, and shutdown. |
| `src/queueworks/handlers.py` | Supported handlers and deterministic fixture generation. |
| `src/queueworks/cli.py` | CLI arguments and command dispatch. |
| `src/queueworks/experiments.py` | Subprocess management, crash injection, metrics, and reports. |

## A job through the code

1. **Validate.** `JobSubmit.validate_handler()` selects the handler-specific model and fills payload defaults. The `/jobs` endpoint resolves file inputs. `Store.submit()` performs that file check only for a new submission, after checking any idempotency key.
2. **Persist.** `Store.submit()` writes the queued job and submission event. `_record()` builds public responses from the job, attempts, and events while omitting private tokens and submission-key details.
3. **Claim.** `Worker.run()` posts its ID to `/internal/claim`. `Store.claim()` reaps expired leases, selects eligible work, and commits ownership and attempt state before returning the token.
4. **Execute.** `Worker._run_attempt()` starts the handler and heartbeat threads. `execute()` dispatches the fixed handler set; a cancellation event is checked during checksum chunks, synthetic work, and thumbnail publication.
5. **Renew.** The heartbeat client posts the token independently of execution. `Store.heartbeat()` checks `_active()` before extending both job and attempt deadlines.
6. **Finish.** The worker sends result or error with elapsed handler milliseconds. `Store.finish()` checks the active lease, records the attempt outcome, and commits success or calls `_retry()`. A lost acknowledgement is logged without retrying that acknowledgement.
7. **Recover.** `create_app()` starts a periodic reaper through FastAPI's lifespan hook. `Store.initialize()` also reaps on startup. Expired work follows the same retry policy as explicit failures.

The [design document](design.md) defines transaction boundaries, exact expiry behavior, and delivery semantics. The [usage reference](usage.md) defines HTTP request/response shapes.

## Tests and experiments

| File | Coverage |
| --- | --- |
| `tests/test_api.py` | Validation, pagination, concurrent idempotency, controlled-time leases, retries, stale updates, and durable state. |
| `tests/test_handlers.py` | Fixture/output repeatability, path containment, and cancellation. |
| `tests/test_worker.py` | Bounded cancellation and platform-specific graceful shutdown. |
| `tests/test_process_integration.py` | Independent HTTP workers, concurrent claims, long-handler renewal, retries, hard termination, and coordinator restart. |

`ProcessCluster` owns subprocesses and logs and cleans them up on context exit. `poll_until()` gives state-based waits a deadline. `_crash_probe()` selects a running job with a live owner, kills that process, and checks terminal outcomes and successful transition counts. Benchmarks use fresh databases per configuration and save per-job rows and attempt histories. The [report](../benchmarks/results/report.md) defines timing boundaries and the sequential comparison.
