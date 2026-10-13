# CV notes

**Locally verified on 13 October 2026.** `python -m pytest` passed 44 tests with one POSIX-only skip on Windows. Ruff lint/format checks and `pip check` also passed. See [the verification record](verification.md). Use only features present in the source and evidence produced by the tests and benchmark commands. Do not imply production deployment, database replication, exactly-once execution, or a passing GitHub CI run before those things exist.

- Built QueueWorks, a Python/FastAPI background job queue with durable SQLite state, an HTTP API, and independent worker processes; implemented atomic claims, renewable leases, attempt fencing, and bounded exponential retries.
- Added submission idempotency and repeatable checksum/thumbnail handlers; verified 44 automated tests and a three-worker crash demo that completed all 12 accepted jobs, with two attempts and one successful transition for the recovered job.
- Benchmarked 1, 2, and 4 HTTP workers against a sequential handler reference, recording throughput, latency, queue wait, execution time, and crash recovery; all 288 measured queued jobs completed successfully on the documented local workloads.

The [measured report](../benchmarks/results/report.md) used Python 3.12.14 on Windows 11, an Intel Core i9-14900HX with 32 logical CPUs, 24 jobs per run, four excluded warm-up jobs, and two repetitions. Its realistic queue was slower than the direct sequential handler reference, and one four-worker run was slower than the corresponding two-worker run. Avoid an unqualified speedup claim. If adding throughput numbers, state the workload and worker count with this measurement context. The short project description should fit your CV; use the longer design explanations during interviews.
