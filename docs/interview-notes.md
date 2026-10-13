# Interview notes

Use these questions to practice explaining the implementation. Refer to source and local evidence rather than claiming production scale or exactly-once execution.

## What problem does QueueWorks solve?

It accepts work quickly through HTTP, stores it durably, and lets independent processes perform it in the background. Clients can inspect progress and results. If a worker crashes, a lease eventually expires and the coordinator retries within a configured attempt limit. The project demonstrates these behaviors on one machine.

## Why is there one coordinator?

It owns SQLite and makes transaction boundaries easy to reason about. Workers use HTTP and never access the database. The coordinator is a single point of availability; restarting it from the same persistent database recovers durable state. This design does not implement replication or leader election.

## How do two workers avoid claiming the same active attempt?

The claim operation takes a short `BEGIN IMMEDIATE` write transaction before selecting and modifying eligible work. SQLite serializes writers. The job state, incremented attempt count, token, lease, and attempt audit record commit together. No database transaction remains open during handler execution.

## Why use a lease instead of a running boolean?

A boolean cannot detect a worker that disappears. A lease gives ownership a deadline. Heartbeats keep a valid long-running attempt alive; expiry lets the coordinator recover abandoned work. The trade-off is that a worker paused or partitioned for too long may be retried even though it later resumes.

## What prevents a stale worker from overwriting a result?

All worker updates check running state, the current attempt token, and an unexpired deadline. A reassigned job has a new token. A stale completion or heartbeat returns HTTP 409. This fences database state, not arbitrary external side effects.

## Is it exactly once?

No. A worker can execute a handler, crash before its completion is committed, and then have that handler executed again. Submission idempotency prevents duplicate jobs from matching client retries, while attempt fencing prevents obsolete updates from changing current state. Neither mechanism eliminates repeated task execution.

## How are the sample handlers safe to retry?

Checksums are deterministic reads. Thumbnails derive deterministic content from supported fixtures and validated dimensions. Attempts write separate temporary files and atomically replace the final output. A repeated attempt can reproduce the final file without exposing a partial final write. SQLite and file replacement are still separate operations.

## Why record attempts in a separate table?

The current job state is useful for scheduling and clients, while attempt history explains retries and recovery. Recording each attempt makes it possible to distinguish expired execution from a later success, attribute work to workers, and analyze duration. The two tables update in the same transaction when state changes.

## Why WAL and FULL synchronization?

WAL allows readers to overlap with a writer, but writes remain serialized. FULL synchronization favors stronger commit durability and incurs synchronization cost. A busy timeout tolerates brief write contention; keeping transactions short is still necessary. Durability ultimately depends on the filesystem and hardware as well as SQLite.

## What happens on shutdown?

Graceful worker termination stops new claims and attempts bounded cleanup of active work. A hard kill cannot execute cleanup, so the lease expires and normal retry policy takes over. Coordinator restart reads the same database and reclaims expired leases at startup. The demo deliberately kills an observed owner of a running job.

## How did you verify reliability?

Point to the actual test suite and local results. Controlled time checks precise lease and backoff boundaries. Process integration tests run a real coordinator, independent workers, and HTTP against SQLite. They test concurrent claims, restart persistence, hard termination and recovery, terminal outcomes, and stale update rejection. Do not call an unrun GitHub workflow a passing CI result.

## Why can more workers be slower?

More workers add HTTP polling, interpreter/process overhead, heartbeats, and competing SQLite writes. CPU, disk bandwidth, synchronization, or tiny handler inputs can dominate. Sleep-heavy synthetic tasks show concurrency easily but do not represent CPU-heavy work. Use saved benchmark data and the configuration record to explain the observed case.

## What does the sequential reference mean?

It runs the same handler workload directly, without submission HTTP, durable queue writes, leases, or process orchestration. It estimates handler cost and illustrates queue overhead. It is not an equal comparison of reliability guarantees. Startup, cache state, input sizes, and repetitions affect interpretation.

## What would you change for a production service?

Start from requirements: access control, admission limits, observability, retention, failure reporting, and deployment availability. A server database can support different concurrency and hosting needs. External handlers need side-effect idempotency. Higher availability requires a deliberate coordinator/database deployment design. QueueWorks does not claim to have implemented those features.

## A concise explanation to practice

"I built a Python HTTP job queue with one SQLite coordinator and independent worker processes. Claims and state updates are transactional. Each attempt has a token and a renewable lease, so a hard-crashed worker's job can be retried and an obsolete worker cannot overwrite the current result. I tested real HTTP and process failure paths and measured 1, 2, and 4 workers on controlled workloads. Retries can execute work more than once, so the sample file handlers are repeatable."

Add a performance number only after reading the measured report, and include the workload and machine context.
