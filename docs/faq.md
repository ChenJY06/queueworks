# Technical FAQ

## How are simultaneous claims serialized?

Claiming starts a short `BEGIN IMMEDIATE` transaction before selecting eligible work. SQLite serializes writers, and ownership, attempt count, token, and audit history commit together. Handler execution starts after the transaction closes.

## What prevents obsolete workers from overwriting results?

Worker updates require the current running state, current attempt token, and an unexpired lease. Reassignment creates a new token. A stale heartbeat, completion, or failure returns HTTP 409. This protects coordinator state; external side effects require their own idempotency or fencing strategy.

## What delivery semantics are provided?

Retries can execute a handler more than once, including when execution finishes but its completion acknowledgement is lost. Attempts are bounded by the configured limit. Submission idempotency prevents duplicate accepted jobs for matching client retries, while attempt fencing rejects obsolete state changes. There is no universal exactly-once execution guarantee.

## How are the built-in handlers safe to repeat?

Checksums are deterministic reads. Thumbnails use attempt-specific temporary files and atomic replacement of deterministic output. Fixtures must remain immutable between attempts. File replacement and database completion are separate operations, and thumbnail publication does not explicitly fsync the file or directory.

## Why use WAL and FULL synchronization?

WAL permits reader/writer overlap while preserving a single SQLite writer. `synchronous=FULL` requests stronger commit durability at additional synchronization cost. A busy timeout handles brief lock contention. Filesystem and storage behavior remain part of the durability boundary.

## What happens when a worker or coordinator stops?

Graceful worker shutdown stops new claims and attempts bounded cancellation and failure reporting. A hard kill leaves its attempt running until lease expiry. The coordinator then retries or fails it at the attempt limit. Coordinator restart preserves accepted jobs and attempts in the same database and reclaims expired leases at startup.

## How is reliability verified?

Controlled-time tests cover exact lease and backoff boundaries. Integration tests use actual HTTP, SQLite, and independent processes to verify concurrent claims, renewal, retries, permanent failure, restart, and recovery after termination. The crash demonstration observes an active owner before injecting failure and verifies terminal outcomes and a single successful state transition.

## Why can additional workers reduce throughput?

Additional workers introduce polling, heartbeats, process overhead, and competing SQLite writes. Small handlers can be dominated by HTTP and persistence costs. The measured checksum/thumbnail workload includes a repetition where four workers were slower than two. Synthetic waiting demonstrates concurrency without establishing CPU-work speedup.

## What is the sequential reference comparing?

The reference executes identical handler inputs directly, without HTTP, SQLite writes, leases, or process orchestration. It provides a handler-cost baseline. It is not a durability-equivalent queue implementation. Workload size, cache state, clock resolution, and repetitions limit interpretation.

## What deployment limits apply?

QueueWorks uses one coordinator and one local SQLite database, with shared fixture storage. The coordinator is a single point of availability. Replication, leader election, multi-tenant authorization, admission quotas, retention policies, and arbitrary executable handlers are outside version one.
