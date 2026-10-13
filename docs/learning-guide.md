# Learning guide

You can understand QueueWorks with basic Python by following one job from submission to completion. Keep the API running in one terminal, a worker in another, and use the CLI in a third. Start with one checksum job before studying failure paths.

## A useful reading order

1. `src/queueworks/models.py`: accepted job shapes and validation. Learn how a small fixed handler set makes the protocol easier to understand.
2. `src/queueworks/store.py`: jobs, attempts, events, explicit transactions, atomic claiming, and token checks. This is the core correctness layer.
3. `src/queueworks/api.py`: HTTP endpoints, request/response mapping, lifespan, and expiry recovery.
4. `src/queueworks/worker.py`: the worker loop, separate heartbeat execution, result reporting, and shutdown.
5. `src/queueworks/handlers.py`: fixture boundaries and repeatable checksum/thumbnail work.
6. `src/queueworks/cli.py`: commands that connect the pieces; then `src/queueworks/experiments.py` for subprocess ownership, the crash demo, metrics, and benchmarks.
7. `tests/`: read controlled-time reliability tests before the process integration tests.

`src/queueworks/config.py` collects configuration defaults and environment parsing. The [design document](design.md) describes the invariants to look for while reading.

## Follow one successful job

Submission validates the payload and inserts a queued job. The returned ID is durable after the transaction commits. A worker sends a claim request. The coordinator starts a write transaction, finds eligible work, increments the attempt count, creates a token, records an attempt, and changes the job to running.

The worker receives a copy of the payload and token. It runs the handler outside the database transaction while a separate heartbeat path renews the lease. On success, it sends the result and current token. The coordinator checks the token, state, and deadline, then records the result and terminal state together.

Try inspecting the job while it is running and again after it succeeds. Notice which timestamps are set at each step, and compare job-level state to the history of attempts.

## What a transaction gives you

A transaction groups database changes into one committed operation. If claiming changed the job to running but failed before creating the attempt record, the transaction would roll back both changes. Other operations should never observe half of a committed claim.

`BEGIN IMMEDIATE` also serializes the competing write decisions. Imagine two workers asking for the oldest queued job simultaneously: one claim owns the write reservation, commits its state change, and then the other claim reads the new state. Running the actual concurrent process test is a useful way to see this invariant exercised.

Exercise: describe the failure you would introduce by selecting a job before starting the transaction. Do not implement that unsafe change in the main branch.

## A lease is a temporary right to work

A lease says, "this attempt may report results until this deadline." A heartbeat extends the deadline. A running status alone is insufficient because a worker can disappear without sending a final message.

Exercise: use a short lease and a long synthetic job. Observe heartbeats keeping it running. Then run the crash demo and explain why the job cannot be reassigned immediately when a hard kill occurs.

The token distinguishes attempts. A job ID identifies durable work; an attempt token identifies one temporary grant to a worker. Treating those as the same concept would allow old workers to alter newly reassigned work.

## Why repeated execution happens

There is a gap between doing the task and committing its result. If a worker finishes the local file but crashes before reporting it, the coordinator must eventually retry or leave the job permanently running. QueueWorks retries, which means execution can repeat.

Exercise: sketch the two histories "crash before writing output" and "crash after writing output but before reporting success." Explain why the coordinator cannot distinguish them from the missing completion alone.

Checksum reads are safe to repeat. Thumbnail writes use attempt-specific temporary files and atomic replacement of a deterministic output. Exercise: propose an email-sending handler and explain why its side effects need a separate idempotency strategy before it would be safe to add.

## Submission idempotency is a different boundary

A client can lose a submission response and retry. With no key, those requests create separate jobs. With the same key and same validated content, they return one job. A changed payload with the same key is an error rather than a silent replacement.

Exercise: submit the same checksum twice with one key, then change the payload or attempt limit and repeat that key. Explain how a database uniqueness constraint handles requests arriving at the same time.

## Tests teach the contract

Controlled-time unit tests make lease and backoff boundaries deterministic. Process tests prove that the protocol works over real HTTP and that independently started workers coordinate through SQLite. Both levels matter: a fake HTTP client cannot demonstrate operating-system process termination, and a slow process test is a poor way to check every time boundary.

Exercise: write down the expected status immediately before expiry, exactly at expiry, and after a new claim. Then read the tests for stale heartbeat and completion updates. Identify which condition rejects each update.

Exercise: inspect an expired attempt followed by a successful attempt. Count handler attempts separately from successful job transitions. More than one attempt does not mean the database accepted two successes.

## Measurements need boundaries

QueueWorks separates submission-to-completion latency, time spent waiting in the queue, and worker-measured handler duration. A synthetic sleep can produce good parallel throughput without doing much CPU work. Tiny files can make HTTP and durability overhead dominate. A sequential reference has no queue reliability work and is therefore a useful baseline with limits.

Exercise: run 1, 2, and 4 workers first on synthetic sleep, then on more CPU iterations. Keep the workload and configuration identical across worker counts. Use the saved raw data to explain a case where adding workers does not help.

Exercise: reduce the lease duration and compare recovery time with normal throughput. Describe the trade-off between fast crash recovery and tolerance for scheduling or network delays.

## Small extension projects

- Add keyset pagination and explain how it behaves under concurrent inserts.
- Add retry jitter with a controllable random source and deterministic tests.
- Add a job cancellation state, specifying how it interacts with a running token and late completion.
- Add a retention command that preserves active jobs and removes old terminal history in bounded batches.
- Add a new deterministic handler only after defining its payload validation and repeat-execution behavior.

For each extension, write its invariant and failure behavior before editing code. Keep the implementation small enough to trace one request from endpoint to transaction to worker.
