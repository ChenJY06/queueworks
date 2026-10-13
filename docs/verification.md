# Local verification record

The implementation was verified on **13 October 2026**, using Python 3.12.14 on Windows 11 (`Windows-11-10.0.26300-SP0`). The CPU was an Intel Core i9-14900HX with 32 logical CPUs. Installation used the resolved versions in `requirements-lock.txt`. This document records local execution, not a GitHub Actions result or production deployment.

## Checks

From the repository root in the installed virtual environment:

```powershell
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m pip check
```

| Check | Actual result |
| --- | --- |
| pytest | 44 passed, 1 skipped, 1 warning in 46.68 seconds. |
| Ruff lint | All checks passed. |
| Ruff formatting | All 15 Python files already formatted. |
| Dependency consistency | No broken requirements found. |

The skipped test checks POSIX SIGTERM behavior and is inapplicable on Windows, where `terminate()` is a hard kill. The Windows Ctrl+Break process test passed and exercised bounded graceful cleanup and subsequent recovery. pytest reported one upstream Starlette/AnyIO deprecation warning. Linux/Python 3.11 and 3.12 checks are configured in `.github/workflows/ci.yml`; their GitHub execution remains pending.

The suite exercises the actual HTTP/SQLite path for API requests, concurrent claims, retry success and attempt-limit failure, stale update rejection, lease renewal, coordinator restart, hard worker termination, and graceful worker cancellation. Controlled coordinator time checks lease deadlines and capped backoff without waiting for real intervals. Handler tests verify deterministic fixture generation and repeated output, including thumbnail replacement and path boundaries.

## Crash demonstration

```powershell
.\.venv\Scripts\python.exe -m queueworks demo --workers 3 --jobs 12 --output-dir artifacts/demo
```

The tool observed `worker-3` owning a running job before killing its process. All **12 accepted jobs succeeded**. The selected job had two attempts—an expired attempt and a successful retry—and exactly one successful state transition. It was reclaimed **5.850 seconds** after termination and durably completed **6.988 seconds** after termination. The tool cleaned up all coordinator and worker processes.

The [small retained summary](../benchmarks/results/crash-demo-summary.json) is copied directly from the demonstration's evidence, excluding the full job list. Full local evidence and diagnostic logs are in `artifacts/demo/crash-demo.json` and adjacent logs; those generated artifacts are ignored by Git. This recovery time includes work already queued on surviving workers as well as lease expiry, retry delay, claim time, and handler execution. It is an observation of this configuration, not a recovery-time guarantee.

## Benchmarks and remaining limits

The completed benchmark compared 1, 2, and 4 workers on both configured workloads, using 24 identical inputs per run, four excluded warm-up jobs, and two repetitions. All 288 measured queued jobs succeeded, with zero failures and zero retries; the direct sequential reference completed 96 executions. Twelve separate crash probes completed all 28 accepted probe jobs. Each selected probe job had two attempts and one successful state transition, with termination-to-success recovery between 2.645 and 2.818 seconds. Those probe retries are excluded from workload throughput and retry counts.

| Workload | Sequential mean success/s | 1 worker | 2 workers | 4 workers |
| --- | ---: | ---: | ---: | ---: |
| Alternating checksum/thumbnail | 134.071 | 17.026 | 26.575 | 30.180 |
| Synthetic 100 ms, 0 iterations | 9.094 | 6.532 | 12.490 | 21.046 |

These are means of the two per-run throughputs. The realistic queue was slower than the direct reference, and its first four-worker run (25.92 success/s) was slower than the first two-worker run (27.21 success/s). The reference does not perform HTTP, SQLite persistence, or lease coordination. The synthetic workload spends time waiting, so its concurrency benefit is not evidence of CPU-work speedup. Small inputs, warm filesystem caches, and two repetitions limit generalization.

SQLite was 3.53.1. Handler elapsed times use high-resolution `perf_counter`, backed by Windows QueryPerformanceCounter with a reported 100 ns resolution. Submission/completion and queue timestamps use the same machine's wall clock, for which Python reports a 15.625 ms resolution. This limits interpretation of small differences in latency and queue time; the reported clock resolution is not a claim of measurement accuracy. Memory usage was not measured reliably and is not claimed.

The [report](../benchmarks/results/report.md) includes each repetition's latency, queue time, handler duration, failures, retries, recovery, and two SVG plots. The [metadata](../benchmarks/results/metadata.json) records exact reproduction arguments and configuration; [summary CSV](../benchmarks/results/summary.csv) and [probe summaries](../benchmarks/results/crash-summary.json) preserve the compact results. Full raw CSV/JSON and terminal histories are in ignored `benchmarks/runs/verified/`; regenerate them with:

```powershell
.\.venv\Scripts\python.exe -m queueworks benchmark --workload all --workers 1 2 4 --jobs 24 --repetitions 2 --warmup 4 --duration-ms 100 --iterations 0 --lease-seconds 1.5 --data-dir data --output-dir benchmarks/runs/verified
```

Docker was not installed in this environment. The Dockerfile and Compose configuration are provided but **container execution remains unverified**. Verify locally with `docker compose up --build`, a submission, inspection of terminal results, and `docker compose down`.

## Publication checks

The destination repository is [ChenJY06/queueworks](https://github.com/ChenJY06/queueworks). Before publication, a fresh local run passed **44 tests, with one POSIX-only skip and one upstream deprecation warning, in 39.84 seconds**. Ruff lint, formatting of all 15 Python files, and dependency consistency checks passed again. A file audit confirmed that the commit contains only QueueWorks source, tests, documentation, packaging, CI configuration, and small benchmark summaries. Runtime data, virtual environments, generated thumbnails, and large raw benchmark outputs remain ignored. GitHub Actions status is separate from these local checks.
