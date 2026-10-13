# Verification

Recorded on **13 October 2026** with Python 3.12.14, Windows 11 (`Windows-11-10.0.26300-SP0`), and an Intel Core i9-14900HX with 32 logical CPUs. Installation used `requirements-lock.txt`. These records cover local execution and GitHub CI; production deployment has not been verified.

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

The skipped test checks POSIX SIGTERM behavior and is inapplicable on Windows, where `terminate()` is a hard kill. The Windows Ctrl+Break process test passed and exercised bounded graceful cleanup and subsequent recovery. pytest reported one upstream Starlette/AnyIO deprecation warning. Linux/Python 3.11 and 3.12 checks are configured in `.github/workflows/ci.yml` and passed in the recorded GitHub Actions run below.

Coverage is mapped to test files in [implementation](implementation.md#tests-and-experiments). API tests use real SQLite with controlled scheduling time; integration tests exercise HTTP and independent processes.

## Crash demonstration

```powershell
.\.venv\Scripts\python.exe -m queueworks demo --workers 3 --jobs 12 --output-dir artifacts/demo
```

The tool observed `worker-3` owning a running job before killing its process. All **12 accepted jobs succeeded**. The selected job had two attempts—an expired attempt and a successful retry—and exactly one successful state transition. It was reclaimed **5.850 seconds** after termination and durably completed **6.988 seconds** after termination. The tool cleaned up all coordinator and worker processes.

The [retained summary](../benchmarks/results/crash-demo-summary.json) excludes the full job list. Full local evidence is in ignored `artifacts/demo/crash-demo.json` and adjacent logs. Recovery includes backlog on surviving workers, lease expiry, retry delay, claim time, and handler execution.

## Benchmark evidence

The benchmark compared 1, 2, and 4 workers on both workloads: 24 jobs per run, four excluded warm-up jobs, and two repetitions. All 288 measured queue jobs succeeded without failures or retries. The direct reference completed 96 executions. Twelve separate crash probes completed all 28 probe jobs; selected jobs had two attempts and one successful transition, recovering in 2.645–2.818 seconds. Probe retries are excluded from workload throughput and retry counts.

The [report](../benchmarks/results/report.md) preserves per-run latency, queue wait, handler duration, failures, retries, recovery, and plots. It also defines throughput boundaries, percentile interpolation, reference limitations, caching effects, and clock resolution. [Metadata](../benchmarks/results/metadata.json) records the environment and reproduction arguments; [summary CSV](../benchmarks/results/summary.csv) and [probe summaries](../benchmarks/results/crash-summary.json) preserve the measurements. The [README](../README.md#measured-results) contains the throughput overview.

Benchmark artifacts were retained without changing their measurements. Full raw CSV/JSON and terminal histories are in ignored `benchmarks/runs/verified/`; regenerate them with:

```powershell
.\.venv\Scripts\python.exe -m queueworks benchmark --workload all --workers 1 2 4 --jobs 24 --repetitions 2 --warmup 4 --duration-ms 100 --iterations 0 --lease-seconds 1.5 --data-dir data --output-dir benchmarks/runs/verified
```

Container execution remains unverified because Docker was unavailable. The [usage reference](usage.md#docker-compose) describes the Compose setup and verification steps.

## Continuous integration

An additional local run passed **44 tests, with one POSIX-only skip and one upstream deprecation warning, in 39.84 seconds**. Ruff lint, formatting of all 15 Python files, and dependency consistency checks also passed.

The [GitHub Actions run](https://github.com/ChenJY06/queueworks/actions/runs/38368363662) for implementation commit [e8e4360](https://github.com/ChenJY06/queueworks/commit/e8e43603955779aedbc030f386698279172462a0) completed successfully. Both Linux jobs, Python 3.11 and Python 3.12, passed dependency installation, Ruff lint, Ruff formatting, and pytest.
