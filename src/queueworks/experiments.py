"""Repeatable HTTP/process demonstrations and deliberately modest benchmarks.

The coordinator and workers in these experiments are real child processes. Only
the coordinator opens the experiment database. Saved evidence is useful both for
debugging a failure and for checking a benchmark's timing boundaries.
"""

from __future__ import annotations

import csv
import html
import json
import math
import os
import platform
import signal
import socket
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx

from queueworks.models import JobSubmit

TERMINAL = {"succeeded", "failed"}
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def poll_until(condition: Callable[[], Any], *, timeout: float = 20.0, description: str) -> Any:
    """Bound every asynchronous wait; include the purpose in timeout failures."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = condition()
            if result:
                return result
        except (httpx.HTTPError, OSError) as exc:
            last_error = exc
        time.sleep(0.025)
    detail = f"; last error: {last_error}" if last_error else ""
    raise TimeoutError(f"Timed out waiting for {description}{detail}")


def _available_port() -> int:
    # Binding port zero asks the OS for a currently available port. The release
    # before the subprocess binds is a small unavoidable race, reported by logs.
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class ProcessCluster:
    """Own every process/log file and clean them up even when an assertion fails."""

    def __init__(
        self,
        directory: Path,
        *,
        data_dir: Path | None = None,
        lease_seconds: float = 1.2,
        retry_base_seconds: float = 0.05,
    ) -> None:
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)
        self.data_dir = Path(data_dir or self.directory / "data").resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.directory / "queue.sqlite3"
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.url = f"http://127.0.0.1:{_available_port()}"
        self.coordinator: subprocess.Popen | None = None
        self.workers: dict[str, subprocess.Popen] = {}
        self._children: list[subprocess.Popen] = []
        self._logs: list[Any] = []
        self._generation = 0
        self.client = httpx.Client(base_url=self.url, timeout=5.0, trust_env=False)

    def __enter__(self) -> ProcessCluster:
        try:
            self.start_coordinator()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _spawn(self, name: str, arguments: list[str]) -> subprocess.Popen:
        log = (self.directory / f"{name}.log").open("ab", buffering=0)
        self._logs.append(log)
        environment = os.environ.copy()
        # An unrelated shell's authentication/durability overrides must not
        # affect these isolated loopback experiments with explicit settings.
        environment.pop("QUEUEWORKS_WORKER_KEY", None)
        environment.update(
            {
                "PYTHONUNBUFFERED": "1",
                "QUEUEWORKS_DB_PATH": str(self.db_path),
                "QUEUEWORKS_DATA_DIR": str(self.data_dir),
                "QUEUEWORKS_LEASE_SECONDS": str(self.lease_seconds),
                "QUEUEWORKS_RETRY_BASE_SECONDS": str(self.retry_base_seconds),
                "QUEUEWORKS_RETRY_CAP_SECONDS": "1",
                "QUEUEWORKS_REAPER_INTERVAL": "0.05",
                "QUEUEWORKS_SQLITE_SYNCHRONOUS": "FULL",
                "QUEUEWORKS_SQLITE_BUSY_TIMEOUT_MS": "5000",
            }
        )
        existing = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = str(PROJECT_ROOT / "src") + (
            os.pathsep + existing if existing else ""
        )
        options: dict[str, Any] = {}
        if os.name == "nt":
            options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            options["start_new_session"] = True
        child = subprocess.Popen(
            [sys.executable, "-m", "queueworks", *arguments],
            cwd=PROJECT_ROOT,
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            **options,
        )
        self._children.append(child)
        return child

    def start_coordinator(self) -> None:
        self._generation += 1
        name = f"coordinator-{self._generation}"
        port = self.url.rsplit(":", 1)[1]
        self.coordinator = self._spawn(name, ["serve", "--host", "127.0.0.1", "--port", port])

        def healthy() -> bool:
            assert self.coordinator is not None
            if self.coordinator.poll() is not None:
                raise RuntimeError(f"Coordinator exited. Read {self.directory / (name + '.log')}")
            return self.client.get("/health").status_code == 200

        poll_until(healthy, timeout=20, description="coordinator/database readiness")

    def start_worker(self, worker_id: str) -> subprocess.Popen:
        child = self._spawn(
            worker_id,
            [
                "worker",
                "--url",
                self.url,
                "--data-dir",
                str(self.data_dir),
                "--worker-id",
                worker_id,
            ],
        )
        self.workers[worker_id] = child

        def ready() -> bool:
            if child.poll() is not None:
                raise RuntimeError(f"Worker exited. Read {self.directory / (worker_id + '.log')}")
            return "worker_started" in (self.directory / f"{worker_id}.log").read_text(
                encoding="utf-8", errors="replace"
            )

        poll_until(ready, timeout=20, description=f"worker {worker_id} readiness")
        return child

    def start_workers(self, count: int) -> None:
        for number in range(count):
            self.start_worker(f"worker-{number + 1}")

    def submit(self, job: dict[str, Any], key: str | None = None) -> dict[str, Any]:
        headers = {"Idempotency-Key": key} if key else {}
        response = self.client.post("/jobs", json=job, headers=headers)
        response.raise_for_status()
        return response.json()

    def job(self, job_id: str) -> dict[str, Any]:
        response = self.client.get(f"/jobs/{job_id}")
        response.raise_for_status()
        return response.json()

    def wait_for_jobs(self, ids: list[str], timeout: float = 30.0) -> list[dict[str, Any]]:
        def terminal_jobs() -> list[dict[str, Any]] | bool:
            jobs = [self.job(job_id) for job_id in ids]
            return jobs if all(job["status"] in TERMINAL for job in jobs) else False

        return poll_until(
            terminal_jobs, timeout=timeout, description="all accepted jobs to terminate"
        )

    def kill_worker(self, worker_id: str) -> float:
        child = self.workers[worker_id]
        if child.poll() is not None:
            raise RuntimeError(f"Selected worker {worker_id} already exited")
        killed_at = time.time()
        child.kill()  # Intentionally bypass all worker cleanup in a crash experiment.
        child.wait(timeout=5)
        return killed_at

    @staticmethod
    def stop_process(child: subprocess.Popen) -> None:
        if child.poll() is not None:
            return
        try:
            if os.name == "nt":
                child.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                child.terminate()
            child.wait(timeout=3)
        except (OSError, subprocess.TimeoutExpired):
            child.kill()
            child.wait(timeout=5)

    def restart_coordinator(self) -> None:
        assert self.coordinator is not None
        self.stop_process(self.coordinator)
        self.start_coordinator()

    def close(self) -> None:
        for child in reversed(self._children):
            self.stop_process(child)
        self.client.close()
        for log in self._logs:
            log.close()


def _synthetic(duration_ms: int, **payload: Any) -> dict[str, Any]:
    return {
        "kind": "synthetic",
        "payload": {"duration_ms": duration_ms, "iterations": 0, **payload},
        "max_attempts": 3,
    }


def _validate_lease(lease_seconds: float) -> None:
    if not math.isfinite(lease_seconds) or lease_seconds <= 0:
        raise ValueError("Lease seconds must be finite and positive")


def _running_owned_job(cluster: ProcessCluster, ids: list[str]) -> dict[str, Any] | bool:
    for job_id in ids:
        job = cluster.job(job_id)
        if job["status"] == "running":
            owner = job.get("worker_id") or job["attempts"][-1]["worker_id"]
            child = cluster.workers.get(owner)
            if child is not None and child.poll() is None:
                job["observed_owner"] = owner
                return job
    return False


def _crash_probe(cluster: ProcessCluster, *, count: int, replace: bool) -> dict[str, Any]:
    ids = [cluster.submit(_synthetic(1100))["id"] for _ in range(count)]
    selected = poll_until(
        lambda: _running_owned_job(cluster, ids),
        timeout=15,
        description="an actual worker owning a running attempt before the crash",
    )
    owner = selected["observed_owner"]
    observed_at = time.time()
    killed_at = cluster.kill_worker(owner)
    if replace:
        cluster.start_worker(f"{owner}-replacement")
    jobs = cluster.wait_for_jobs(ids, timeout=45)
    recovered = next(job for job in jobs if job["id"] == selected["id"])
    successful_attempts = sum(attempt["status"] == "succeeded" for attempt in recovered["attempts"])
    if recovered["status"] != "succeeded" or recovered["attempt_count"] < 2:
        raise RuntimeError(f"Crash recovery did not succeed: {recovered}")
    if successful_attempts != 1 or not all(job["status"] == "succeeded" for job in jobs):
        raise RuntimeError(
            "Crash demonstration did not complete every accepted job exactly once in state"
        )
    successful_transitions = sum(
        event["to_status"] == "succeeded" for event in recovered.get("events", [])
    )
    if recovered.get("events") and successful_transitions != 1:
        raise RuntimeError("Recovered job has more than one recorded transition to succeeded")
    second_start = recovered["attempts"][1]["started_at"]
    return {
        "job_id": recovered["id"],
        "worker_id": owner,
        "worker_pid": cluster.workers[owner].pid,
        "observed_running_at": observed_at,
        "killed_at": killed_at,
        "reclaimed_at": second_start,
        "completed_at": recovered["completed_at"],
        "reclaim_seconds": second_start - killed_at,
        "recovery_seconds": recovered["completed_at"] - killed_at,
        "attempt_count": recovered["attempt_count"],
        "successful_state_transitions": successful_transitions or successful_attempts,
        "accepted_jobs": len(jobs),
        "succeeded_jobs": sum(job["status"] == "succeeded" for job in jobs),
        "replacement_started": replace,
        "jobs": jobs,
    }


def run_demo(args: Any) -> dict[str, Any]:
    """Crash a known owner, then save the complete attempt audit as evidence."""
    from queueworks.handlers import generate_fixtures

    _validate_lease(args.lease_seconds)
    output = Path(getattr(args, "output", "artifacts/demo")).resolve()
    output.mkdir(parents=True, exist_ok=True)
    data_dir = Path(args.data_dir).resolve()
    generate_fixtures(data_dir)
    workers = int(getattr(args, "workers", 3))
    if workers < 2:
        raise ValueError("The crash demo needs at least two workers")
    with tempfile.TemporaryDirectory(prefix="processes-", dir=output) as temporary:
        with ProcessCluster(
            Path(temporary), data_dir=data_dir, lease_seconds=args.lease_seconds
        ) as cluster:
            cluster.start_workers(workers)
            evidence = _crash_probe(
                cluster, count=max(workers, int(getattr(args, "jobs", 9))), replace=False
            )
            # Copy small diagnostic logs out before the temporary DB is removed.
            for log in cluster.directory.glob("*.log"):
                (output / log.name).write_bytes(log.read_bytes())
    evidence["processes_cleaned_up"] = True
    evidence["architecture"] = "one coordinator, one SQLite database, independent HTTP workers"
    path = output / "crash-demo.json"
    path.write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    print(
        f"Killed {evidence['worker_id']} (PID {evidence['worker_pid']}) while it owned "
        f"job {evidence['job_id']}.\n"
        f"Reclaimed after {evidence['reclaim_seconds']:.3f}s; completed after "
        f"{evidence['recovery_seconds']:.3f}s with {evidence['attempt_count']} attempts "
        f"and {evidence['successful_state_transitions']} successful state transition.\n"
        f"All {evidence['accepted_jobs']} accepted jobs succeeded. All children cleaned up.\n"
        f"Evidence: {path}"
    )
    return evidence


def _percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _job_metrics(job: dict[str, Any], submission: dict[str, Any]) -> dict[str, Any]:
    queued_at = job["created_at"]
    queue_seconds = 0.0
    running_seconds = 0.0
    handler_seconds = 0.0
    missing_measurements = 0
    for attempt in job["attempts"]:
        queue_seconds += max(0.0, attempt["started_at"] - queued_at)
        finish = attempt["finished_at"]
        if finish is not None:
            running_seconds += max(0.0, finish - attempt["started_at"])
            queued_at = finish
        execution_ms = attempt.get("execution_ms")
        if execution_ms is None:
            missing_measurements += 1
        else:
            handler_seconds += execution_ms / 1000
    return {
        "job_id": job["id"],
        "kind": job["kind"],
        "status": job["status"],
        "attempt_count": job["attempt_count"],
        "retries": max(0, job["attempt_count"] - 1),
        "client_submit_started_at": submission["client_submit_started_at"],
        "client_submission_received_at": submission["client_submission_received_at"],
        "created_at": job["created_at"],
        "completed_at": job["completed_at"],
        "latency_seconds": job["completed_at"] - submission["client_submit_started_at"],
        "queue_seconds": queue_seconds,
        "running_seconds": running_seconds,
        "handler_seconds": handler_seconds,
        "unmeasured_handler_attempts": missing_measurements,
    }


def _summarize(rows: list[dict[str, Any]], elapsed: float) -> dict[str, Any]:
    successes = [row for row in rows if row["status"] == "succeeded"]
    latencies = [row["latency_seconds"] for row in successes]
    return {
        "jobs": len(rows),
        "succeeded": len(successes),
        "failed": len(rows) - len(successes),
        "retries": sum(row["retries"] for row in rows),
        "elapsed_seconds": elapsed,
        "completions_per_second": len(successes) / elapsed if elapsed else 0,
        "latency_median_seconds": statistics.median(latencies) if latencies else 0,
        "latency_p95_seconds": _percentile(latencies, 0.95),
        "queue_median_seconds": statistics.median(row["queue_seconds"] for row in rows),
        "queue_p95_seconds": _percentile([row["queue_seconds"] for row in rows], 0.95),
        "handler_median_seconds": statistics.median(row["handler_seconds"] for row in rows),
        "handler_p95_seconds": _percentile([row["handler_seconds"] for row in rows], 0.95),
        "unmeasured_handler_attempts": sum(row["unmeasured_handler_attempts"] for row in rows),
    }


def _workload(name: str, args: Any) -> list[dict[str, Any]]:
    if name == "realistic":
        return [
            {"kind": "checksum", "payload": {"path": "fixtures/sample.bin"}, "max_attempts": 3}
            if index % 2 == 0
            else {
                "kind": "thumbnail",
                "payload": {"path": "fixtures/sample.png", "width": 128, "height": 128},
                "max_attempts": 3,
            }
            for index in range(args.jobs)
        ]
    return [
        _synthetic(args.duration_ms, iterations=int(getattr(args, "iterations", 0)))
        for _ in range(args.jobs)
    ]


def _sequential_run(
    inputs: list[dict[str, Any]], data_dir: Path, warmup: int
) -> tuple[list[dict[str, Any]], float]:
    from queueworks.handlers import execute

    def handle(job: dict[str, Any], job_id: str) -> dict[str, Any]:
        return execute(
            job["kind"],
            job["payload"],
            data_dir=data_dir,
            job_id=job_id,
            attempt=1,
            token="sequential",
            cancel=threading.Event(),
        )

    for index in range(warmup):
        handle(inputs[index % len(inputs)], f"warmup-{index}")
    # A fixed batch exists at this instant. Sequential latency therefore includes
    # time waiting for earlier inputs, just as the queue's batch does.
    batch_start = time.time()
    monotonic_start = time.perf_counter()
    rows = []
    for index, job in enumerate(inputs):
        started_at = time.time()
        handler_start = time.perf_counter()
        handle(job, f"sequential-{index}")
        duration = time.perf_counter() - handler_start
        finished_at = time.time()
        rows.append(
            {
                "job_id": f"sequential-{index}",
                "kind": job["kind"],
                "status": "succeeded",
                "attempt_count": 1,
                "retries": 0,
                "client_submit_started_at": batch_start,
                "client_submission_received_at": batch_start,
                "created_at": batch_start,
                "completed_at": finished_at,
                "latency_seconds": finished_at - batch_start,
                "queue_seconds": started_at - batch_start,
                "running_seconds": finished_at - started_at,
                "handler_seconds": duration,
                "unmeasured_handler_attempts": 0,
            }
        )
    return rows, time.perf_counter() - monotonic_start


def _queue_run(
    cluster: ProcessCluster, inputs: list[dict[str, Any]], warmup: int
) -> tuple[list[dict[str, Any]], float, list[dict[str, Any]]]:
    warmup_ids = [cluster.submit(inputs[index % len(inputs)])["id"] for index in range(warmup)]
    if warmup_ids:
        cluster.wait_for_jobs(warmup_ids)
    submissions = []
    for job in inputs:
        started_at = time.time()
        accepted = cluster.submit(job)
        submissions.append(
            {
                "id": accepted["id"],
                "client_submit_started_at": started_at,
                "client_submission_received_at": time.time(),
            }
        )
    jobs = cluster.wait_for_jobs([submission["id"] for submission in submissions], timeout=120)
    elapsed = max(job["completed_at"] for job in jobs) - submissions[0]["client_submit_started_at"]
    return (
        [_job_metrics(job, submission) for job, submission in zip(jobs, submissions, strict=True)],
        elapsed,
        jobs,
    )


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]) if rows else [])
        writer.writeheader()
        writer.writerows(rows)


def _plot(
    path: Path,
    summaries: list[dict[str, Any]],
    *,
    metric: str = "completions_per_second",
    title: str = "Successful completions per second (each repetition)",
) -> None:
    """A portable SVG; preserve raw measurements instead of hiding variation."""
    queue = [row for row in summaries if row["mode"] == "queue"]
    width, height = 780, 390
    margin = 75
    max_value = max(row[metric] for row in queue) * 1.15 or 1
    bar_width = (width - 2 * margin) / max(1, len(queue))
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<g font-family="sans-serif" font-size="12" fill="#172033">',
        f'<text x="40" y="27" font-size="18">{html.escape(title)}</text>',
        f'<path d="M{margin} 50 V{height - margin} H{width - 20}" fill="none" stroke="#555"/>',
    ]
    for tick in range(6):
        value = max_value * tick / 5
        y = height - margin - (height - margin - 50) * tick / 5
        precision = 3 if "seconds" in metric else 1
        parts.append(
            f'<text x="{margin - 8}" y="{y + 4:.1f}" text-anchor="end">{value:.{precision}f}</text>'
        )
        parts.append(f'<path d="M{margin} {y:.1f} H{width - 20}" stroke="#eee"/>')
    for index, row in enumerate(queue):
        x = margin + index * bar_width + 7
        bar_height = row[metric] / max_value * (height - margin - 50)
        y = height - margin - bar_height
        color = "#3479ab" if row["workload"] == "realistic" else "#e18532"
        label = f"{row['workers']}w r{row['repetition']}"
        parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(2, bar_width - 14):.1f}" '
            f'height="{bar_height:.1f}" fill="{color}"/>'
        )
        parts.append(f'<text x="{x:.1f}" y="{height - margin + 18}">{html.escape(label)}</text>')
    parts.append(
        '<text x="75" y="370">Blue: checksum/thumbnail | Orange: synthetic '
        "| No speedup assumed</text>"
    )
    parts.append("</g></svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def _report(metadata: dict[str, Any], summaries: list[dict[str, Any]]) -> str:
    lines = [
        "# QueueWorks local benchmark",
        "",
        f"Measured at {metadata['measured_at_utc']} on {metadata['os']}; "
        f"CPU: {metadata['processor']}; logical CPUs: {metadata['logical_cpus']}; "
        f"Python: {metadata['python']}.",
        "",
        f"Workloads: {metadata['jobs_per_run']} identical inputs per run; "
        f"{metadata['warmup_jobs']} excluded warm-up jobs; "
        f"{metadata['repetitions']} repetitions. "
        f"Synthetic: {metadata['synthetic_duration_ms']} ms minimum duration with "
        f"{metadata['synthetic_iterations']} Python integer-loop iterations per job.",
        "",
        "Realistic inputs alternate SHA-256 of the generated 2 MiB file and a thumbnail "
        "bounded to 128×128 of the generated 1024×768 PNG. Both run through the same handlers "
        "in the sequential reference and workers. See metadata.json for the configuration.",
        "",
        "| Workload | Mode | Workers | Run | Success/s | Median latency (s) | p95 latency (s) "
        "| Median queue (s) | Median handler (s) | Failed | Retries | Crash recovery (s) |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        recovery = row.get("crash_recovery_seconds")
        recovery_text = f"{recovery:.3f}" if recovery is not None else "—"
        lines.append(
            f"| {row['workload']} | {row['mode']} | {row['workers']} | {row['repetition']} | "
            f"{row['completions_per_second']:.2f} | {row['latency_median_seconds']:.4f} | "
            f"{row['latency_p95_seconds']:.4f} | {row['queue_median_seconds']:.4f} | "
            f"{row['handler_median_seconds']:.4f} | {row['failed']} | {row['retries']} | "
            f"{recovery_text} |"
        )
    lines.extend(
        [
            "",
            "![Measured queue throughput](throughput.svg)",
            "",
            "![Measured p95 submission-to-completion latency](latency.svg)",
            "",
            "## Timing and limits",
            "",
            "Queue throughput uses the first client's POST start to the last stored terminal "
            "timestamp, excluding startup and warm-up. Latency uses each client's POST start "
            "to its stored completion timestamp; successful jobs only. Queue residence sums "
            "created→first attempt start and previous finish→next start, including retry backoff. "
            "Handler time sums worker-measured execution milliseconds; a killed process cannot "
            "report its execution duration. Raw rows also retain running lease-occupation time. "
            "Handler durations use high-resolution perf_counter; client/coordinator timestamps "
            "use this one machine's wall clock. p95 linearly interpolates ordered observations.",
            "",
            f"The wall clock reports {metadata['wall_clock']['resolution_seconds'] * 1000:.3f} ms "
            "resolution on this environment. Latency, queue residence and recovery timestamps "
            "inherit that granularity. Handler perf_counter reports "
            f"{metadata['handler_timer']['resolution_seconds'] * 1e9:.0f} ns resolution; "
            "clock resolution is not a guarantee of measurement accuracy.",
            "",
            "Crash recovery is a separate 1100 ms synthetic probe after each measured queue "
            "run: kill an observed running job's owner, start one replacement, and measure "
            "kill→successful completion. The replacement preserves the configured worker count "
            "and is necessary for the one-worker case. Probes do not affect workload throughput, "
            "failures, or retry counts. Their full attempts are saved in crash-probes.json.",
            "",
            "The sequential reference calls the same handlers directly without HTTP, SQLite, "
            "leases, or durability. A batch becomes available at the start, so sequential latency "
            "includes waiting behind earlier jobs. Its throughput is a baseline for handler cost, "
            "not an equivalent durable queue. Repeated fixtures benefit from OS caching; small "
            "local runs are sensitive to scheduling. Submissions are serial HTTP POSTs; SQLite "
            "writes serialize; coordinator and client overhead can dominate fast handlers. "
            "Synthetic waiting can scale with workers without proving CPU-work speedup. "
            "No memory usage is reported because it was not measured reliably.",
            "",
        ]
    )
    for name in sorted({row["workload"] for row in summaries}):
        modes = [row for row in summaries if row["workload"] == name]
        sequential = statistics.mean(
            row["completions_per_second"] for row in modes if row["mode"] == "sequential"
        )
        queue_rows = [row for row in modes if row["mode"] == "queue"]
        best = max(queue_rows, key=lambda row: row["completions_per_second"])
        lines.append(
            f"Observed {name}: sequential mean {sequential:.2f} success/s; best queue run "
            f"{best['completions_per_second']:.2f} success/s with {best['workers']} workers. "
            "These measured values include overhead; they do not imply a guaranteed speedup."
        )
        lines.append("")
        for repetition in sorted({row["repetition"] for row in queue_rows}):
            ordered = sorted(
                [row for row in queue_rows if row["repetition"] == repetition],
                key=lambda row: row["workers"],
            )
            for previous, current in zip(ordered, ordered[1:], strict=False):
                if current["completions_per_second"] < previous["completions_per_second"]:
                    lines.append(
                        f"Unfavorable result: {name}, repetition {repetition}, "
                        f"{current['workers']} workers achieved "
                        f"{current['completions_per_second']:.2f} success/s versus "
                        f"{previous['completions_per_second']:.2f} with {previous['workers']}. "
                        "More workers did not always improve throughput in these local runs."
                    )
                    lines.append("")
    return "\n".join(lines)


def _processor_name() -> str:
    if os.name == "nt":
        # This single read-only registry value gives a CPU model rather than
        # platform.processor()'s generic Windows architecture/family identifier.
        import winreg

        try:
            with winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"HARDWARE\DESCRIPTION\System\CentralProcessor\0",
            ) as key:
                return winreg.QueryValueEx(key, "ProcessorNameString")[0].strip()
        except OSError:
            pass
    return platform.processor() or "not reported by OS"


def run_benchmark(args: Any) -> dict[str, Any]:
    """Measure independent fresh databases, preserve raw data, never invent results."""
    from queueworks.handlers import generate_fixtures

    _validate_lease(args.lease_seconds)
    output = Path(getattr(args, "output", "artifacts/benchmark")).resolve()
    output.mkdir(parents=True, exist_ok=True)
    data_dir = Path(args.data_dir).resolve()
    generate_fixtures(data_dir)
    workers = list(getattr(args, "workers", [1, 2, 4]))
    repetitions = int(getattr(args, "repetitions", 2))
    warmup = int(getattr(args, "warmup", 4))
    if args.jobs < 1 or repetitions < 1 or warmup < 0 or any(count < 1 for count in workers):
        raise ValueError("Jobs, repetitions and workers must be positive; warmup may be zero")
    workload_names = ["realistic", "synthetic"] if args.workload == "all" else [args.workload]
    metadata = {
        "complete": False,
        "measured_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "os": platform.platform(),
        "processor": _processor_name(),
        "logical_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "workers": workers,
        "jobs_per_run": args.jobs,
        "warmup_jobs": warmup,
        "repetitions": repetitions,
        "synthetic_duration_ms": args.duration_ms,
        "synthetic_iterations": int(getattr(args, "iterations", 0)),
        "lease_seconds": args.lease_seconds,
        "retry_base_seconds": 0.05,
        "retry_cap_seconds": 1,
        "reaper_interval_seconds": 0.05,
        "sqlite": "WAL, synchronous FULL; see docs/design.md",
        "sqlite_runtime_version": sqlite3.sqlite_version,
        "sqlite_busy_timeout_ms": 5000,
        "worker_poll_interval_seconds": 0.1,
        "worker_request_timeout_seconds": 2,
        "max_attempts": 3,
        "memory_usage": "not measured",
        "handler_timer": {
            "clock": "perf_counter",
            "implementation": time.get_clock_info("perf_counter").implementation,
            "resolution_seconds": time.get_clock_info("perf_counter").resolution,
        },
        "wall_clock": {
            "implementation": time.get_clock_info("time").implementation,
            "resolution_seconds": time.get_clock_info("time").resolution,
        },
        "workloads": workload_names,
        "fixture_sizes_bytes": {
            name: (data_dir / "fixtures" / name).stat().st_size
            for name in ["sample.bin", "sample.png"]
        },
        "reproduction_arguments": [
            "python",
            "-m",
            "queueworks",
            "benchmark",
            "--workload",
            args.workload,
            "--workers",
            *map(str, workers),
            "--jobs",
            str(args.jobs),
            "--repetitions",
            str(repetitions),
            "--warmup",
            str(warmup),
            "--duration-ms",
            str(args.duration_ms),
            "--iterations",
            str(getattr(args, "iterations", 0)),
            "--lease-seconds",
            str(args.lease_seconds),
            "--data-dir",
            str(args.data_dir),
            "--output-dir",
            str(args.output),
        ],
    }
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    summaries = []
    raw_rows = []
    raw_jobs = []
    probes = []
    for name in workload_names:
        # Direct sequential execution must enforce the same bounded payloads as
        # HTTP submissions, including synthetic duration/iteration limits.
        inputs = [JobSubmit.model_validate(job).model_dump() for job in _workload(name, args)]
        for repetition in range(1, repetitions + 1):
            rows, elapsed = _sequential_run(inputs, data_dir, warmup)
            identity = {
                "workload": name,
                "mode": "sequential",
                "workers": 0,
                "repetition": repetition,
            }
            raw_rows.extend({**identity, **row} for row in rows)
            summaries.append(
                {**identity, **_summarize(rows, elapsed), "crash_recovery_seconds": None}
            )
            for count in workers:
                with tempfile.TemporaryDirectory(prefix="run-", dir=output) as temporary:
                    with ProcessCluster(
                        Path(temporary), data_dir=data_dir, lease_seconds=args.lease_seconds
                    ) as cluster:
                        cluster.start_workers(count)
                        rows, elapsed, jobs = _queue_run(cluster, inputs, warmup)
                        probe = _crash_probe(cluster, count=count, replace=True)
                identity = {
                    "workload": name,
                    "mode": "queue",
                    "workers": count,
                    "repetition": repetition,
                }
                raw_rows.extend({**identity, **row} for row in rows)
                raw_jobs.append({**identity, "jobs": jobs})
                probes.append({**identity, **probe})
                summary = {
                    **identity,
                    **_summarize(rows, elapsed),
                    "crash_recovery_seconds": probe["recovery_seconds"],
                }
                summaries.append(summary)
                print(
                    f"{name}: {count} workers, run {repetition}: "
                    f"{summary['completions_per_second']:.2f} success/s, "
                    f"p95 {summary['latency_p95_seconds']:.3f}s, "
                    f"{summary['failed']} failures, {summary['retries']} retries; "
                    f"crash recovery {probe['recovery_seconds']:.3f}s",
                    flush=True,
                )
                # Save after each completed run; interrupted experiments retain
                # real observations and are distinguishable by metadata + rows.
                for filename, value in [
                    ("metadata.json", metadata),
                    ("summary.json", summaries),
                    ("raw-jobs.json", raw_jobs),
                    ("crash-probes.json", probes),
                ]:
                    (output / filename).write_text(
                        json.dumps(value, indent=2) + "\n", encoding="utf-8"
                    )
                _write_csv(output / "raw.csv", raw_rows)
                _write_csv(output / "summary.csv", summaries)
    _plot(output / "throughput.svg", summaries)
    _plot(
        output / "latency.svg",
        summaries,
        metric="latency_p95_seconds",
        title="p95 submission-to-completion latency, seconds (each repetition)",
    )
    (output / "report.md").write_text(_report(metadata, summaries), encoding="utf-8")
    metadata["complete"] = True
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(f"Raw CSV/JSON, report and SVG: {output}")
    return {"metadata": metadata, "summaries": summaries, "output": str(output)}
