"""Graceful cancellation returns a live attempt through the real HTTP API."""

from __future__ import annotations

import os
import signal
import threading
import time
from pathlib import Path

import pytest

from queueworks.experiments import ProcessCluster, poll_until
from queueworks.worker import Worker

pytestmark = pytest.mark.integration


def _submit_long_job(cluster: ProcessCluster) -> str:
    return cluster.submit(
        {
            "kind": "synthetic",
            "payload": {"duration_ms": 3000},
            "max_attempts": 3,
        }
    )["id"]


def _wait_running(cluster: ProcessCluster, job_id: str) -> dict:
    def running() -> dict | bool:
        job = cluster.job(job_id)
        return job if job["status"] == "running" else False

    return poll_until(running, timeout=10, description="running job before graceful stop")


def _assert_cancelled_and_recover(cluster: ProcessCluster, job_id: str) -> None:
    cancelled = cluster.job(job_id)
    assert cancelled["status"] == "queued"
    assert cancelled["attempt_count"] == 1
    assert cancelled["attempts"][0]["status"] == "failed"
    assert "HandlerCancelled" in cancelled["attempts"][0]["error"]
    cluster.start_worker("replacement")
    completed = cluster.wait_for_jobs([job_id], timeout=15)[0]
    assert completed["status"] == "succeeded"
    assert completed["attempt_count"] == 2
    assert [attempt["status"] for attempt in completed["attempts"]] == ["failed", "succeeded"]
    assert sum(event["to_status"] == "succeeded" for event in completed["events"]) == 1


def test_worker_request_stop_is_bounded_and_recovers(tmp_path: Path) -> None:
    # Use the same shutdown request that a signal handler issues. This portable
    # test checks the cleanup logic on Windows as well as POSIX; HTTP and SQLite
    # are real and the replacement is a separate worker process.
    with ProcessCluster(tmp_path, lease_seconds=5) as cluster:
        worker = Worker(cluster.url, cluster.data_dir, worker_id="graceful", shutdown_timeout=0.8)
        thread = threading.Thread(target=worker.run, daemon=True)
        thread.start()
        try:
            job_id = _submit_long_job(cluster)
            _wait_running(cluster, job_id)
            stopped_at = time.monotonic()
            worker.request_stop()
            thread.join(timeout=1.5)
            assert not thread.is_alive()
            assert time.monotonic() - stopped_at < 1.5
            _assert_cancelled_and_recover(cluster, job_id)
        finally:
            worker.request_stop()
            thread.join(timeout=2)


@pytest.mark.skipif(os.name == "nt", reason="Windows terminate() is a hard kill, not SIGTERM")
def test_sigterm_worker_process_is_graceful_and_bounded(tmp_path: Path) -> None:
    with ProcessCluster(tmp_path, lease_seconds=5) as cluster:
        process = cluster.start_worker("graceful-signal")
        job_id = _submit_long_job(cluster)
        _wait_running(cluster, job_id)
        stopped_at = time.monotonic()
        process.terminate()
        process.wait(timeout=6)
        assert process.returncode == 0
        assert time.monotonic() - stopped_at < 5.5
        _assert_cancelled_and_recover(cluster, job_id)


@pytest.mark.skipif(os.name != "nt", reason="Ctrl+Break is a Windows console signal")
def test_windows_ctrl_break_worker_process_is_graceful(tmp_path: Path) -> None:
    with ProcessCluster(tmp_path, lease_seconds=5) as cluster:
        process = cluster.start_worker("graceful-console")
        job_id = _submit_long_job(cluster)
        _wait_running(cluster, job_id)
        stopped_at = time.monotonic()
        try:
            process.send_signal(signal.CTRL_BREAK_EVENT)
        except OSError as error:
            pytest.skip(f"execution host has no usable Windows console: {error}")
        process.wait(timeout=6)
        assert process.returncode == 0
        assert time.monotonic() - stopped_at < 5.5
        _assert_cancelled_and_recover(cluster, job_id)
