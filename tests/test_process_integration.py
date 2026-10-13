"""Recovery tests use real coordinator and worker processes against SQLite."""

import time

import pytest
from process_helpers import ProcessCluster, poll_until

pytestmark = pytest.mark.integration


def synthetic(duration_ms=150, **payload):
    return {
        "kind": "synthetic",
        "payload": {"duration_ms": duration_ms, "iterations": 0, **payload},
        "max_attempts": 3,
    }


def running_job(cluster, job_id):
    job = cluster.job(job_id)
    return job if job["status"] == "running" else False


def test_concurrent_real_workers_claim_distinct_attempts(tmp_path):
    # Allow CI scheduling delays when checking claim isolation; expiry has separate tests.
    with ProcessCluster(tmp_path, lease_seconds=5.0) as cluster:
        cluster.start_workers(3)
        ids = [cluster.submit(synthetic(250))["id"] for _ in range(12)]
        jobs = cluster.wait_for_jobs(ids, timeout=30)
        assert all(job["status"] == "succeeded" for job in jobs)
        assert all(job["attempt_count"] == 1 for job in jobs)
        assert all(len(job["attempts"]) == 1 for job in jobs)
        assert all(job["attempts"][0]["status"] == "succeeded" for job in jobs)
        assert len({job["attempts"][0]["worker_id"] for job in jobs}) == 3
        assert all(
            sum(event["to_status"] == "succeeded" for event in job["events"]) == 1 for job in jobs
        )


def test_hard_worker_termination_recovers_all_accepted_jobs(tmp_path):
    with ProcessCluster(tmp_path, lease_seconds=0.6) as cluster:
        cluster.start_workers(2)
        ids = [cluster.submit(synthetic(900))["id"] for _ in range(3)]

        def select_running():
            return next((job for job_id in ids if (job := running_job(cluster, job_id))), False)

        selected = poll_until(select_running, timeout=15, description="owned running job")
        owner = selected.get("worker_id") or selected["attempts"][-1]["worker_id"]
        killed_at = cluster.kill_worker(owner)
        jobs = cluster.wait_for_jobs(ids, timeout=30)
        recovered = next(job for job in jobs if job["id"] == selected["id"])
        assert all(job["status"] == "succeeded" for job in jobs)
        assert recovered["attempt_count"] == 2
        assert [attempt["status"] for attempt in recovered["attempts"]] == ["expired", "succeeded"]
        assert recovered["completed_at"] > killed_at
        assert recovered["attempts"][1]["worker_id"] != owner
        assert sum(event["to_status"] == "succeeded" for event in recovered["events"]) == 1


def test_coordinator_restart_preserves_accepted_and_expired_work(tmp_path):
    with ProcessCluster(tmp_path, lease_seconds=0.5) as cluster:
        completed_id = cluster.submit(synthetic(10))["id"]
        cluster.start_workers(1)
        completed = cluster.wait_for_jobs([completed_id])[0]
        running_id = cluster.submit(synthetic(800))["id"]
        selected = poll_until(
            lambda: running_job(cluster, running_id), timeout=15, description="job before restart"
        )
        cluster.kill_worker(selected["attempts"][-1]["worker_id"])
        queued_id = cluster.submit(synthetic(20))["id"]
        assert cluster.coordinator is not None
        cluster.stop_process(cluster.coordinator)
        # Poll a clock boundary to guarantee the persisted lease has expired
        # while there is no coordinator; no assumption about startup duration.
        expiry = selected["attempts"][-1]["lease_expires_at"]
        poll_until(
            lambda: time.time() > expiry, timeout=3, description="persisted lease expiration"
        )
        cluster.start_coordinator()
        assert cluster.job(completed_id)["result"] == completed["result"]
        assert cluster.job(completed_id)["attempt_count"] == 1
        cluster.start_worker("replacement")
        jobs = cluster.wait_for_jobs([running_id, queued_id])
        assert all(job["status"] == "succeeded" for job in jobs)
        assert jobs[0]["attempt_count"] == 2
        assert jobs[0]["attempts"][0]["status"] == "expired"
        assert jobs[1]["attempt_count"] == 1


def test_long_handler_heartbeat_and_http_retry_limit(tmp_path):
    with ProcessCluster(tmp_path, lease_seconds=0.45, retry_base_seconds=0.03) as cluster:
        cluster.start_workers(2)
        long_id = cluster.submit(synthetic(1400))["id"]
        retry_id = cluster.submit(synthetic(5, fail_until_attempt=2))["id"]
        failed_id = cluster.submit(synthetic(5, fail_until_attempt=10))["id"]
        long_job, retried, failed = cluster.wait_for_jobs([long_id, retry_id, failed_id])
        assert long_job["status"] == "succeeded"
        assert long_job["attempt_count"] == 1
        assert long_job["attempts"][0]["execution_ms"] >= 1200
        assert retried["status"] == "succeeded"
        assert retried["attempt_count"] == 3
        assert [attempt["status"] for attempt in retried["attempts"]] == [
            "failed",
            "failed",
            "succeeded",
        ]
        assert failed["status"] == "failed"
        assert failed["attempt_count"] == 3
        assert len(failed["attempts"]) == 3
