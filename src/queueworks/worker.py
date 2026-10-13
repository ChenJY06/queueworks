"""Independent HTTP worker with a separate heartbeat thread.

Only the coordinator knows SQLite exists. A lease is permission to report one
attempt's result, not an exactly-once guarantee for the handler's side effects.
"""

from __future__ import annotations

import json
import math
import signal
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import httpx

from queueworks.handlers import HandlerCancelled, execute


def _log(event: str, worker_id: str, **details: Any) -> None:
    print(json.dumps({"event": event, "worker_id": worker_id, **details}), flush=True)


class Worker:
    """Claim, run, and acknowledge jobs until a shutdown signal is received."""

    def __init__(
        self,
        url: str,
        data_dir: Path,
        *,
        worker_id: str | None = None,
        poll_interval: float = 0.1,
        shutdown_timeout: float = 5.0,
        request_timeout: float = 2.0,
        worker_key: str | None = None,
    ) -> None:
        timings = (poll_interval, shutdown_timeout, request_timeout)
        if any(not math.isfinite(value) or value <= 0 for value in timings):
            raise ValueError("worker timeouts and poll interval must be finite and positive")
        safe_id_characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-"
        if worker_id is not None and (
            not worker_id
            or len(worker_id) > 100
            or any(character not in safe_id_characters for character in worker_id)
        ):
            raise ValueError("worker id must be 1-100 characters using letters, digits, _, ., or -")
        self.url = url.rstrip("/")
        self.data_dir = Path(data_dir)
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:12]}"
        self.poll_interval = poll_interval
        self.shutdown_timeout = shutdown_timeout
        self.request_timeout = request_timeout
        self.headers = {"X-Worker-Key": worker_key} if worker_key else {}
        self.stop = threading.Event()

    def request_stop(self, _signal: int | None = None, _frame: Any = None) -> None:
        """Signal handlers do no network or file work; cleanup happens in run()."""
        self.stop.set()

    def run(self) -> None:
        previous_handlers: dict[int, Any] = {}
        if threading.current_thread() is threading.main_thread():
            shutdown_signals = [signal.SIGINT, signal.SIGTERM]
            if hasattr(signal, "SIGBREAK"):
                # Windows console process groups use Ctrl+Break for a graceful
                # stop; terminate()/kill() are hard termination on Windows.
                shutdown_signals.append(signal.SIGBREAK)
            for signum in shutdown_signals:
                previous_handlers[signum] = signal.signal(signum, self.request_stop)
        _log("worker_started", self.worker_id, url=self.url)
        try:
            with httpx.Client(
                base_url=self.url,
                headers=self.headers,
                timeout=self.request_timeout,
                trust_env=False,
            ) as client:
                while not self.stop.is_set():
                    claimed_at = time.monotonic()
                    try:
                        response = client.post(
                            "/internal/claim", json={"worker_id": self.worker_id}
                        )
                        response.raise_for_status()
                        job = response.json()["job"]
                    except (httpx.HTTPError, ValueError, KeyError) as error:
                        _log("claim_unavailable", self.worker_id, error=str(error))
                        self.stop.wait(max(self.poll_interval, 0.25))
                        continue
                    if job is None:
                        self.stop.wait(self.poll_interval)
                        continue
                    self._run_attempt(client, job, claimed_at)
        finally:
            for signum, previous in previous_handlers.items():
                signal.signal(signum, previous)
            _log("worker_stopped", self.worker_id)

    def _run_attempt(self, client: httpx.Client, job: dict[str, Any], claimed_at: float) -> None:
        job_id = job["id"]
        token = job["attempt_token"]
        lease_seconds = float(job["lease_seconds"])
        cancel = threading.Event()
        if self.stop.is_set():
            cancel.set()
        lease_lost = threading.Event()
        heartbeat_stop = threading.Event()
        finished = threading.Event()
        deadline_lock = threading.Lock()
        # Starting the budget at the request start is conservative: coordinator
        # time is never assumed to agree with this machine's wall clock.
        deadline = [claimed_at + lease_seconds]
        outcome: dict[str, Any] = {}
        # perf_counter provides finer duration resolution on Windows Python
        # 3.11/3.12; monotonic remains the independent lease/shutdown clock.
        execution_started = time.perf_counter()
        cleanup_deadline: float | None = None

        def lose_lease() -> None:
            lease_lost.set()
            cancel.set()

        def heartbeat() -> None:
            interval = max(0.02, lease_seconds / 3)
            with httpx.Client(
                base_url=self.url,
                headers=self.headers,
                timeout=min(self.request_timeout, max(0.05, lease_seconds / 2)),
                trust_env=False,
            ) as heartbeat_client:
                while not heartbeat_stop.wait(interval):
                    with deadline_lock:
                        remaining = deadline[0] - time.monotonic()
                    if remaining <= 0:
                        lose_lease()
                        return
                    renewal_started = time.monotonic()
                    try:
                        response = heartbeat_client.post(
                            f"/internal/jobs/{job_id}/heartbeat", json={"token": token}
                        )
                        if response.status_code in (401, 403, 404, 409):
                            lose_lease()
                            return
                        response.raise_for_status()
                        renewed_seconds = float(response.json()["lease_seconds"])
                        with deadline_lock:
                            deadline[0] = renewal_started + renewed_seconds
                        interval = max(0.02, renewed_seconds / 3)
                    except (httpx.HTTPError, ValueError, KeyError):
                        # A lost response does not prove the renewal failed. It
                        # also does not prove it succeeded, so retain the old
                        # deadline and retry only while that budget remains.
                        interval = min(interval, 0.1)

        def run_handler() -> None:
            nonlocal execution_started
            execution_started = time.perf_counter()
            try:
                outcome["result"] = execute(
                    job["kind"],
                    job["payload"],
                    data_dir=self.data_dir,
                    job_id=job_id,
                    attempt=job["attempt_count"],
                    token=token,
                    cancel=cancel,
                )
            except Exception as error:  # A handler failure must not kill its worker.
                outcome["error"] = f"{type(error).__name__}: {error}"[:2000]
            finally:
                outcome["execution_ms"] = (time.perf_counter() - execution_started) * 1000
                finished.set()

        _log("claimed", self.worker_id, job_id=job_id, attempt=job["attempt_count"])
        heartbeat_thread = threading.Thread(target=heartbeat, name="heartbeat", daemon=True)
        execution_thread = threading.Thread(target=run_handler, name="handler", daemon=True)
        heartbeat_thread.start()
        execution_thread.start()
        try:
            while not finished.wait(0.02):
                with deadline_lock:
                    expired_locally = time.monotonic() >= deadline[0]
                if expired_locally:
                    lose_lease()
                if self.stop.is_set() or lease_lost.is_set():
                    cancel.set()
                    cleanup_deadline = time.monotonic() + self.shutdown_timeout
                    finished.wait(max(0, self.shutdown_timeout - self.request_timeout))
                    if not finished.is_set():
                        # Python cannot safely kill an arbitrary thread. Exit
                        # this process instead of starting overlapping work.
                        self.stop.set()
                    if self.stop.is_set():
                        outcome["error"] = f"{HandlerCancelled.__name__}: worker shutting down"
                    outcome.setdefault(
                        "execution_ms", (time.perf_counter() - execution_started) * 1000
                    )
                    break

            with deadline_lock:
                if time.monotonic() >= deadline[0]:
                    lose_lease()
            if lease_lost.is_set():
                _log("lease_lost", self.worker_id, job_id=job_id)
                return
            endpoint = "fail" if "error" in outcome else "complete"
            payload = {"token": token, "execution_ms": outcome["execution_ms"]}
            if endpoint == "fail":
                payload["error"] = outcome["error"]
            else:
                payload["result"] = outcome["result"]
            try:
                # No acknowledgement retry here: if the response is lost, the
                # job may already have committed. Polling/recovery is the source
                # of truth; a new attempt will be fenced by a different token.
                acknowledgement_timeout = self.request_timeout
                if cleanup_deadline is not None:
                    acknowledgement_timeout = min(
                        acknowledgement_timeout, cleanup_deadline - time.monotonic()
                    )
                    if acknowledgement_timeout <= 0:
                        return
                response = client.post(
                    f"/internal/jobs/{job_id}/{endpoint}",
                    json=payload,
                    timeout=acknowledgement_timeout,
                )
                if response.status_code == 409:
                    _log("stale_update", self.worker_id, job_id=job_id)
                else:
                    response.raise_for_status()
                    _log(
                        "completed" if endpoint == "complete" else "attempt_failed",
                        self.worker_id,
                        job_id=job_id,
                        attempt=job["attempt_count"],
                        execution_ms=round(outcome["execution_ms"], 3),
                        **({"error": outcome["error"]} if endpoint == "fail" else {}),
                    )
            except httpx.HTTPError as error:
                _log(
                    "acknowledgement_unavailable",
                    self.worker_id,
                    job_id=job_id,
                    error=str(error),
                )
        finally:
            cancel.set()
            heartbeat_stop.set()
            join_budget = self.request_timeout + 0.1
            if cleanup_deadline is not None:
                join_budget = max(0, min(join_budget, cleanup_deadline - time.monotonic()))
            heartbeat_thread.join(timeout=join_budget)
            execution_thread.join(timeout=0)
