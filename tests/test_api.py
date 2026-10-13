"""API validation and token fencing against real SQLite with a controlled clock."""

import concurrent.futures
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from queueworks.api import create_app
from queueworks.config import Settings
from queueworks.handlers import generate_fixtures
from queueworks.models import JobSubmit
from queueworks.store import Store


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def system(tmp_path):
    settings = Settings(
        db_path=tmp_path / "jobs.sqlite3",
        data_dir=tmp_path / "data",
        lease_seconds=3,
        retry_base_seconds=1,
        retry_cap_seconds=2,
        reaper_interval=3600,
    )
    generate_fixtures(settings.data_dir)
    clock = Clock()
    store = Store(settings, clock)
    with TestClient(create_app(settings, store)) as client:
        yield client, store, clock, settings


SPEC = {"kind": "synthetic", "payload": {"duration_ms": 0}, "max_attempts": 3}


def submit(client, spec=None):
    response = client.post("/jobs", json=spec or SPEC)
    assert response.status_code == 201, response.text
    return response.json()


def claim(client, worker="worker"):
    response = client.post("/internal/claim", json={"worker_id": worker})
    assert response.status_code == 200, response.text
    return response.json()["job"]


def update(client, job, action, **kwargs):
    return client.post(
        f"/internal/jobs/{job['id']}/{action}", json={"token": job["attempt_token"], **kwargs}
    )


def test_submission_status_health_and_snapshot(system):
    client, store, _, _ = system
    assert client.get("/health").json() == {"status": "ready", "database": "ready"}
    job = submit(client)
    assert job["status"] == "queued" and job["attempt_count"] == 0
    assert job["completed_at"] is None
    assert client.get(f"/jobs/{job['id']}").json() == job
    assert client.get("/jobs/missing").status_code == 404
    with store.connection() as conn:
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000


@pytest.mark.parametrize(
    "spec",
    [
        {"kind": "python", "payload": {"code": "print(1)"}},
        {"kind": "checksum", "payload": {"path": "../secret"}},
        {"kind": "checksum", "payload": {"path": "fixtures/../../secret"}},
        {"kind": "checksum", "payload": {"path": "C:/secret"}},
        {"kind": "checksum", "payload": {"path": "fixtures/a\u0000.bin"}},
        {"kind": "checksum", "payload": {"path": "fixtures/missing.bin"}},
        {"kind": "thumbnail", "payload": {"path": "fixtures/sample.png", "width": 0}},
        {"kind": "synthetic", "payload": {"duration_ms": 30001}},
        {"kind": "synthetic", "payload": {"duration_ms": True}},
        {"kind": "synthetic", "payload": {"command": "whoami"}},
        {**SPEC, "max_attempts": 0},
        {**SPEC, "max_attempts": 11},
        {**SPEC, "surprise": True},
    ],
)
def test_invalid_submission(system, spec):
    assert system[0].post("/jobs", json=spec).status_code == 422
    assert system[0].get("/jobs").json()["total"] == 0


def test_valid_file_jobs(system):
    for kind, path in (("checksum", "sample.bin"), ("thumbnail", "sample.png")):
        job = submit(system[0], {"kind": kind, "payload": {"path": f"fixtures/{path}"}})
        assert job["kind"] == kind


def test_pagination_and_filter(system):
    client, _, clock, _ = system
    ids = []
    for _ in range(5):
        ids.append(submit(client)["id"])
        clock.advance(1)
    active = claim(client)
    page = client.get("/jobs", params={"limit": 2, "offset": 2}).json()
    assert [j["id"] for j in page["items"]] == ids[2:4]
    assert page["total"] == 5
    assert (
        client.get("/jobs", params={"status": "running"}).json()["items"][0]["id"] == active["id"]
    )
    assert client.get("/jobs", params={"status": "queued"}).json()["total"] == 4
    for query in ({"limit": 101}, {"limit": 0}, {"offset": -1}, {"status": "unknown"}):
        assert client.get("/jobs", params=query).status_code == 422


def test_idempotency_normalizes_defaults_and_conflicts(system):
    client = system[0]
    headers = {"Idempotency-Key": "submission-1"}
    first = client.post("/jobs", json=SPEC, headers=headers)
    second = client.post(
        "/jobs",
        json={**SPEC, "payload": {"duration_ms": 0, "iterations": 0, "fail_until_attempt": 0}},
        headers=headers,
    )
    assert first.status_code == 201 and second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert (
        client.post("/jobs", json={**SPEC, "max_attempts": 2}, headers=headers).status_code == 409
    )
    assert client.get("/jobs").json()["total"] == 1
    assert client.post("/jobs", json=SPEC, headers={"Idempotency-Key": ""}).status_code == 422


def test_concurrent_idempotency_is_atomic(system):
    store = system[1]

    def accept(_):
        return store.submit(JobSubmit.model_validate(SPEC), "same-key")

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(accept, range(16)))
    assert len({job["id"] for job, _ in results}) == 1
    assert sum(created for _, created in results) == 1


def test_idempotency_survives_removed_fixture(system):
    client, _, _, settings = system
    spec = {"kind": "checksum", "payload": {"path": "fixtures/sample.bin"}}
    headers = {"Idempotency-Key": "file-submission"}
    first = client.post("/jobs", json=spec, headers=headers)
    assert first.status_code == 201
    (settings.data_dir / "fixtures" / "sample.bin").unlink()
    repeated = client.post("/jobs", json=spec, headers=headers)
    assert repeated.status_code == 200
    assert repeated.json()["id"] == first.json()["id"]
    assert client.post("/jobs", json=spec).status_code == 422


def test_heartbeat_renewal_and_exact_expiry(system):
    client, store, clock, _ = system
    submit(client)
    job = claim(client)
    clock.advance(2)
    response = update(client, job, "heartbeat")
    assert response.json()["lease_expires_at"] == clock.now + 3
    clock.advance(2)
    assert store.reap() == 0
    clock.advance(1)
    assert update(client, job, "heartbeat").status_code == 409
    assert store.get(job["id"])["status"] == "queued"
    assert store.get(job["id"])["attempts"][0]["status"] == "expired"


def test_retry_backoff_caps_and_permanent_failure(system):
    client, store, clock, _ = system
    submitted = submit(client, {**SPEC, "max_attempts": 4})
    for expected_attempt, delay in enumerate((1, 2, 2, 2), start=1):
        job = claim(client)
        assert job["attempt_count"] == expected_attempt
        response = update(client, job, "fail", error="injected failure", execution_ms=5)
        assert response.status_code == 200
        record = store.get(submitted["id"])
        if expected_attempt < 4:
            assert record["status"] == "queued"
            assert record["eligible_at"] == clock.now + delay
            assert claim(client) is None
            clock.advance(delay)
        else:
            assert record["status"] == "failed"
            assert record["completed_at"] == clock.now
    assert len(record["attempts"]) == 4
    assert claim(client) is None


def test_reassigned_attempt_fences_every_update_and_duplicate_success(system):
    client, store, clock, _ = system
    submit(client)
    old = claim(client, "old")
    clock.advance(3)
    # Completion itself expires the attempt, even before a background reaper tick.
    assert update(client, old, "complete", result={}, execution_ms=1).status_code == 409
    clock.advance(1)
    current = claim(client, "new")
    assert old["attempt_token"] != current["attempt_token"]
    assert old["attempt_count"] == 1 and current["attempt_count"] == 2
    for action, body in (
        ("heartbeat", {}),
        ("complete", {"result": {}, "execution_ms": 1}),
        ("fail", {"error": "stale failure", "execution_ms": 1}),
    ):
        assert update(client, old, action, **body).status_code == 409
    assert (
        update(client, current, "complete", result={"answer": 42}, execution_ms=2).status_code
        == 200
    )
    assert (
        update(client, current, "complete", result={"answer": 43}, execution_ms=3).status_code
        == 409
    )
    assert update(client, current, "heartbeat").status_code == 409
    final = store.get(current["id"])
    assert final["result"] == {"answer": 42} and final["error"] is None
    assert [a["status"] for a in final["attempts"]] == ["expired", "succeeded"]
    assert sum(e["to_status"] == "succeeded" for e in final["events"]) == 1
    assert "attempt_token" not in final
    assert all("token" not in a for a in final["attempts"])


def test_retry_then_success(system):
    client, store, clock, _ = system
    submit(client)
    first = claim(client)
    assert (
        update(client, first, "fail", error="temporary", execution_ms=10).json()["status"]
        == "queued"
    )
    clock.advance(1)
    second = claim(client)
    assert (
        update(client, second, "complete", result={"ok": True}, execution_ms=15).json()["status"]
        == "succeeded"
    )
    assert store.get(second["id"])["attempt_count"] == 2


def test_startup_reaps_durable_expired_attempt_and_keeps_result(system):
    client, store, clock, settings = system
    completed = submit(client)
    job = claim(client)
    update(client, job, "complete", result={"durable": True}, execution_ms=1)
    pending = submit(client)
    claim(client)
    clock.advance(10)
    restarted = Store(settings, clock)
    restarted.initialize()
    assert restarted.get(completed["id"])["result"] == {"durable": True}
    assert restarted.get(pending["id"])["attempts"][0]["status"] == "expired"
    clock.advance(1)
    new = restarted.claim("restarted-worker")
    assert new["id"] == pending["id"] and new["attempt_count"] == 2


def test_internal_key_and_invalid_worker_messages(system):
    _, store, _, settings = system
    with TestClient(create_app(replace(settings, worker_key="secret"), store)) as client:
        assert client.post("/internal/claim", json={"worker_id": "w"}).status_code == 401
        headers = {"X-Worker-Key": "secret"}
        assert (
            client.post("/internal/claim", json={"worker_id": "w"}, headers=headers).status_code
            == 200
        )
        assert (
            client.post("/internal/claim", json={"worker_id": ""}, headers=headers).status_code
            == 422
        )
        assert (
            client.post(
                "/internal/jobs/unknown/complete",
                headers=headers,
                json={"token": "x", "result": {}, "execution_ms": -1},
            ).status_code
            == 422
        )
        assert client.get("/health").status_code == 200


def test_non_finite_worker_results_are_validation_errors(system):
    client = system[0]
    submit(client)
    job = claim(client)
    response = client.post(
        f"/internal/jobs/{job['id']}/complete",
        content='{"token":"' + job["attempt_token"] + '","result":{"x":NaN},"execution_ms":0}',
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422
    assert client.get(f"/jobs/{job['id']}").json()["status"] == "running"


def test_last_attempt_expiry_is_terminal(system):
    client, store, clock, _ = system
    submitted = submit(client, {**SPEC, "max_attempts": 1})
    claim(client)
    clock.advance(3)
    assert store.reap() == 1
    final = store.get(submitted["id"])
    assert final["status"] == "failed" and final["error"] == "Worker lease expired"
    assert store.reap() == 0
