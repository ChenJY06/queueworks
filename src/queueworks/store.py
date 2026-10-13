"""SQLite persistence. Every write owns a short transaction, never a handler execution."""

import json
import sqlite3
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from queueworks.config import Settings
from queueworks.models import JobSubmit


class NotFound(Exception):
    pass


class Conflict(Exception):
    pass


class StaleAttempt(Exception):
    pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL,
    canonical_content TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('queued','running','succeeded','failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    queued_at REAL NOT NULL,
    eligible_at REAL NOT NULL,
    first_started_at REAL,
    completed_at REAL,
    worker_id TEXT,
    attempt_token TEXT,
    lease_expires_at REAL,
    result TEXT,
    error TEXT,
    CHECK(attempt_count >= 0 AND attempt_count <= max_attempts),
    CHECK((status = 'running') = (attempt_token IS NOT NULL)),
    CHECK((status = 'running') = (lease_expires_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status, eligible_at, created_at, id);
CREATE INDEX IF NOT EXISTS jobs_expiry ON jobs(status, lease_expires_at);
CREATE TABLE IF NOT EXISTS attempts (
    job_id TEXT NOT NULL REFERENCES jobs(id),
    attempt_no INTEGER NOT NULL,
    token TEXT NOT NULL UNIQUE,
    worker_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('running','succeeded','failed','expired')),
    queued_at REAL NOT NULL,
    eligible_at REAL NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL,
    lease_expires_at REAL NOT NULL,
    execution_ms REAL,
    error TEXT,
    PRIMARY KEY(job_id, attempt_no)
);
CREATE TABLE IF NOT EXISTS events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id),
    timestamp REAL NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_job ON events(job_id, sequence);
PRAGMA user_version = 1;
"""


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


class Store:
    def __init__(self, settings: Settings, clock: Callable[[], float] = time.time):
        self.settings = settings
        self.clock = clock

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(
            self.settings.db_path,
            timeout=self.settings.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        try:
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout = {self.settings.busy_timeout_ms}")
            conn.execute(f"PRAGMA synchronous = {self.settings.synchronous}")
            conn.execute("PRAGMA foreign_keys = ON")
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def initialize(self) -> None:
        self.settings.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise RuntimeError(f"Unsupported database schema version {version}")
            mode = conn.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if mode != "wal":
                raise RuntimeError("SQLite WAL mode unavailable; use a local filesystem")
            conn.executescript(SCHEMA)
        self.reap()

    def ready(self) -> bool:
        with self.connection() as conn:
            return conn.execute("SELECT 1 FROM jobs LIMIT 1").fetchone() is not None or (
                conn.execute("SELECT 1").fetchone()[0] == 1
            )

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        job_id: str,
        now: float,
        old: str | None,
        new: str,
        attempt: int,
        reason: str,
    ) -> None:
        conn.execute(
            "INSERT INTO events(job_id,timestamp,from_status,to_status,attempt_no,reason) "
            "VALUES(?,?,?,?,?,?)",
            (job_id, now, old, new, attempt, reason),
        )

    @staticmethod
    def _record(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        job = dict(row)
        for key in ("canonical_content", "idempotency_key", "attempt_token"):
            job.pop(key)
        job["payload"] = json.loads(job["payload"])
        job["result"] = json.loads(job["result"]) if job["result"] is not None else None
        job["attempts"] = [
            {key: value for key, value in dict(a).items() if key not in {"token", "job_id"}}
            for a in conn.execute(
                "SELECT * FROM attempts WHERE job_id=? ORDER BY attempt_no", (job["id"],)
            )
        ]
        job["events"] = [
            dict(e)
            for e in conn.execute(
                "SELECT sequence,timestamp,from_status,to_status,attempt_no,reason FROM events "
                "WHERE job_id=? ORDER BY sequence",
                (job["id"],),
            )
        ]
        return job

    def submit(
        self,
        spec: JobSubmit,
        key: str | None = None,
        validate_new: Callable[[], None] | None = None,
    ) -> tuple[dict[str, Any], bool]:
        content = canonical(spec.model_dump())
        with self.transaction() as conn:
            now = self.clock()
            if key is not None:
                previous = conn.execute(
                    "SELECT * FROM jobs WHERE idempotency_key=?", (key,)
                ).fetchone()
                if previous is not None:
                    if previous["canonical_content"] != content:
                        raise Conflict("Idempotency key already belongs to a different submission")
                    return self._record(conn, previous), False
            if validate_new is not None:
                validate_new()
            job_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO jobs(id,kind,payload,canonical_content,idempotency_key,status,"
                "max_attempts,created_at,updated_at,queued_at,eligible_at) "
                "VALUES(?,?,?,?,?,'queued',?,?,?,?,?)",
                (
                    job_id,
                    spec.kind,
                    canonical(spec.payload),
                    content,
                    key,
                    spec.max_attempts,
                    now,
                    now,
                    now,
                    now,
                ),
            )
            self._event(conn, job_id, now, None, "queued", 0, "submitted")
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            return self._record(conn, row), True

    def get(self, job_id: str) -> dict[str, Any]:
        # A read transaction gives a coherent job + attempt + event snapshot.
        with self.connection() as conn:
            conn.execute("BEGIN")
            row = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if row is None:
                raise NotFound(job_id)
            return self._record(conn, row)

    def list(self, status: str | None, limit: int, offset: int) -> dict[str, Any]:
        with self.connection() as conn:
            conn.execute("BEGIN")
            where = " WHERE status=?" if status else ""
            params = (status,) if status else ()
            total = conn.execute("SELECT COUNT(*) FROM jobs" + where, params).fetchone()[0]
            rows = conn.execute(
                "SELECT * FROM jobs" + where + " ORDER BY created_at,id LIMIT ? OFFSET ?",
                (*params, limit, offset),
            ).fetchall()
            return {
                "items": [self._record(conn, r) for r in rows],
                "total": total,
                "limit": limit,
                "offset": offset,
            }

    def _retry(
        self, conn: sqlite3.Connection, row: sqlite3.Row, now: float, error: str, reason: str
    ) -> None:
        exhausted = row["attempt_count"] >= row["max_attempts"]
        status = "failed" if exhausted else "queued"
        delay = min(
            self.settings.retry_cap_seconds,
            self.settings.retry_base_seconds * 2 ** (row["attempt_count"] - 1),
        )
        conn.execute(
            "UPDATE jobs SET status=?,updated_at=?,queued_at=?,eligible_at=?,completed_at=?,"
            "attempt_token=NULL,lease_expires_at=NULL,error=? WHERE id=?",
            (status, now, now, now + delay, now if exhausted else None, error, row["id"]),
        )
        self._event(conn, row["id"], now, "running", status, row["attempt_count"], reason)

    def _reap(self, conn: sqlite3.Connection, now: float) -> int:
        rows = conn.execute(
            "SELECT * FROM jobs WHERE status='running' AND lease_expires_at<=?", (now,)
        ).fetchall()
        for row in rows:
            error = "Worker lease expired"
            conn.execute(
                "UPDATE attempts SET status='expired',finished_at=?,error=? "
                "WHERE job_id=? AND attempt_no=? AND status='running'",
                (now, error, row["id"], row["attempt_count"]),
            )
            self._retry(conn, row, now, error, "lease_expired")
        return len(rows)

    def reap(self) -> int:
        with self.transaction() as conn:
            return self._reap(conn, self.clock())

    def claim(self, worker_id: str) -> dict[str, Any] | None:
        with self.transaction() as conn:
            now = self.clock()
            self._reap(conn, now)
            row = conn.execute(
                "SELECT * FROM jobs WHERE status='queued' AND eligible_at<=? "
                "ORDER BY eligible_at,created_at,id LIMIT 1",
                (now,),
            ).fetchone()
            if row is None:
                return None
            attempt = row["attempt_count"] + 1
            token = str(uuid.uuid4())
            expires = now + self.settings.lease_seconds
            conn.execute(
                "UPDATE jobs SET status='running',attempt_count=?,attempt_token=?,worker_id=?,"
                "lease_expires_at=?,updated_at=?,first_started_at=COALESCE(first_started_at,?) "
                "WHERE id=?",
                (attempt, token, worker_id, expires, now, now, row["id"]),
            )
            conn.execute(
                "INSERT INTO attempts(job_id,attempt_no,token,worker_id,status,queued_at,"
                "eligible_at,started_at,lease_expires_at) VALUES(?,?,?,?,'running',?,?,?,?)",
                (
                    row["id"],
                    attempt,
                    token,
                    worker_id,
                    row["queued_at"],
                    row["eligible_at"],
                    now,
                    expires,
                ),
            )
            self._event(conn, row["id"], now, "queued", "running", attempt, "claimed")
            current = conn.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone()
            job = self._record(conn, current)
            job.update(attempt_token=token, lease_seconds=self.settings.lease_seconds)
            return job

    @staticmethod
    def _active(
        conn: sqlite3.Connection, job_id: str, token: str, now: float
    ) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM jobs WHERE id=? AND status='running' AND attempt_token=? "
            "AND lease_expires_at>?",
            (job_id, token, now),
        ).fetchone()

    def heartbeat(self, job_id: str, token: str) -> dict[str, float]:
        accepted = False
        with self.transaction() as conn:
            now = self.clock()
            self._reap(conn, now)
            row = self._active(conn, job_id, token, now)
            if row is not None:
                expires = now + self.settings.lease_seconds
                conn.execute(
                    "UPDATE jobs SET lease_expires_at=?,updated_at=? WHERE id=?",
                    (expires, now, job_id),
                )
                conn.execute(
                    "UPDATE attempts SET lease_expires_at=? WHERE job_id=? AND token=?",
                    (expires, job_id, token),
                )
                accepted = True
        if not accepted:
            raise StaleAttempt("Attempt is expired, finished, unknown, or reassigned")
        return {"lease_expires_at": expires, "lease_seconds": self.settings.lease_seconds}

    def finish(
        self,
        job_id: str,
        token: str,
        execution_ms: float,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> dict[str, str]:
        accepted = False
        with self.transaction() as conn:
            now = self.clock()
            self._reap(conn, now)
            row = self._active(conn, job_id, token, now)
            if row is not None:
                attempt_status = "failed" if error is not None else "succeeded"
                conn.execute(
                    "UPDATE attempts SET status=?,finished_at=?,execution_ms=?,error=? "
                    "WHERE job_id=? AND token=?",
                    (attempt_status, now, execution_ms, error, job_id, token),
                )
                if error is not None:
                    self._retry(conn, row, now, error, "handler_failed")
                else:
                    conn.execute(
                        "UPDATE jobs SET status='succeeded',result=?,error=NULL,updated_at=?,"
                        "completed_at=?,attempt_token=NULL,lease_expires_at=NULL WHERE id=?",
                        (canonical(result), now, now, job_id),
                    )
                    self._event(
                        conn, job_id, now, "running", "succeeded", row["attempt_count"], "completed"
                    )
                accepted = True
                status = conn.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()[0]
        if not accepted:
            raise StaleAttempt("Attempt is expired, finished, unknown, or reassigned")
        return {"status": status}
