"""Public jobs API and a token-fenced internal worker protocol."""

import asyncio
import hmac
import logging
import sqlite3
from contextlib import asynccontextmanager, suppress
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from queueworks.config import Settings
from queueworks.models import (
    ClaimRequest,
    CompleteRequest,
    FailRequest,
    JobSubmit,
    Status,
    TokenRequest,
)
from queueworks.store import Conflict, NotFound, StaleAttempt, Store

logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, store: Store | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    store = store or Store(settings)

    async def reaper() -> None:
        while True:
            await asyncio.sleep(settings.reaper_interval)
            try:
                await asyncio.to_thread(store.reap)
            except sqlite3.Error:
                logger.exception("Lease reaper database error; next interval will retry")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        store.initialize()
        task = asyncio.create_task(reaper())
        try:
            yield
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    app = FastAPI(title="QueueWorks", version="0.1.0", lifespan=lifespan)
    app.state.store = store

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_, exc):
        # Error inputs can contain non-JSON values such as NaN. Return useful
        # locations/messages without trying to serialize the rejected input.
        errors = [
            {"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]}
            for error in exc.errors()
        ]
        return JSONResponse(status_code=422, content={"detail": errors})

    @app.exception_handler(NotFound)
    async def not_found(_, exc):
        return JSONResponse(status_code=404, content={"detail": "Job not found"})

    @app.exception_handler(Conflict)
    @app.exception_handler(StaleAttempt)
    async def conflict(_, exc):
        return JSONResponse(status_code=409, content={"detail": str(exc)})

    @app.exception_handler(sqlite3.OperationalError)
    async def unavailable(_, exc):
        logger.error("Database operation failed: %s", exc)
        return JSONResponse(status_code=503, content={"detail": "Database temporarily unavailable"})

    def authorize(x_worker_key: Annotated[str | None, Header()] = None) -> None:
        if settings.worker_key and not hmac.compare_digest(
            (x_worker_key or "").encode("utf-8"), settings.worker_key.encode("utf-8")
        ):
            raise HTTPException(401, "Invalid worker key")

    @app.get("/health")
    def health():
        try:
            ready = store.ready()
        except sqlite3.Error:
            raise HTTPException(503, "Database unavailable") from None
        return {"status": "ready" if ready else "unavailable", "database": "ready"}

    @app.post("/jobs", status_code=201)
    def submit(
        spec: JobSubmit,
        response: Response,
        idempotency_key: Annotated[str | None, Header(min_length=1, max_length=200)] = None,
    ):
        def validate_input() -> None:
            if spec.kind in {"checksum", "thumbnail"}:
                # Resolve symlinks too; workers independently repeat this containment check.
                try:
                    data_root = settings.data_dir.resolve()
                    fixtures = (data_root / "fixtures").resolve()
                    path = (settings.data_dir / spec.payload["path"]).resolve()
                except (OSError, ValueError):
                    raise HTTPException(422, "Invalid input file path") from None
                if (
                    not fixtures.is_relative_to(data_root)
                    or not path.is_relative_to(fixtures)
                    or not path.is_file()
                ):
                    raise HTTPException(
                        422, "Input must be an existing file inside the data fixtures directory"
                    )

        # Existing idempotent submissions remain retrievable if a fixture is later removed.
        job, created = store.submit(spec, idempotency_key, validate_new=validate_input)
        response.status_code = 201 if created else 200
        return job

    @app.get("/jobs")
    def list_jobs(
        status: Status | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
        offset: Annotated[int, Query(ge=0, le=1_000_000)] = 0,
    ):
        return store.list(status, limit, offset)

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        return store.get(job_id)

    @app.post("/internal/claim", dependencies=[Depends(authorize)])
    def claim(request: ClaimRequest):
        return {"job": store.claim(request.worker_id)}

    @app.post("/internal/jobs/{job_id}/heartbeat", dependencies=[Depends(authorize)])
    def heartbeat(job_id: str, request: TokenRequest):
        return store.heartbeat(job_id, request.token)

    @app.post("/internal/jobs/{job_id}/complete", dependencies=[Depends(authorize)])
    def complete(job_id: str, request: CompleteRequest):
        return store.finish(job_id, request.token, request.execution_ms, result=request.result)

    @app.post("/internal/jobs/{job_id}/fail", dependencies=[Depends(authorize)])
    def fail(job_id: str, request: FailRequest):
        return store.finish(job_id, request.token, request.execution_ms, error=request.error)

    return app
