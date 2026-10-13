"""Explicit coordinator configuration; workers do not import the storage layer."""

import math
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path("runtime/queueworks.sqlite3")
    data_dir: Path = Path("data")
    lease_seconds: float = 10.0
    retry_base_seconds: float = 0.5
    retry_cap_seconds: float = 30.0
    reaper_interval: float = 0.25
    busy_timeout_ms: int = 5000
    synchronous: str = "FULL"
    worker_key: str | None = None

    def __post_init__(self) -> None:
        if not all(
            math.isfinite(value)
            for value in (
                self.lease_seconds,
                self.reaper_interval,
                self.retry_base_seconds,
                self.retry_cap_seconds,
            )
        ):
            raise ValueError("Queue timing settings must be finite")
        if self.lease_seconds <= 0 or self.reaper_interval <= 0:
            raise ValueError("Lease and reaper intervals must be positive")
        if self.retry_base_seconds < 0 or self.retry_cap_seconds < self.retry_base_seconds:
            raise ValueError("Retry cap must be >= nonnegative retry base")
        if self.busy_timeout_ms < 0 or self.synchronous not in {"FULL", "NORMAL"}:
            raise ValueError("Invalid SQLite configuration")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=Path(os.getenv("QUEUEWORKS_DB_PATH", "runtime/queueworks.sqlite3")),
            data_dir=Path(os.getenv("QUEUEWORKS_DATA_DIR", "data")),
            lease_seconds=float(os.getenv("QUEUEWORKS_LEASE_SECONDS", "10")),
            retry_base_seconds=float(os.getenv("QUEUEWORKS_RETRY_BASE_SECONDS", "0.5")),
            retry_cap_seconds=float(os.getenv("QUEUEWORKS_RETRY_CAP_SECONDS", "30")),
            reaper_interval=float(os.getenv("QUEUEWORKS_REAPER_INTERVAL", "0.25")),
            busy_timeout_ms=int(os.getenv("QUEUEWORKS_SQLITE_BUSY_TIMEOUT_MS", "5000")),
            synchronous=os.getenv("QUEUEWORKS_SQLITE_SYNCHRONOUS", "FULL").upper(),
            worker_key=os.getenv("QUEUEWORKS_WORKER_KEY") or None,
        )
