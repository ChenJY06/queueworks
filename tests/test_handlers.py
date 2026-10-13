"""Repeatability, containment, and cooperative cancellation of real handlers."""

from __future__ import annotations

import hashlib
import threading
from pathlib import Path

import pytest
from PIL import Image

from queueworks.handlers import HandlerCancelled, execute, generate_fixtures


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    generate_fixtures(tmp_path)
    return tmp_path


def run(data_dir: Path, kind: str, payload: dict, attempt: int = 1) -> dict:
    return execute(
        kind,
        payload,
        data_dir=data_dir,
        job_id="test-job",
        attempt=attempt,
        token=f"attempt-token-{attempt}",
        cancel=threading.Event(),
    )


def test_checksum_matches_fixture_and_repeats(data_dir: Path) -> None:
    expected = hashlib.sha256((data_dir / "fixtures/sample.bin").read_bytes()).hexdigest()
    first = run(data_dir, "checksum", {"path": "fixtures/sample.bin"})
    assert first["sha256"] == expected
    assert first["size_bytes"] == 2 * 1024 * 1024
    assert run(data_dir, "checksum", {"path": "fixtures/sample.bin"}, 2) == first


def test_thumbnail_repeated_attempt_has_identical_output(data_dir: Path) -> None:
    payload = {"path": "fixtures/sample.png", "width": 128, "height": 128}
    first = run(data_dir, "thumbnail", payload)
    output = data_dir / first["path"]
    original_bytes = output.read_bytes()
    second = run(data_dir, "thumbnail", payload, 2)
    assert first == second
    assert output.read_bytes() == original_bytes
    assert first["sha256"] == hashlib.sha256(original_bytes).hexdigest()
    with Image.open(output) as thumbnail:
        assert thumbnail.size == (128, 96)
    assert sorted(path.name for path in output.parent.iterdir()) == ["test-job.png"]


@pytest.mark.parametrize(
    "path",
    ["../secret", "fixtures/../secret", "/etc/passwd", "C:\\secret", "thumbnails/output.png"],
)
def test_paths_outside_fixtures_are_rejected(data_dir: Path, path: str) -> None:
    with pytest.raises(ValueError):
        run(data_dir, "checksum", {"path": path})


def test_cancelled_attempt_never_publishes_thumbnail(data_dir: Path) -> None:
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(HandlerCancelled):
        execute(
            "thumbnail",
            {"path": "fixtures/sample.png", "width": 128, "height": 128},
            data_dir=data_dir,
            job_id="cancelled",
            attempt=1,
            token="cancelled-token",
            cancel=cancel,
        )
    assert not (data_dir / "thumbnails/cancelled.png").exists()


def test_synthetic_requested_failures_end_after_configured_attempt(data_dir: Path) -> None:
    payload = {"duration_ms": 0, "iterations": 10, "fail_until_attempt": 2}
    with pytest.raises(RuntimeError, match="attempt 1"):
        run(data_dir, "synthetic", payload, 1)
    with pytest.raises(RuntimeError, match="attempt 2"):
        run(data_dir, "synthetic", payload, 2)
    assert run(data_dir, "synthetic", payload, 3) == run(data_dir, "synthetic", payload, 4)


def test_long_synthetic_attempt_cooperates_with_cancellation(data_dir: Path) -> None:
    cancel = threading.Event()
    errors: list[Exception] = []
    started = threading.Event()

    def execute_long_job() -> None:
        started.set()
        try:
            execute(
                "synthetic",
                {"duration_ms": 10_000, "iterations": 0},
                data_dir=data_dir,
                job_id="cancel-synthetic",
                attempt=1,
                token="cancel-token",
                cancel=cancel,
            )
        except Exception as error:
            errors.append(error)

    execution = threading.Thread(target=execute_long_job)
    execution.start()
    assert started.wait(timeout=1)
    cancel.set()
    execution.join(timeout=1)
    assert not execution.is_alive()
    assert len(errors) == 1
    assert isinstance(errors[0], HandlerCancelled)


def test_fixture_generation_is_reproducible(tmp_path: Path) -> None:
    generate_fixtures(tmp_path)
    first = {path.name: path.read_bytes() for path in (tmp_path / "fixtures").iterdir()}
    generate_fixtures(tmp_path)
    second = {path.name: path.read_bytes() for path in (tmp_path / "fixtures").iterdir()}
    assert first == second
