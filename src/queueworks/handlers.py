"""Repeatable handlers restricted to the configured fixture and output directory.

Workers recheck path containment independently of coordinator validation.
"""

from __future__ import annotations

import hashlib
import os
import tempfile
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Any

from PIL import Image


class HandlerCancelled(RuntimeError):
    """The worker stopped, or its lease can no longer be trusted."""


def _check_cancel(cancel: threading.Event) -> None:
    if cancel.is_set():
        raise HandlerCancelled("handler cancelled before committing its output")


def _inside(root: Path, candidate: Path) -> Path:
    resolved = candidate.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("path must stay inside the configured data directory")
    return resolved


def _fixture_path(data_dir: Path, name: str) -> Path:
    # Normalize separators so the same payload has the same meaning on Windows
    # and Linux. Reject traversal even when it happens to resolve inside root.
    relative = PurePosixPath(name.replace("\\", "/"))
    if (
        relative.is_absolute()
        or not relative.parts
        or relative.parts[0] != "fixtures"
        or ".." in relative.parts
        or ":" in name
    ):
        raise ValueError("input must be a relative path beneath fixtures/")
    root = data_dir.resolve()
    fixtures = _inside(root, root / "fixtures")
    result = _inside(root, root.joinpath(*relative.parts))
    if not result.is_relative_to(fixtures) or not result.is_file():
        raise ValueError("input must be an existing fixture file")
    return result


def _checksum(path: Path, cancel: threading.Event) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while True:
            _check_cancel(cancel)
            chunk = source.read(256 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    _check_cancel(cancel)
    return digest.hexdigest(), size


def generate_fixtures(data_dir: Path) -> dict[str, str]:
    """Generate the same binary, text, and PNG fixture bytes on repeated calls."""
    root = Path(data_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)
    fixtures = _inside(root, root / "fixtures")
    fixtures.mkdir(parents=True, exist_ok=True)
    block = bytes(range(256)) * 256
    binary = _inside(root, fixtures / "sample.bin")
    with binary.open("wb") as output:
        for _ in range(32):
            output.write(block)
    text = _inside(root, fixtures / "sample.txt")
    text.write_bytes(b"QueueWorks: durable jobs, explicit leases, repeatable handlers.\n" * 128)
    # Generate image detail without downloads or random input.
    pixels = bytes(
        component
        for y in range(768)
        for x in range(1024)
        for component in ((x // 4) % 256, (y // 3) % 256, (x + y) % 256)
    )
    picture = _inside(root, fixtures / "sample.png")
    with Image.frombytes("RGB", (1024, 768), pixels) as image:
        image.save(picture, format="PNG", compress_level=6)
    return {
        "checksum": "fixtures/sample.bin",
        "text": "fixtures/sample.txt",
        "thumbnail": "fixtures/sample.png",
    }


def execute(
    kind: str,
    payload: dict[str, Any],
    *,
    data_dir: Path,
    job_id: str,
    attempt: int,
    token: str,
    cancel: threading.Event,
) -> dict[str, Any]:
    """Execute a supported handler against the configured data directory."""
    _check_cancel(cancel)
    if kind == "checksum":
        digest, size = _checksum(_fixture_path(Path(data_dir), payload["path"]), cancel)
        return {"path": payload["path"], "sha256": digest, "size_bytes": size}

    if kind == "thumbnail":
        safe_characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        if not job_id or any(character not in safe_characters for character in job_id):
            raise ValueError("job id must be a safe filename component")
        root = Path(data_dir).resolve()
        source_path = _fixture_path(root, payload["path"])
        output_dir = _inside(root, root / "thumbnails")
        output_dir.mkdir(parents=True, exist_ok=True)
        destination = output_dir / f"{job_id}.png"
        _inside(root, destination)
        # Retain the destination path: os.replace replaces a symlink rather than its target.
        # Unique temporary files prevent partial output; retries need immutable fixtures.
        token_tag = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
        with tempfile.NamedTemporaryFile(
            dir=output_dir,
            prefix=f".{job_id}.{attempt}.{token_tag}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        try:
            with Image.open(source_path) as source:
                thumbnail = source.convert("RGB")
            try:
                thumbnail.thumbnail((payload["width"], payload["height"]), Image.Resampling.LANCZOS)
                _check_cancel(cancel)
                thumbnail.save(temporary_path, format="PNG", compress_level=6)
                digest, _ = _checksum(temporary_path, cancel)
                width, height = thumbnail.size
            finally:
                thumbnail.close()
            _check_cancel(cancel)
            os.replace(temporary_path, destination)
            return {
                "path": f"thumbnails/{job_id}.png",
                "width": width,
                "height": height,
                "sha256": digest,
            }
        finally:
            temporary_path.unlink(missing_ok=True)

    if kind == "synthetic":
        started = time.monotonic()
        iterations = payload.get("iterations", 0)
        duration_ms = payload.get("duration_ms", 0)
        value = 0
        for index in range(iterations):
            if index % 4096 == 0:
                _check_cancel(cancel)
            value = (1664525 * value + 1013904223) & 0xFFFFFFFF
        deadline = started + duration_ms / 1000
        while (remaining := deadline - time.monotonic()) > 0:
            if cancel.wait(min(remaining, 0.05)):
                _check_cancel(cancel)
        _check_cancel(cancel)
        if attempt <= payload.get("fail_until_attempt", 0):
            raise RuntimeError(f"synthetic failure requested for attempt {attempt}")
        return {
            "duration_ms": duration_ms,
            "iterations": iterations,
            "checksum": f"{value:08x}",
        }

    raise ValueError(f"unsupported handler: {kind}")
