"""Validation for supported job payloads and worker messages."""

from pathlib import PurePosixPath
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

Status = Literal["queued", "running", "succeeded", "failed"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class FilePayload(StrictModel):
    path: str = Field(min_length=1, max_length=240)

    @model_validator(mode="after")
    def safe_path(self) -> "FilePayload":
        # A portable relative path prevents drive letters and alternate data streams on Windows.
        path = PurePosixPath(self.path)
        if (
            "\\" in self.path
            or ":" in self.path
            or any(ord(character) < 32 for character in self.path)
            or path.is_absolute()
            or ".." in path.parts
            or len(path.parts) < 2
            or path.parts[0] != "fixtures"
        ):
            raise ValueError("Input must be a relative path inside fixtures/")
        self.path = path.as_posix()
        return self


class ThumbnailPayload(FilePayload):
    width: int = Field(default=128, ge=1, le=2048, strict=True)
    height: int = Field(default=128, ge=1, le=2048, strict=True)


class SyntheticPayload(StrictModel):
    duration_ms: int = Field(default=100, ge=0, le=30000, strict=True)
    iterations: int = Field(default=0, ge=0, le=5_000_000, strict=True)
    fail_until_attempt: int = Field(default=0, ge=0, le=10, strict=True)


class JobSubmit(StrictModel):
    kind: Literal["checksum", "thumbnail", "synthetic"]
    payload: dict[str, JsonValue]
    max_attempts: int = Field(default=3, ge=1, le=10, strict=True)

    @model_validator(mode="after")
    def validate_handler(self) -> "JobSubmit":
        payload_class = {
            "checksum": FilePayload,
            "thumbnail": ThumbnailPayload,
            "synthetic": SyntheticPayload,
        }[self.kind]
        self.payload = payload_class.model_validate(self.payload).model_dump()
        return self


class ClaimRequest(StrictModel):
    worker_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_.-]+$")


class TokenRequest(StrictModel):
    token: str = Field(min_length=1, max_length=100)


class CompleteRequest(TokenRequest):
    result: dict[str, JsonValue]
    execution_ms: float = Field(ge=0, le=86_400_000, allow_inf_nan=False)


class FailRequest(TokenRequest):
    error: str = Field(min_length=1, max_length=4000)
    execution_ms: float = Field(ge=0, le=86_400_000, allow_inf_nan=False)
