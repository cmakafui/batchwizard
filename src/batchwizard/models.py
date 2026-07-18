# models.py
from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field


class JobState(StrEnum):
    PENDING = "pending"  # submitted; provider still working
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


TERMINAL_STATES = {
    JobState.COMPLETED,
    JobState.FAILED,
    JobState.EXPIRED,
    JobState.CANCELLED,
}


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class JobRecord(BaseModel):
    """A batch job as tracked in the local manifest."""

    id: int | None = None
    provider: str = "openai"
    batch_id: str
    input_path: str
    endpoint: str = "/v1/chat/completions"
    state: JobState = JobState.PENDING
    provider_status: str = ""
    output_path: str | None = None
    error_path: str | None = None
    error_summary: str | None = None
    created_at: str = Field(default_factory=utcnow)
    updated_at: str = Field(default_factory=utcnow)


class BatchStatus(BaseModel):
    """Normalized status snapshot from a provider."""

    provider_status: str
    state: JobState | None = None  # None while the batch is still running
    completed_count: int = 0
    failed_count: int = 0
    total_count: int = 0
    error_summary: str | None = None


class DownloadedResults(BaseModel):
    output_path: Path | None = None
    error_path: Path | None = None
