# models.py
from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field


class JobState(StrEnum):
    """Normalized remote lifecycle state.

    Provider-native status strings remain in ``provider_status``.  Keeping the
    normalized state deliberately small lets providers add statuses without
    forcing BatchWizard to guess what an unknown status means.
    """

    PENDING = "pending"
    RUNNING = "running"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


ACTIVE_STATES = frozenset(
    {
        JobState.PENDING,
        JobState.RUNNING,
        JobState.CANCELLING,
    }
)

TERMINAL_STATES = frozenset(
    {
        JobState.COMPLETED,
        JobState.FAILED,
        JobState.EXPIRED,
        JobState.CANCELLED,
    }
)


class CollectionState(StrEnum):
    """State of copying provider artifacts into durable local files."""

    NOT_READY = "not_ready"
    PENDING = "pending"
    COLLECTED = "collected"
    FAILED = "failed"


ACTIONABLE_COLLECTION_STATES = frozenset(
    {
        CollectionState.PENDING,
        CollectionState.FAILED,
    }
)


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
    collection_state: CollectionState = CollectionState.NOT_READY
    completed_count: int = 0
    failed_count: int = 0
    cancelled_count: int = 0
    expired_count: int = 0
    total_count: int = 0
    output_path: str | None = None
    error_path: str | None = None
    error_summary: str | None = None
    last_local_error: str | None = None
    poll_failures: int = 0
    created_at: str = Field(default_factory=utcnow)
    updated_at: str = Field(default_factory=utcnow)

    @property
    def is_actionable(self) -> bool:
        return self.state in ACTIVE_STATES or (
            self.state in TERMINAL_STATES
            and self.collection_state in ACTIONABLE_COLLECTION_STATES
        )


class BatchStatus(BaseModel):
    """Normalized status snapshot from a provider."""

    provider_status: str
    # None means the provider returned an unknown status.  The raw status is
    # still preserved and the job remains actionable rather than being guessed
    # terminal.
    state: JobState | None = None
    completed_count: int = 0
    failed_count: int = 0
    cancelled_count: int = 0
    expired_count: int = 0
    total_count: int = 0
    error_summary: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES


class ProviderJobSummary(BaseModel):
    """Provider-neutral row used by ``list-jobs``."""

    batch_id: str
    provider_status: str
    created_at: int | None = None
    completed_count: int = 0
    failed_count: int = 0
    total_count: int = 0


class DownloadedResults(BaseModel):
    output_path: Path | None = None
    error_path: Path | None = None
