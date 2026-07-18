# providers/base.py
from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from ..models import (
    BatchStatus,
    DownloadedResults,
    ProviderJobSummary,
    SubmittedBatch,
)


class ArtifactUnavailableError(RuntimeError):
    """Provider artifacts have been permanently removed or archived."""


@runtime_checkable
class BatchProvider(Protocol):
    """A provider adapter for durable, provider-native batch jobs."""

    name: str

    async def submit(
        self,
        input_file: Path,
        endpoint: str | None = None,
        intent_id: str | None = None,
    ) -> SubmittedBatch:
        """Validate provider-native input and create a batch."""
        ...

    async def status(self, batch_id: str) -> BatchStatus:
        """Fetch and normalize the current status of a batch."""
        ...

    async def fetch_results(self, batch_id: str, output_dir: Path) -> DownloadedResults:
        """Idempotently collect available results and per-request errors."""
        ...

    async def cancel(self, batch_id: str) -> BatchStatus:
        """Request cancellation and return the provider's immediate status."""
        ...

    async def list_jobs(self, limit: int = 20) -> list[ProviderJobSummary]:
        """List recent jobs without leaking a provider SDK through the CLI."""
        ...

    async def close(self) -> None: ...
