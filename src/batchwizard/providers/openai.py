# providers/openai.py
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from loguru import logger
from openai import AsyncOpenAI

from ..config import config
from ..models import (
    BatchStatus,
    DownloadedResults,
    JobState,
    ProviderJobSummary,
)

SUPPORTED_ENDPOINTS = frozenset(
    {
        "/v1/responses",
        "/v1/chat/completions",
        "/v1/embeddings",
        "/v1/completions",
        "/v1/moderations",
        "/v1/images/generations",
        "/v1/images/edits",
        "/v1/videos",
    }
)

_STATE_MAP = {
    "validating": JobState.PENDING,
    "in_progress": JobState.RUNNING,
    "finalizing": JobState.RUNNING,
    "cancelling": JobState.CANCELLING,
    "completed": JobState.COMPLETED,
    "failed": JobState.FAILED,
    "expired": JobState.EXPIRED,
    "cancelled": JobState.CANCELLED,
}


def _error_summary(batch) -> str | None:
    """Flatten the batch-level errors object into a readable one-liner (issue #3)."""
    errors = getattr(batch, "errors", None)
    data = getattr(errors, "data", None) if errors else None
    if not data:
        return None
    parts = []
    for err in data:
        code = getattr(err, "code", None) or "error"
        message = getattr(err, "message", None) or ""
        line = getattr(err, "line", None)
        loc = f" (line {line})" if line is not None else ""
        parts.append(f"{code}: {message}{loc}")
    return "; ".join(parts)


class OpenAIBatchProvider:
    name = "openai"

    def __init__(self, client: AsyncOpenAI | None = None):
        self.client = client or AsyncOpenAI(api_key=config.get_api_key())

    async def submit(self, input_file: Path, endpoint: str) -> str:
        if endpoint not in SUPPORTED_ENDPOINTS:
            available = ", ".join(sorted(SUPPORTED_ENDPOINTS))
            raise ValueError(
                f"Unsupported OpenAI Batch endpoint {endpoint!r}. "
                f"Available: {available}"
            )
        uploaded = await self.client.files.create(file=input_file, purpose="batch")
        batch = await self.client.batches.create(
            input_file_id=uploaded.id,
            endpoint=endpoint,
            completion_window="24h",
        )
        logger.info(f"Submitted {input_file.name} as batch {batch.id}")
        return batch.id

    async def status(self, batch_id: str) -> BatchStatus:
        batch = await self.client.batches.retrieve(batch_id)
        return _normalize_status(batch)

    async def fetch_results(self, batch_id: str, output_dir: Path) -> DownloadedResults:
        batch = await self.client.batches.retrieve(batch_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        results = DownloadedResults()
        for file_id, suffix, attr in (
            (batch.output_file_id, "results", "output_path"),
            (batch.error_file_id, "errors", "error_path"),
        ):
            if not file_id:
                continue
            path = output_dir / f"{batch_id}_{suffix}.jsonl"
            await self._download_atomic(file_id, path)
            setattr(results, attr, path)
            logger.info(f"Downloaded {suffix} for {batch_id} to {path}")
        return results

    async def _download_atomic(self, file_id: str, destination: Path) -> None:
        """Stream a provider file and expose it only after a complete download."""
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent,
            prefix=f".{destination.name}.",
            suffix=".part",
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            async with self.client.files.with_streaming_response.content(
                file_id
            ) as response:
                await response.stream_to_file(temporary)
            os.replace(temporary, destination)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    async def cancel(self, batch_id: str) -> BatchStatus:
        batch = await self.client.batches.cancel(batch_id)
        return _normalize_status(batch)

    async def list_jobs(self, limit: int = 20) -> list[ProviderJobSummary]:
        page = await self.client.batches.list(limit=limit)
        jobs = []
        for batch in page.data:
            counts = getattr(batch, "request_counts", None)
            jobs.append(
                ProviderJobSummary(
                    batch_id=batch.id,
                    provider_status=batch.status,
                    created_at=getattr(batch, "created_at", None),
                    completed_count=getattr(counts, "completed", 0) or 0,
                    failed_count=getattr(counts, "failed", 0) or 0,
                    total_count=getattr(counts, "total", 0) or 0,
                )
            )
        return jobs

    async def close(self) -> None:
        await self.client.close()


def _normalize_status(batch) -> BatchStatus:
    """Normalize an SDK Batch while preserving unknown future statuses."""
    provider_status = str(batch.status).lower()
    state = _STATE_MAP.get(provider_status)
    counts = getattr(batch, "request_counts", None)
    return BatchStatus(
        provider_status=provider_status,
        state=state,
        completed_count=getattr(counts, "completed", 0) or 0,
        failed_count=getattr(counts, "failed", 0) or 0,
        total_count=getattr(counts, "total", 0) or 0,
        error_summary=_error_summary(batch),
    )
