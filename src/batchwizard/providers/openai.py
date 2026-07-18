# providers/openai.py
from __future__ import annotations

from pathlib import Path

import aiofiles
from loguru import logger
from openai import AsyncOpenAI

from ..config import config
from ..models import BatchStatus, DownloadedResults, JobState

# OpenAI batch statuses that mean "still working"
_RUNNING_STATUSES = {"validating", "in_progress", "finalizing", "cancelling"}

_TERMINAL_MAP = {
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
        async with aiofiles.open(input_file, "rb") as f:
            content = await f.read()
        uploaded = await self.client.files.create(
            file=(input_file.name, content), purpose="batch"
        )
        batch = await self.client.batches.create(
            input_file_id=uploaded.id,
            endpoint=endpoint,
            completion_window="24h",
        )
        logger.info(f"Submitted {input_file.name} as batch {batch.id}")
        return batch.id

    async def status(self, batch_id: str) -> BatchStatus:
        batch = await self.client.batches.retrieve(batch_id)
        provider_status = batch.status.lower()
        state = _TERMINAL_MAP.get(provider_status)
        counts = getattr(batch, "request_counts", None)
        return BatchStatus(
            provider_status=provider_status,
            state=state,
            completed_count=getattr(counts, "completed", 0) or 0,
            failed_count=getattr(counts, "failed", 0) or 0,
            total_count=getattr(counts, "total", 0) or 0,
            error_summary=_error_summary(batch),
        )

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
            content = await self.client.files.content(file_id)
            async with aiofiles.open(path, "wb") as f:
                await f.write(content.content)
            setattr(results, attr, path)
            logger.info(f"Downloaded {suffix} for {batch_id} to {path}")
        return results

    async def cancel(self, batch_id: str) -> None:
        await self.client.batches.cancel(batch_id)

    async def close(self) -> None:
        await self.client.close()
