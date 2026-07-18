# processor.py
from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from .models import JobRecord, JobState
from .providers.base import BatchProvider
from .store import JobStore
from .utils import discover_jsonl

# Consecutive status-poll failures tolerated before a job is marked failed
_MAX_POLL_FAILURES = 5


@dataclass
class JobEvent:
    """Progress event emitted by the orchestrator; the UI subscribes to these."""

    kind: str  # "log" | "submitted" | "status" | "finished"
    job: JobRecord | None = None
    message: str = ""


EventCallback = Callable[[JobEvent], None]


class BatchOrchestrator:
    def __init__(
        self,
        provider: BatchProvider,
        store: JobStore,
        on_event: EventCallback | None = None,
        check_interval: float = 5,
        max_concurrent_jobs: int = 5,
    ):
        self.provider = provider
        self.store = store
        self.on_event = on_event
        self.check_interval = check_interval
        self.max_concurrent_jobs = max_concurrent_jobs

    def _emit(self, kind: str, job: JobRecord | None = None, message: str = "") -> None:
        if self.on_event:
            self.on_event(JobEvent(kind=kind, job=job, message=message))

    async def submit_paths(
        self, input_paths: list[Path], endpoint: str = "/v1/chat/completions"
    ) -> list[JobRecord]:
        """Upload inputs, create batches, and record them in the manifest."""
        files = discover_jsonl(input_paths)
        if not files:
            self._emit("log", message="No JSONL files found in the provided paths")
            return []

        semaphore = asyncio.Semaphore(self.max_concurrent_jobs)

        async def submit_one(input_file: Path) -> JobRecord | None:
            async with semaphore:
                try:
                    batch_id = await self.provider.submit(input_file, endpoint)
                except Exception as e:
                    logger.error(f"Failed to submit {input_file.name}: {e}")
                    self._emit(
                        "log", message=f"Failed to submit {input_file.name}: {e}"
                    )
                    return None
            job = self.store.add(
                JobRecord(
                    provider=self.provider.name,
                    batch_id=batch_id,
                    input_path=str(input_file),
                    endpoint=endpoint,
                )
            )
            self._emit("submitted", job)
            return job

        results = await asyncio.gather(*(submit_one(f) for f in files))
        return [job for job in results if job is not None]

    async def watch(self, jobs: list[JobRecord], output_dir: Path) -> list[JobRecord]:
        """Poll jobs until terminal; download results/errors and update the manifest."""
        if not jobs:
            self._emit("log", message="No pending jobs to watch")
            return []
        return list(
            await asyncio.gather(*(self._watch_job(job, output_dir) for job in jobs))
        )

    async def _watch_job(self, job: JobRecord, output_dir: Path) -> JobRecord:
        interval = self.check_interval
        poll_failures = 0
        while True:
            try:
                status = await self.provider.status(job.batch_id)
                poll_failures = 0
            except Exception as e:
                poll_failures += 1
                logger.warning(
                    f"Status check failed for {job.batch_id} "
                    f"({poll_failures}/{_MAX_POLL_FAILURES}): {e}"
                )
                if poll_failures >= _MAX_POLL_FAILURES:
                    job.state = JobState.FAILED
                    job.error_summary = f"Lost contact with provider: {e}"
                    self.store.update(job)
                    self._emit("finished", job)
                    return job
                await asyncio.sleep(interval)
                continue

            job.provider_status = status.provider_status
            job.error_summary = status.error_summary or job.error_summary
            progress = (
                f"{status.completed_count}/{status.total_count}"
                if status.total_count
                else ""
            )
            self._emit("status", job, message=progress)

            if status.state is not None:
                job.state = status.state
                try:
                    results = await self.provider.fetch_results(
                        job.batch_id, output_dir
                    )
                    job.output_path = (
                        str(results.output_path) if results.output_path else None
                    )
                    job.error_path = (
                        str(results.error_path) if results.error_path else None
                    )
                except Exception as e:
                    logger.error(f"Failed to download results for {job.batch_id}: {e}")
                    self._emit(
                        "log",
                        message=f"Failed to download results for {job.batch_id}: {e}",
                    )
                self.store.update(job)
                self._emit("finished", job)
                return job

            self.store.update(job)
            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 60)  # exponential backoff

    async def process(
        self,
        input_paths: list[Path],
        output_dir: Path,
        endpoint: str = "/v1/chat/completions",
    ) -> list[JobRecord]:
        """Submit and then watch to completion (the classic blocking flow)."""
        jobs = await self.submit_paths(input_paths, endpoint)
        return await self.watch(jobs, output_dir)
