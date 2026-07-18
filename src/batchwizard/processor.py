# processor.py
from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from .models import (
    TERMINAL_STATES,
    BatchStatus,
    CollectionState,
    JobRecord,
)
from .providers.base import ArtifactUnavailableError, BatchProvider
from .store import JobStore
from .utils import discover_jsonl

# Retryable status failures tolerated in one watch invocation before it pauses.
_MAX_POLL_FAILURES = 5


@dataclass
class JobEvent:
    """Progress event emitted by the orchestrator; the UI subscribes to these."""

    kind: str  # "log" | "submitted" | "status" | "attention" | "finished"
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
        self, input_paths: list[Path], endpoint: str | None = None
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
                    submitted = await self.provider.submit(input_file, endpoint)
                except Exception as e:
                    logger.error(f"Failed to submit {input_file.name}: {e}")
                    self._emit(
                        "log", message=f"Failed to submit {input_file.name}: {e}"
                    )
                    return None
            job = self.store.add(
                JobRecord(
                    provider=self.provider.name,
                    batch_id=submitted.batch_id,
                    input_path=str(input_file),
                    endpoint=submitted.endpoint,
                    provider_status=submitted.provider_status,
                )
            )
            self._emit("submitted", job)
            return job

        results = await asyncio.gather(*(submit_one(f) for f in files))
        return [job for job in results if job is not None]

    async def watch(self, jobs: list[JobRecord], output_dir: Path) -> list[JobRecord]:
        """Advance actionable jobs and durably collect terminal artifacts."""
        if not jobs:
            self._emit("log", message="No actionable jobs to watch")
            return []
        return list(
            await asyncio.gather(*(self._watch_job(job, output_dir) for job in jobs))
        )

    async def _watch_job(self, job: JobRecord, output_dir: Path) -> JobRecord:
        if job.state in TERMINAL_STATES:
            return await self.collect(job, output_dir)

        interval = self.check_interval
        watch_failures = 0
        while True:
            try:
                status = await self.provider.status(job.batch_id)
            except Exception as e:
                watch_failures += 1
                job.poll_failures += 1
                job.last_local_error = f"Status check failed: {e}"
                self.store.update(job)
                retryable = _is_retryable(e)
                failure_limit = _MAX_POLL_FAILURES if retryable else 1
                logger.warning(
                    f"Status check failed for {job.batch_id} "
                    f"({job.poll_failures}/{failure_limit}): {e}"
                )
                self._emit("attention", job, message=job.last_local_error)
                if watch_failures >= failure_limit:
                    self._emit(
                        "log",
                        job,
                        message=(
                            f"Paused watching {job.batch_id}; it remains actionable "
                            "and a later 'batchwizard watch' will retry it"
                        ),
                    )
                    return job
                await asyncio.sleep(interval)
                continue

            self._apply_status(job, status)
            progress = (
                f"{status.completed_count}/{status.total_count}"
                if status.total_count
                else ""
            )
            self._emit("status", job, message=progress)

            if status.is_terminal:
                self.store.update(job)
                return await self.collect(job, output_dir)

            self.store.update(job)
            await asyncio.sleep(interval)
            interval = min(interval * 1.5, 60)  # exponential backoff

    def _apply_status(self, job: JobRecord, status: BatchStatus) -> None:
        job.provider_status = status.provider_status
        if status.state is not None:
            job.state = status.state
        job.completed_count = status.completed_count
        job.failed_count = status.failed_count
        job.cancelled_count = status.cancelled_count
        job.expired_count = status.expired_count
        job.total_count = status.total_count
        job.error_summary = status.error_summary
        job.last_local_error = None
        job.poll_failures = 0
        if status.is_terminal and job.collection_state in {
            CollectionState.NOT_READY,
            CollectionState.FAILED,
        }:
            job.collection_state = CollectionState.PENDING

    async def collect(self, job: JobRecord, output_dir: Path) -> JobRecord:
        """Collect terminal artifacts idempotently and persist retryable failures."""
        if job.state not in TERMINAL_STATES:
            raise ValueError(f"Cannot collect non-terminal job {job.batch_id}")
        try:
            results = await self.provider.fetch_results(job.batch_id, output_dir)
        except ArtifactUnavailableError as e:
            job.collection_state = CollectionState.UNAVAILABLE
            job.error_summary = str(e)
            job.last_local_error = None
            self.store.update(job)
            logger.warning(f"Artifacts unavailable for {job.batch_id}: {e}")
            self._emit("finished", job, message=str(e))
            return job
        except Exception as e:
            job.collection_state = CollectionState.FAILED
            job.last_local_error = f"Artifact collection failed: {e}"
            self.store.update(job)
            logger.error(f"Failed to collect artifacts for {job.batch_id}: {e}")
            self._emit("attention", job, message=job.last_local_error)
            return job

        job.output_path = str(results.output_path) if results.output_path else None
        job.error_path = str(results.error_path) if results.error_path else None
        job.collection_state = CollectionState.COLLECTED
        if results.error_summary:
            job.error_summary = results.error_summary
        job.last_local_error = None
        self.store.update(job)
        self._emit("finished", job)
        return job

    async def request_cancel(self, batch_id: str) -> BatchStatus:
        """Request cancellation without pretending it has already completed."""
        status = await self.provider.cancel(batch_id)
        job = self.store.get(batch_id, provider=self.provider.name)
        if job:
            self._apply_status(job, status)
            self.store.update(job)
            self._emit("status", job)
        return status

    async def process(
        self,
        input_paths: list[Path],
        output_dir: Path,
        endpoint: str | None = None,
    ) -> list[JobRecord]:
        """Submit and then watch to completion (the classic blocking flow)."""
        jobs = await self.submit_paths(input_paths, endpoint)
        return await self.watch(jobs, output_dir)


def _is_retryable(error: Exception) -> bool:
    """Follow common HTTP retry semantics without importing a provider SDK."""
    status_code = getattr(error, "status_code", None)
    if status_code is None:
        return True
    return status_code in {408, 409, 429} or status_code >= 500
