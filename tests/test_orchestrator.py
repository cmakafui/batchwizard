from __future__ import annotations

from pathlib import Path

from batchwizard.models import (
    BatchStatus,
    CollectionState,
    DownloadedResults,
    JobState,
    SubmittedBatch,
)
from batchwizard.processor import BatchOrchestrator
from batchwizard.providers.base import ArtifactUnavailableError
from batchwizard.store import JobStore
from batchwizard.utils import discover_jsonl


class FakeProvider:
    """Scriptable provider: each batch_id gets a queue of statuses to play back."""

    name = "fake"

    def __init__(self):
        self.counter = 0
        self.status_scripts: dict[str, list[BatchStatus]] = {}
        self.files: dict[str, DownloadedResults] = {}
        self.submitted: list[tuple[Path, str | None]] = []
        self.fail_submit_for: set[str] = set()
        self.fetch_failures: dict[str, int] = {}
        self.cancel_status = BatchStatus(
            provider_status="cancelling", state=JobState.CANCELLING
        )

    async def submit(
        self, input_file: Path, endpoint: str | None = None
    ) -> SubmittedBatch:
        if input_file.name in self.fail_submit_for:
            raise RuntimeError("upload exploded")
        self.submitted.append((input_file, endpoint))
        self.counter += 1
        return SubmittedBatch(
            batch_id=f"batch_{self.counter}",
            provider_status="validating",
            endpoint=endpoint or "/v1/chat/completions",
        )

    async def status(self, batch_id: str) -> BatchStatus:
        script = self.status_scripts[batch_id]
        return script.pop(0) if len(script) > 1 else script[0]

    async def fetch_results(self, batch_id: str, output_dir: Path) -> DownloadedResults:
        if self.fetch_failures.get(batch_id, 0):
            self.fetch_failures[batch_id] -= 1
            raise ConnectionError("download interrupted")
        return self.files.get(batch_id, DownloadedResults())

    async def cancel(self, batch_id: str) -> BatchStatus:
        return self.cancel_status

    async def list_jobs(self, limit: int = 20):
        return []

    async def close(self) -> None: ...


def running(count=0):
    return BatchStatus(
        provider_status="in_progress",
        state=JobState.RUNNING,
        completed_count=count,
        total_count=2,
    )


def done():
    return BatchStatus(provider_status="completed", state=JobState.COMPLETED)


def failed(summary: str | None = None):
    return BatchStatus(
        provider_status="failed", state=JobState.FAILED, error_summary=summary
    )


def test_discover_jsonl_mixes_files_and_dirs(jsonl_dir: Path, tmp_path: Path):
    single = tmp_path / "single.jsonl"
    single.write_text("{}\n")
    found = discover_jsonl([jsonl_dir, single, tmp_path / "nope.txt"])
    assert [f.name for f in found] == ["a.jsonl", "b.jsonl", "single.jsonl"]


async def test_submit_records_jobs_in_manifest(store: JobStore, jsonl_dir: Path):
    provider = FakeProvider()
    orchestrator = BatchOrchestrator(provider, store)

    jobs = await orchestrator.submit_paths([jsonl_dir], "/v1/responses")

    assert len(jobs) == 2
    assert {j.batch_id for j in jobs} == {"batch_1", "batch_2"}
    # persisted, pending, and carrying the endpoint for later resume
    pending = store.pending()
    assert len(pending) == 2
    assert all(j.endpoint == "/v1/responses" for j in pending)


async def test_submit_failure_skips_manifest(store: JobStore, jsonl_dir: Path):
    provider = FakeProvider()
    provider.fail_submit_for = {"a.jsonl"}
    orchestrator = BatchOrchestrator(provider, store)

    jobs = await orchestrator.submit_paths([jsonl_dir])

    assert len(jobs) == 1  # only b.jsonl made it
    assert len(store.pending()) == 1


async def test_watch_polls_to_completion_and_downloads(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    provider = FakeProvider()
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir])

    out = tmp_path / "out"
    result_file = out / "batch_1_results.jsonl"
    provider.status_scripts = {
        "batch_1": [running(0), running(1), done()],
        "batch_2": [failed("invalid_request: bad model")],
    }
    provider.files["batch_1"] = DownloadedResults(output_path=result_file)
    err_file = out / "batch_2_errors.jsonl"
    provider.files["batch_2"] = DownloadedResults(error_path=err_file)

    finished = await orchestrator.watch(jobs, out)

    by_id = {j.batch_id: j for j in finished}
    assert by_id["batch_1"].state == JobState.COMPLETED
    assert by_id["batch_1"].collection_state == CollectionState.COLLECTED
    assert by_id["batch_1"].output_path == str(result_file)
    assert by_id["batch_2"].state == JobState.FAILED
    assert by_id["batch_2"].collection_state == CollectionState.COLLECTED
    assert by_id["batch_2"].error_summary == "invalid_request: bad model"
    assert by_id["batch_2"].error_path == str(err_file)
    # manifest reflects terminal state — nothing left pending
    assert store.pending() == []


async def test_watch_resumes_from_manifest_after_restart(
    tmp_path: Path, jsonl_dir: Path
):
    """The issue #2 flow: submit in one process, watch in another."""
    db = tmp_path / "jobs.db"

    # process 1: submit-only, then exit
    store1 = JobStore(db)
    provider1 = FakeProvider()
    await BatchOrchestrator(provider1, store1).submit_paths([jsonl_dir])
    store1.close()

    # process 2: fresh store from the same db picks the jobs back up
    store2 = JobStore(db)
    pending = store2.pending()
    assert len(pending) == 2

    provider2 = FakeProvider()
    provider2.status_scripts = {"batch_1": [done()], "batch_2": [done()]}
    orchestrator = BatchOrchestrator(provider2, store2, check_interval=0)
    finished = await orchestrator.watch(pending, tmp_path / "out")

    assert all(j.state == JobState.COMPLETED for j in finished)
    assert store2.pending() == []
    store2.close()


async def test_watch_pauses_after_repeated_poll_failures_without_rewriting_remote_state(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    class DeadProvider(FakeProvider):
        async def status(self, batch_id: str) -> BatchStatus:
            raise ConnectionError("network down")

    provider = DeadProvider()
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir / "a.jsonl"])

    finished = await orchestrator.watch(jobs, tmp_path / "out")

    assert finished[0].state == JobState.PENDING
    assert finished[0].poll_failures == 5
    assert "network down" in finished[0].last_local_error
    assert store.pending()[0].batch_id == finished[0].batch_id
    assert store.actionable()[0].batch_id == finished[0].batch_id

    retried = await orchestrator.watch(store.actionable(), tmp_path / "out")
    assert retried[0].poll_failures == 10
    assert retried[0].state == JobState.PENDING


async def test_non_retryable_poll_failure_pauses_immediately_without_remote_failure(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    class AuthenticationFailure(RuntimeError):
        status_code = 401

    class UnauthorizedProvider(FakeProvider):
        async def status(self, batch_id: str) -> BatchStatus:
            raise AuthenticationFailure("invalid API key")

    provider = UnauthorizedProvider()
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir / "a.jsonl"])

    paused = (await orchestrator.watch(jobs, tmp_path / "out"))[0]

    assert paused.state == JobState.PENDING
    assert paused.poll_failures == 1
    assert "invalid API key" in paused.last_local_error
    assert paused.is_actionable


async def test_terminal_collection_failure_survives_restart_and_retries(
    tmp_path: Path, jsonl_dir: Path
):
    db = tmp_path / "jobs.db"
    store1 = JobStore(db)
    provider1 = FakeProvider()
    provider1.status_scripts = {"batch_1": [done()]}
    provider1.fetch_failures["batch_1"] = 1
    orchestrator1 = BatchOrchestrator(provider1, store1, check_interval=0)
    jobs = await orchestrator1.submit_paths([jsonl_dir / "a.jsonl"])

    first = (await orchestrator1.watch(jobs, tmp_path / "out"))[0]
    assert first.state == JobState.COMPLETED
    assert first.collection_state == CollectionState.FAILED
    assert "download interrupted" in first.last_local_error
    assert store1.pending() == []
    assert [job.batch_id for job in store1.actionable()] == ["batch_1"]
    store1.close()

    store2 = JobStore(db)
    provider2 = FakeProvider()
    result = tmp_path / "out" / "batch_1_results.jsonl"
    provider2.files["batch_1"] = DownloadedResults(output_path=result)
    second = (
        await BatchOrchestrator(provider2, store2).watch(
            store2.actionable(), tmp_path / "out"
        )
    )[0]

    assert second.collection_state == CollectionState.COLLECTED
    assert second.output_path == str(result)
    assert second.last_local_error is None
    assert store2.actionable() == []
    store2.close()


async def test_cancel_request_preserves_intermediate_state(
    store: JobStore, jsonl_dir: Path
):
    provider = FakeProvider()
    orchestrator = BatchOrchestrator(provider, store)
    job = (await orchestrator.submit_paths([jsonl_dir / "a.jsonl"]))[0]

    status = await orchestrator.request_cancel(job.batch_id)

    persisted = store.get(job.batch_id)
    assert status.state == JobState.CANCELLING
    assert persisted.state == JobState.CANCELLING
    assert persisted.collection_state == CollectionState.NOT_READY
    assert persisted.is_actionable


async def test_request_outcomes_do_not_override_terminal_job_state(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    provider = FakeProvider()
    provider.status_scripts = {
        "batch_1": [
            BatchStatus(
                provider_status="completed",
                state=JobState.COMPLETED,
                completed_count=8,
                failed_count=2,
                total_count=10,
            )
        ]
    }
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir / "a.jsonl"])

    finished = (await orchestrator.watch(jobs, tmp_path / "out"))[0]

    assert finished.state == JobState.COMPLETED
    assert finished.completed_count == 8
    assert finished.failed_count == 2
    assert finished.total_count == 10
    assert finished.collection_state == CollectionState.COLLECTED


async def test_terminal_job_with_no_provider_files_is_still_fully_collected(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    provider = FakeProvider()
    provider.status_scripts = {"batch_1": [failed("validation failed")]}
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir / "a.jsonl"])

    finished = (await orchestrator.watch(jobs, tmp_path / "out"))[0]

    assert finished.state == JobState.FAILED
    assert finished.collection_state == CollectionState.COLLECTED
    assert finished.output_path is None
    assert finished.error_path is None
    assert not finished.is_actionable


async def test_permanently_unavailable_artifacts_do_not_retry_forever(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    class ArchivedProvider(FakeProvider):
        async def fetch_results(self, batch_id, output_dir):
            raise ArtifactUnavailableError("provider retention window elapsed")

    provider = ArchivedProvider()
    provider.status_scripts = {"batch_1": [done()]}
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir / "a.jsonl"])

    finished = (await orchestrator.watch(jobs, tmp_path / "out"))[0]

    assert finished.state == JobState.COMPLETED
    assert finished.collection_state == CollectionState.UNAVAILABLE
    assert finished.error_summary == "provider retention window elapsed"
    assert not finished.is_actionable
    assert store.actionable() == []

    # A later status refresh must not turn permanent retention loss retryable again.
    orchestrator._apply_status(finished, done())
    assert finished.collection_state == CollectionState.UNAVAILABLE


async def test_events_are_emitted_in_order(store: JobStore, jsonl_dir: Path, tmp_path):
    provider = FakeProvider()
    provider.status_scripts = {"batch_1": [running(1), done()]}
    events = []
    orchestrator = BatchOrchestrator(
        provider, store, on_event=lambda e: events.append(e.kind), check_interval=0
    )
    await orchestrator.process([jsonl_dir / "a.jsonl"], tmp_path / "out")

    assert events[0] == "submitted"
    assert events[-1] == "finished"
    assert "status" in events
