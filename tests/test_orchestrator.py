from __future__ import annotations

from pathlib import Path

from batchwizard.models import BatchStatus, DownloadedResults, JobState
from batchwizard.processor import BatchOrchestrator
from batchwizard.store import JobStore
from batchwizard.utils import discover_jsonl


class FakeProvider:
    """Scriptable provider: each batch_id gets a queue of statuses to play back."""

    name = "fake"

    def __init__(self):
        self.counter = 0
        self.status_scripts: dict[str, list[BatchStatus]] = {}
        self.files: dict[str, DownloadedResults] = {}
        self.submitted: list[tuple[Path, str]] = []
        self.fail_submit_for: set[str] = set()

    async def submit(self, input_file: Path, endpoint: str) -> str:
        if input_file.name in self.fail_submit_for:
            raise RuntimeError("upload exploded")
        self.submitted.append((input_file, endpoint))
        self.counter += 1
        return f"batch_{self.counter}"

    async def status(self, batch_id: str) -> BatchStatus:
        script = self.status_scripts[batch_id]
        return script.pop(0) if len(script) > 1 else script[0]

    async def fetch_results(self, batch_id: str, output_dir: Path) -> DownloadedResults:
        return self.files.get(batch_id, DownloadedResults())

    async def cancel(self, batch_id: str) -> None: ...

    async def close(self) -> None: ...


def running(count=0):
    return BatchStatus(
        provider_status="in_progress", completed_count=count, total_count=2
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
    assert by_id["batch_1"].output_path == str(result_file)
    assert by_id["batch_2"].state == JobState.FAILED
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


async def test_watch_gives_up_after_repeated_poll_failures(
    store: JobStore, jsonl_dir: Path, tmp_path: Path
):
    class DeadProvider(FakeProvider):
        async def status(self, batch_id: str) -> BatchStatus:
            raise ConnectionError("network down")

    provider = DeadProvider()
    orchestrator = BatchOrchestrator(provider, store, check_interval=0)
    jobs = await orchestrator.submit_paths([jsonl_dir / "a.jsonl"])

    finished = await orchestrator.watch(jobs, tmp_path / "out")

    assert finished[0].state == JobState.FAILED
    assert "Lost contact" in finished[0].error_summary
    assert store.pending() == []


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
