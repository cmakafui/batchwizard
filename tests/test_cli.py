from types import SimpleNamespace

from typer.testing import CliRunner

import batchwizard.cli as cli
from batchwizard.models import (
    BatchStatus,
    CollectionState,
    JobRecord,
    JobState,
    ProviderJobSummary,
)
from batchwizard.store import JobStore

runner = CliRunner()


class CliProvider:
    name = "fake"

    def __init__(self):
        self.closed = False

    async def cancel(self, batch_id: str) -> BatchStatus:
        return BatchStatus(provider_status="cancelling", state=JobState.CANCELLING)

    async def list_jobs(self, limit: int = 20) -> list[ProviderJobSummary]:
        return [
            ProviderJobSummary(
                batch_id="batch_remote",
                provider_status="in_progress",
                created_at=1714508499,
                completed_count=3,
                failed_count=1,
                total_count=10,
            )
        ]

    async def close(self) -> None:
        self.closed = True


def test_status_defaults_to_actionable_jobs(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    store.add(JobRecord(batch_id="batch_active", input_path="/tmp/active.jsonl"))
    retry = store.add(JobRecord(batch_id="batch_retry", input_path="/tmp/retry.jsonl"))
    retry.state = JobState.COMPLETED
    retry.provider_status = "completed"
    retry.collection_state = CollectionState.FAILED
    retry.last_local_error = "Artifact collection failed: disk full"
    store.update(retry)
    done = store.add(JobRecord(batch_id="batch_done", input_path="/tmp/done.jsonl"))
    done.state = JobState.COMPLETED
    done.collection_state = CollectionState.COLLECTED
    store.update(done)
    store.close()
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))

    result = runner.invoke(cli.app, ["status"], env={"COLUMNS": "220"})

    assert result.exit_code == 0
    assert "batch_active" in result.stdout
    assert "batch_retry" in result.stdout
    assert "disk full" in result.stdout
    assert "batch_done" not in result.stdout


def test_list_jobs_uses_provider_contract_without_sdk_client(monkeypatch):
    provider = CliProvider()
    monkeypatch.setattr(cli, "get_api_key", lambda: "test-key")
    monkeypatch.setattr(cli, "get_provider", lambda: provider)

    result = runner.invoke(cli.app, ["list-jobs", "--limit", "3"])

    assert result.exit_code == 0
    assert "batch_remote" in result.stdout
    assert "in_progress" in result.stdout
    assert provider.closed


def test_cancel_records_provider_intermediate_state(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    store.add(JobRecord(batch_id="batch_cancel", input_path="/tmp/in.jsonl"))
    store.close()
    provider = CliProvider()
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda: "test-key")
    monkeypatch.setattr(cli, "get_provider", lambda: provider)

    result = runner.invoke(cli.app, ["cancel", "batch_cancel"])

    assert result.exit_code == 0
    assert "Cancellation requested" in result.stdout
    assert "cancelling" in result.stdout
    persisted = JobStore(database).get("batch_cancel")
    assert persisted.state == JobState.CANCELLING
    assert persisted.is_actionable
