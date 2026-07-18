from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import batchwizard.cli as cli
from batchwizard.models import (
    BatchStatus,
    CollectionState,
    DownloadedResults,
    JobRecord,
    JobState,
    ProviderJobSummary,
)
from batchwizard.store import JobStore

runner = CliRunner()


def test_root_help_describes_the_shipped_providers():
    result = runner.invoke(cli.app, ["--help"])

    assert result.exit_code == 0
    assert "Manage LLM batch jobs across OpenAI and Anthropic" in result.stdout
    assert "Gemini" not in result.stdout


@pytest.mark.parametrize(
    "command",
    [
        "process",
        "submit",
        "watch",
        "status",
        "configure",
        "reconcile",
        "list-jobs",
        "cancel",
        "download",
    ],
)
def test_command_help_surfaces(command):
    result = runner.invoke(cli.app, [command, "--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout


class CliProvider:
    name = "openai"

    def __init__(self, name: str = "openai"):
        self.name = name
        self.closed = False
        self.intent_id = None

    async def cancel(self, batch_id: str) -> BatchStatus:
        return BatchStatus(provider_status="cancelling", state=JobState.CANCELLING)

    async def list_jobs(self, limit: int = 20) -> list[ProviderJobSummary]:
        return [
            ProviderJobSummary(
                batch_id="batch_remote",
                provider_status="in_progress",
                endpoint="/v1/responses",
                intent_id=self.intent_id,
                created_at=1714508499,
                completed_count=3,
                failed_count=1,
                total_count=10,
            )
        ]

    async def status(self, batch_id: str) -> BatchStatus:
        return BatchStatus(provider_status="in_progress", state=JobState.RUNNING)

    async def fetch_results(self, batch_id, output_dir) -> DownloadedResults:
        return DownloadedResults()

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


def test_status_renders_provider_for_seeded_manifest(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    store.add(
        JobRecord(
            provider="anthropic",
            batch_id="msgbatch_seeded",
            input_path="/tmp/anthropic.jsonl",
            endpoint="/v1/messages/batches",
            provider_status="in_progress",
        )
    )
    store.close()
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))

    result = runner.invoke(cli.app, ["status"], env={"COLUMNS": "180"})

    assert result.exit_code == 0
    assert "Provider" in result.stdout
    assert "anthropic" in result.stdout
    assert "msgbatch_seeded" in result.stdout


def test_list_jobs_uses_provider_contract_without_sdk_client(monkeypatch):
    provider = CliProvider()
    monkeypatch.setattr(cli, "get_api_key", lambda name: "test-key")
    monkeypatch.setattr(cli, "get_provider", lambda name: provider)

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
    monkeypatch.setattr(cli, "get_api_key", lambda name: "test-key")
    monkeypatch.setattr(cli, "get_provider", lambda name: provider)

    result = runner.invoke(cli.app, ["cancel", "batch_cancel"])

    assert result.exit_code == 0
    assert "Cancellation requested" in result.stdout
    assert "cancelling" in result.stdout
    persisted = JobStore(database).get("batch_cancel")
    assert persisted.state == JobState.CANCELLING
    assert persisted.is_actionable


def test_cancel_infers_anthropic_from_tracked_job(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    store.add(
        JobRecord(
            provider="anthropic",
            batch_id="msgbatch_1",
            input_path="/tmp/in.jsonl",
            endpoint="/v1/messages/batches",
        )
    )
    store.close()
    selected = []
    provider = CliProvider("anthropic")
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda name: "anthropic-key")

    def get_provider(name):
        selected.append(name)
        return provider

    monkeypatch.setattr(cli, "get_provider", get_provider)

    result = runner.invoke(cli.app, ["cancel", "msgbatch_1"])

    assert result.exit_code == 0
    assert selected == ["anthropic"]
    persisted = JobStore(database).get("msgbatch_1", provider="anthropic")
    assert persisted.state == JobState.CANCELLING


class WatchProvider:
    def __init__(self, name: str):
        self.name = name
        self.status_calls = []
        self.closed = False

    async def status(self, batch_id: str) -> BatchStatus:
        self.status_calls.append(batch_id)
        return BatchStatus(provider_status="ended", state=JobState.COMPLETED)

    async def fetch_results(self, batch_id, output_dir):
        return DownloadedResults()

    async def close(self):
        self.closed = True


def test_watch_groups_jobs_by_provider(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    store.add(
        JobRecord(
            provider="openai", batch_id="openai_1", input_path="/tmp/openai.jsonl"
        )
    )
    store.add(
        JobRecord(
            provider="anthropic",
            batch_id="anthropic_1",
            input_path="/tmp/anthropic.jsonl",
            endpoint="/v1/messages/batches",
        )
    )
    store.close()
    providers = {
        "openai": WatchProvider("openai"),
        "anthropic": WatchProvider("anthropic"),
    }
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda name: f"{name}-key")
    monkeypatch.setattr(cli, "get_provider", lambda name: providers[name])

    result = runner.invoke(
        cli.app,
        ["watch", "--check-interval", "0", "--output-directory", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert providers["openai"].status_calls == ["openai_1"]
    assert providers["anthropic"].status_calls == ["anthropic_1"]
    assert providers["openai"].closed and providers["anthropic"].closed
    reopened = JobStore(database)
    assert (
        reopened.get("openai_1", provider="openai").collection_state
        == CollectionState.COLLECTED
    )
    assert (
        reopened.get("anthropic_1", provider="anthropic").collection_state
        == CollectionState.COLLECTED
    )


def test_missing_provider_key_does_not_block_other_watch_groups(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    store.add(JobRecord(provider="openai", batch_id="openai_1", input_path="a"))
    store.add(
        JobRecord(
            provider="anthropic",
            batch_id="anthropic_1",
            input_path="b",
            endpoint="/v1/messages/batches",
        )
    )
    store.close()
    openai = WatchProvider("openai")
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(
        cli, "get_api_key", lambda name: "openai-key" if name == "openai" else None
    )
    monkeypatch.setattr(cli, "get_provider", lambda name: openai)

    result = runner.invoke(cli.app, ["watch", "--check-interval", "0"])

    assert result.exit_code == 0
    reopened = JobStore(database)
    assert reopened.get("openai_1", provider="openai").state == JobState.COMPLETED
    anthropic = reopened.get("anthropic_1", provider="anthropic")
    assert anthropic.state == JobState.PENDING
    assert "Missing ANTHROPIC API key" in anthropic.last_local_error
    assert anthropic.is_actionable


def test_unknown_provider_lists_available_choices(monkeypatch):
    result = runner.invoke(cli.app, ["list-jobs", "--provider", "not-real"])

    assert result.exit_code != 0
    assert "anthropic" in result.output
    assert "openai" in result.output


def test_anthropic_rejects_explicit_openai_endpoint_before_key_check(tmp_path):
    input_file = tmp_path / "input.jsonl"
    input_file.write_text("{}\n")

    result = runner.invoke(
        cli.app,
        [
            "submit",
            "--provider",
            "anthropic",
            "--endpoint",
            "/v1/responses",
            str(input_file),
        ],
    )

    assert result.exit_code != 0
    assert "OpenAI-specific" in result.output


def test_untracked_anthropic_id_infers_provider_for_cancel(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    selected = []
    provider = CliProvider("anthropic")
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda name: "key")
    monkeypatch.setattr(
        cli,
        "get_provider",
        lambda name: selected.append(name) or provider,
    )

    result = runner.invoke(cli.app, ["cancel", "msgbatch_untracked"])

    assert result.exit_code == 0
    assert selected == ["anthropic"]


def test_untracked_anthropic_id_infers_provider_for_download(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    selected = []
    provider = CliProvider("anthropic")
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda name: "key")
    monkeypatch.setattr(
        cli,
        "get_provider",
        lambda name: selected.append(name) or provider,
    )

    result = runner.invoke(
        cli.app,
        ["download", "msgbatch_untracked", "--output-directory", str(tmp_path)],
    )

    assert result.exit_code == 0
    assert selected == ["anthropic"]


def test_untracked_unknown_id_requires_provider(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=tmp_path / "jobs.db"))

    result = runner.invoke(cli.app, ["download", "opaque-id"])

    assert result.exit_code != 0
    assert "Cannot infer the provider" in result.output


def test_reconcile_matches_openai_intent_metadata(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    intent = store.add(
        JobRecord(
            provider="openai",
            input_path="/tmp/input.jsonl",
            state=JobState.SUBMITTING,
            provider_status="submitting",
        )
    )
    store.close()
    provider = CliProvider("openai")
    provider.intent_id = intent.intent_id
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda name: "key")
    monkeypatch.setattr(cli, "get_provider", lambda name: provider)

    result = runner.invoke(cli.app, ["reconcile", intent.intent_id])

    assert result.exit_code == 0
    assert "Attached intent" in result.output
    recovered = JobStore(database).get("batch_remote", provider="openai")
    assert recovered.intent_id == intent.intent_id
    assert recovered.state == JobState.RUNNING
    assert recovered.endpoint == "/v1/responses"


def test_reconcile_attaches_manually_identified_anthropic_batch(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    intent = store.add(
        JobRecord(
            provider="anthropic",
            input_path="/tmp/input.jsonl",
            state=JobState.SUBMITTING,
            provider_status="submitting",
        )
    )
    store.close()
    provider = CliProvider("anthropic")
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))
    monkeypatch.setattr(cli, "get_api_key", lambda name: "key")
    monkeypatch.setattr(cli, "get_provider", lambda name: provider)

    result = runner.invoke(
        cli.app,
        ["reconcile", intent.intent_id, "--batch-id", "msgbatch_recovered"],
    )

    assert result.exit_code == 0
    recovered = JobStore(database).get("msgbatch_recovered", provider="anthropic")
    assert recovered.intent_id == intent.intent_id
    assert recovered.state == JobState.RUNNING


def test_reconcile_can_discard_confirmed_nonexistent_intent(tmp_path, monkeypatch):
    database = tmp_path / "jobs.db"
    store = JobStore(database)
    intent = store.add(
        JobRecord(input_path="/tmp/input.jsonl", state=JobState.SUBMITTING)
    )
    store.close()
    monkeypatch.setattr(cli, "config", SimpleNamespace(db_file=database))

    result = runner.invoke(cli.app, ["reconcile", intent.intent_id, "--discard"])

    assert result.exit_code == 0
    assert "Discarded unresolved intent" in result.output
    assert JobStore(database).get_intent(intent.intent_id) is None


def test_status_age_is_compact():
    now = datetime(2026, 7, 18, 12, tzinfo=UTC)

    assert cli._format_age((now - timedelta(seconds=20)).isoformat(), now) == "just now"
    assert cli._format_age((now - timedelta(minutes=3)).isoformat(), now) == "3m ago"
    assert cli._format_age((now - timedelta(hours=2)).isoformat(), now) == "2h ago"
