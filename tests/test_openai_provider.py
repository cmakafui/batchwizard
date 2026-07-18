from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from openai import NotFoundError

import batchwizard.providers.openai as openai_provider
from batchwizard.models import JobState
from batchwizard.providers.base import ArtifactUnavailableError
from batchwizard.providers.openai import OpenAIBatchProvider


class StubStreamResponse:
    def __init__(self, content: bytes, fail: bool = False):
        self.content = content
        self.fail = fail

    async def stream_to_file(self, path: Path):
        path.write_bytes(self.content[: max(1, len(self.content) // 2)])
        if self.fail:
            raise ConnectionError("stream interrupted")
        path.write_bytes(self.content)


class StubStreamContext:
    def __init__(self, response: StubStreamResponse):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, *exc):
        return None


class StubStreamingFiles:
    def __init__(self, client):
        self.client = client

    def content(self, file_id):
        if file_id in self.client.not_found_for:
            request = httpx.Request(
                "GET", f"https://api.openai.test/v1/files/{file_id}/content"
            )
            response = httpx.Response(404, request=request)
            raise NotFoundError("deleted", response=response, body=None)
        return StubStreamContext(
            StubStreamResponse(
                self.client._file_contents[file_id],
                fail=file_id in self.client.fail_stream_for,
            )
        )


class StubClient:
    """Minimal stand-in for AsyncOpenAI covering what the provider touches."""

    def __init__(self, batch):
        self._batch = batch
        self._file_contents = {}
        self.files = SimpleNamespace(
            create=self._file_create,
            content=self._file_content,
            with_streaming_response=StubStreamingFiles(self),
        )
        self.batches = SimpleNamespace(
            create=self._batch_create,
            retrieve=self._batch_retrieve,
            cancel=self._batch_cancel,
            list=self._batch_list,
        )
        self.created_batches = []
        self.uploaded_files = []
        self.fail_stream_for = set()
        self.not_found_for = set()
        self.list_items = [self._batch]

    def add_file(self, file_id: str, content: bytes):
        self._file_contents[file_id] = content

    async def _file_create(self, file, purpose):
        self.uploaded_files.append((file, purpose))
        return SimpleNamespace(id="file_in_1")

    async def _file_content(self, file_id):
        return SimpleNamespace(content=self._file_contents[file_id])

    async def _batch_create(self, **kwargs):
        self.created_batches.append(kwargs)
        return SimpleNamespace(id="batch_new", status="validating")

    async def _batch_retrieve(self, batch_id):
        return self._batch

    async def _batch_cancel(self, batch_id):
        return make_batch(id=batch_id, status="cancelling")

    async def _batch_list(self, limit):
        return StubPage(self.list_items, first_page_size=1)

    async def close(self): ...


class StubPage:
    def __init__(self, items, first_page_size: int | None = None):
        self.items = items
        self.data = items[:first_page_size]

    async def __aiter__(self):
        for item in self.items:
            yield item


def make_batch(**overrides):
    defaults = dict(
        id="batch_1",
        created_at=1714508499,
        status="in_progress",
        output_file_id=None,
        error_file_id=None,
        errors=None,
        request_counts=SimpleNamespace(completed=3, failed=1, total=10),
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_default_client_uses_aiohttp_without_hidden_sdk_retries(monkeypatch):
    transport = object()
    captured = {}
    client = StubClient(make_batch())
    monkeypatch.setattr(openai_provider, "DefaultAioHttpClient", lambda: transport)

    def make_client(**kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(openai_provider, "AsyncOpenAI", make_client)
    monkeypatch.setattr(
        openai_provider,
        "config",
        SimpleNamespace(get_api_key=lambda name: "key"),
    )

    provider = OpenAIBatchProvider()

    assert provider.client is client
    assert captured["http_client"] is transport
    assert captured["max_retries"] == 0


async def test_submit_uploads_and_creates_batch(tmp_path: Path):
    client = StubClient(make_batch())
    provider = OpenAIBatchProvider(client=client)
    input_file = tmp_path / "in.jsonl"
    input_file.write_text(
        '{"custom_id":"1","method":"POST","url":"/v1/responses",'
        '"body":{"model":"gpt-5.4","input":"hello"}}\n'
    )

    submitted = await provider.submit(input_file, "/v1/responses")

    assert submitted.batch_id == "batch_new"
    assert submitted.provider_status == "validating"
    assert submitted.endpoint == "/v1/responses"
    assert client.uploaded_files == [(input_file, "batch")]
    assert client.created_batches == [
        {
            "input_file_id": "file_in_1",
            "endpoint": "/v1/responses",
            "completion_window": "24h",
        }
    ]


async def test_submit_correlates_openai_batch_with_durable_intent(tmp_path: Path):
    client = StubClient(make_batch())
    provider = OpenAIBatchProvider(client=client)
    input_file = tmp_path / "in.jsonl"
    input_file.write_text("{}\n")

    await provider.submit(input_file, intent_id="intent-123")

    assert client.created_batches[0]["metadata"] == {"batchwizard_intent": "intent-123"}


async def test_status_maps_active_states():
    provider = OpenAIBatchProvider(client=StubClient(make_batch(status="in_progress")))
    status = await provider.status("batch_1")
    assert status.state == JobState.RUNNING
    assert status.provider_status == "in_progress"
    assert status.completed_count == 3
    assert status.total_count == 10

    for raw, expected in [
        ("validating", JobState.PENDING),
        ("finalizing", JobState.RUNNING),
        ("cancelling", JobState.CANCELLING),
    ]:
        provider = OpenAIBatchProvider(client=StubClient(make_batch(status=raw)))
        assert (await provider.status("batch_1")).state == expected


async def test_status_maps_terminal_states():
    for raw, expected in [
        ("completed", JobState.COMPLETED),
        ("failed", JobState.FAILED),
        ("expired", JobState.EXPIRED),
        ("cancelled", JobState.CANCELLED),
    ]:
        provider = OpenAIBatchProvider(client=StubClient(make_batch(status=raw)))
        status = await provider.status("batch_1")
        assert status.state == expected


async def test_unknown_provider_status_is_preserved_without_guessing():
    provider = OpenAIBatchProvider(
        client=StubClient(make_batch(status="new_provider_state"))
    )
    status = await provider.status("batch_1")
    assert status.provider_status == "new_provider_state"
    assert status.state is None


async def test_status_extracts_error_summary():
    """Issue #3: batch-level errors must surface to the user."""
    errors = SimpleNamespace(
        data=[
            SimpleNamespace(
                code="insufficient_funds",
                message="Billing hard limit reached",
                line=None,
            ),
            SimpleNamespace(code="invalid_request", message="bad model", line=7),
        ]
    )
    provider = OpenAIBatchProvider(
        client=StubClient(make_batch(status="failed", errors=errors))
    )
    status = await provider.status("batch_1")
    assert status.state == JobState.FAILED
    assert (
        status.error_summary
        == "insufficient_funds: Billing hard limit reached; invalid_request: bad model (line 7)"
    )


async def test_fetch_results_downloads_output_and_error_files(tmp_path: Path):
    """Issue #3: per-request error files must be downloaded, not ignored."""
    client = StubClient(
        make_batch(
            status="completed", output_file_id="file_out", error_file_id="file_err"
        )
    )
    client.add_file("file_out", b'{"custom_id": "1", "response": {}}\n')
    client.add_file(
        "file_err", b'{"custom_id": "2", "error": {"code": "rate_limit"}}\n'
    )
    provider = OpenAIBatchProvider(client=client)

    results = await provider.fetch_results("batch_1", tmp_path / "out")

    assert results.output_path.read_bytes() == b'{"custom_id": "1", "response": {}}\n'
    assert b"rate_limit" in results.error_path.read_bytes()
    assert results.output_path.name == "batch_1_results.jsonl"
    assert results.error_path.name == "batch_1_errors.jsonl"
    assert list((tmp_path / "out").glob("*.part")) == []


async def test_interrupted_download_is_not_exposed_as_final_file(tmp_path: Path):
    client = StubClient(make_batch(status="completed", output_file_id="file_out"))
    client.add_file("file_out", b'{"custom_id": "1"}\n')
    client.fail_stream_for.add("file_out")
    provider = OpenAIBatchProvider(client=client)

    with pytest.raises(ConnectionError, match="stream interrupted"):
        await provider.fetch_results("batch_1", tmp_path / "out")

    assert not (tmp_path / "out" / "batch_1_results.jsonl").exists()
    assert list((tmp_path / "out").glob("*.part")) == []


async def test_deleted_openai_artifact_is_permanently_unavailable(tmp_path: Path):
    client = StubClient(make_batch(status="completed", output_file_id="file_gone"))
    client.not_found_for.add("file_gone")
    provider = OpenAIBatchProvider(client=client)

    with pytest.raises(ArtifactUnavailableError, match="no longer available"):
        await provider.fetch_results("batch_1", tmp_path / "out")

    assert list((tmp_path / "out").glob("*.part")) == []


async def test_fetch_results_with_no_files(tmp_path: Path):
    provider = OpenAIBatchProvider(client=StubClient(make_batch(status="failed")))
    results = await provider.fetch_results("batch_1", tmp_path / "out")
    assert results.output_path is None
    assert results.error_path is None


async def test_cancel_returns_immediate_cancelling_status():
    provider = OpenAIBatchProvider(client=StubClient(make_batch()))
    status = await provider.cancel("batch_1")
    assert status.state == JobState.CANCELLING
    assert status.provider_status == "cancelling"


async def test_list_jobs_returns_provider_neutral_summaries():
    provider = OpenAIBatchProvider(client=StubClient(make_batch(status="completed")))
    jobs = await provider.list_jobs(limit=3)
    assert len(jobs) == 1
    assert jobs[0].batch_id == "batch_1"
    assert jobs[0].provider_status == "completed"
    assert jobs[0].completed_count == 3


async def test_list_jobs_iterates_beyond_first_sdk_page():
    client = StubClient(make_batch())
    client.list_items = [make_batch(id=f"batch_{index}") for index in range(3)]
    provider = OpenAIBatchProvider(client=client)

    jobs = await provider.list_jobs(limit=3)

    assert [job.batch_id for job in jobs] == ["batch_0", "batch_1", "batch_2"]


async def test_invalid_endpoint_fails_before_upload(tmp_path: Path):
    import pytest

    client = StubClient(make_batch())
    provider = OpenAIBatchProvider(client=client)
    input_file = tmp_path / "in.jsonl"
    input_file.write_text("{}\n")

    with pytest.raises(ValueError, match="Unsupported OpenAI Batch endpoint"):
        await provider.submit(input_file, "/v1/not-real")

    assert client.uploaded_files == []
