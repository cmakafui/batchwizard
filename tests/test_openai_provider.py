from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from batchwizard.models import JobState
from batchwizard.providers.openai import OpenAIBatchProvider


class StubClient:
    """Minimal stand-in for AsyncOpenAI covering what the provider touches."""

    def __init__(self, batch):
        self._batch = batch
        self._file_contents = {}
        self.files = SimpleNamespace(
            create=self._file_create, content=self._file_content
        )
        self.batches = SimpleNamespace(
            create=self._batch_create, retrieve=self._batch_retrieve
        )
        self.created_batches = []

    def add_file(self, file_id: str, content: bytes):
        self._file_contents[file_id] = content

    async def _file_create(self, file, purpose):
        return SimpleNamespace(id="file_in_1")

    async def _file_content(self, file_id):
        return SimpleNamespace(content=self._file_contents[file_id])

    async def _batch_create(self, **kwargs):
        self.created_batches.append(kwargs)
        return SimpleNamespace(id="batch_new")

    async def _batch_retrieve(self, batch_id):
        return self._batch


def make_batch(**overrides):
    defaults = dict(
        id="batch_1",
        status="in_progress",
        output_file_id=None,
        error_file_id=None,
        errors=None,
        request_counts=SimpleNamespace(completed=3, failed=1, total=10),
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


async def test_submit_uploads_and_creates_batch(tmp_path: Path):
    client = StubClient(make_batch())
    provider = OpenAIBatchProvider(client=client)
    input_file = tmp_path / "in.jsonl"
    input_file.write_text('{"custom_id": "1"}\n')

    batch_id = await provider.submit(input_file, "/v1/responses")

    assert batch_id == "batch_new"
    assert client.created_batches == [
        {
            "input_file_id": "file_in_1",
            "endpoint": "/v1/responses",
            "completion_window": "24h",
        }
    ]


async def test_status_running_has_no_terminal_state():
    provider = OpenAIBatchProvider(client=StubClient(make_batch(status="in_progress")))
    status = await provider.status("batch_1")
    assert status.state is None
    assert status.provider_status == "in_progress"
    assert status.completed_count == 3
    assert status.total_count == 10


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


async def test_fetch_results_with_no_files(tmp_path: Path):
    provider = OpenAIBatchProvider(client=StubClient(make_batch(status="failed")))
    results = await provider.fetch_results("batch_1", tmp_path / "out")
    assert results.output_path is None
    assert results.error_path is None
