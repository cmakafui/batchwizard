from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from google.genai import errors as genai_errors
from google.genai import types as genai_types

import batchwizard.providers.gemini as gemini_provider
from batchwizard.models import JobState
from batchwizard.providers.base import ArtifactUnavailableError
from batchwizard.providers.gemini import GeminiBatchProvider


def stats(**overrides):
    # The wire serializes proto int64 counters as strings.
    values = {
        "requestCount": "6",
        "successfulRequestCount": "3",
        "failedRequestCount": "1",
        "pendingRequestCount": "2",
    }
    values.update(overrides)
    return values


def operation(state: str = "JOB_STATE_RUNNING", *, error=None, **metadata_overrides):
    metadata = {
        "name": "batches/abc123",
        "displayName": "batchwizard-intent1",
        "model": "models/gemini-3.6-flash",
        "state": state,
        "createTime": "2026-07-30T09:00:00Z",
        "batchStats": stats(),
    }
    metadata.update(metadata_overrides)
    body = {"name": "batches/abc123", "metadata": metadata}
    if error is not None:
        body["error"] = error
    return body


def typed_job(**overrides):
    values = dict(
        name="batches/abc123",
        display_name="batchwizard-intent1",
        state=genai_types.JobState.JOB_STATE_SUCCEEDED,
        model="models/gemini-3.6-flash",
        create_time=datetime(2026, 7, 30, 9, tzinfo=UTC),
        dest=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class StubBatches:
    def __init__(self, job=None):
        self.job = job or typed_job()
        self.create_calls = 0
        self.created_kwargs = None
        self.cancelled: list[str] = []
        self.list_items = [self.job]

    async def create(self, *, model, src, config):
        self.create_calls += 1
        self.created_kwargs = {"model": model, "src": src, "config": config}
        return typed_job(state=genai_types.JobState.JOB_STATE_PENDING)

    async def get(self, *, name):
        return self.job

    async def cancel(self, *, name):
        self.cancelled.append(name)
        return None  # pinned by the SDK contract test

    async def list(self, *, config):
        items = self.list_items

        class Pager:
            async def __aiter__(self):
                for item in items:
                    yield item

        return Pager()


class StubFiles:
    def __init__(self):
        self.uploaded = None
        self.download_blob: bytes | Exception = b""
        self.downloaded: list[str] = []

    async def upload(self, *, file, config):
        self.uploaded = {"file": file, "config": config}
        return SimpleNamespace(name="files/in-1")

    async def download(self, *, file):
        self.downloaded.append(file)
        if isinstance(self.download_blob, Exception):
            raise self.download_blob
        return self.download_blob


class StubApiClient:
    def __init__(self, body: dict):
        self.body = body
        self.requests: list[tuple[str, str]] = []

    async def async_request(self, *, http_method, path, request_dict):
        self.requests.append((http_method, path))
        if isinstance(self.body, Exception):
            raise self.body
        return SimpleNamespace(body=json.dumps(self.body))


class StubClient:
    def __init__(self, batches=None, files=None, raw_body=None):
        self.batches = batches or StubBatches()
        self.files = files or StubFiles()
        self.closed = False

        async def aclose():
            self.closed = True

        self.aio = SimpleNamespace(
            batches=self.batches, files=self.files, aclose=aclose
        )
        self._api_client = StubApiClient(raw_body or operation())


def write_valid(path: Path, key: str = "row-1") -> None:
    path.write_text(
        json.dumps(
            {
                "key": key,
                "request": {
                    "contents": [{"parts": [{"text": "hello"}], "role": "user"}]
                },
            }
        )
        + "\n"
    )


def test_default_client_reads_gemini_key(monkeypatch):
    captured = {}
    client = StubClient()

    def make_client(**kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(gemini_provider.genai, "Client", make_client)
    monkeypatch.setattr(
        gemini_provider,
        "config",
        SimpleNamespace(get_api_key=lambda name: f"{name}-key"),
    )

    provider = GeminiBatchProvider()

    assert provider.client is client
    assert captured["api_key"] == "gemini-key"


async def test_submit_uploads_then_creates_with_intent_display_name(tmp_path: Path):
    client = StubClient()
    provider = GeminiBatchProvider(client=client)
    input_file = tmp_path / "input.jsonl"
    write_valid(input_file)

    submitted = await provider.submit(
        input_file, intent_id="intent1", model="gemini-3.6-flash"
    )

    assert submitted.batch_id == "batches/abc123"
    assert submitted.provider_status == "JOB_STATE_PENDING"
    assert submitted.endpoint is None
    assert client.files.uploaded["file"] == input_file
    assert client.files.uploaded["config"].mime_type == "jsonl"
    created = client.batches.created_kwargs
    assert created["model"] == "gemini-3.6-flash"
    assert created["src"] == "files/in-1"
    assert created["config"].display_name == "batchwizard-intent1"


async def test_submit_requires_model_and_rejects_endpoint(tmp_path: Path):
    client = StubClient()
    provider = GeminiBatchProvider(client=client)
    input_file = tmp_path / "input.jsonl"
    write_valid(input_file)

    with pytest.raises(ValueError, match="batch-level argument"):
        await provider.submit(input_file, intent_id="x")
    with pytest.raises(ValueError, match="OpenAI-specific"):
        await provider.submit(
            input_file, "/v1/responses", intent_id="x", model="gemini-3.6-flash"
        )
    assert client.batches.create_calls == 0


@pytest.mark.parametrize(
    ("line", "message"),
    [
        ("not-json\n", "invalid JSON"),
        ("[]\n", "must be a JSON object"),
        ('{"request": {"contents": [{}]}}\n', "key must be a nonempty string"),
        ('{"key": "", "request": {"contents": [{}]}}\n', "key must be"),
        ('{"key": "x"}\n', "request must be a JSON object"),
        ('{"key": "x", "request": {}}\n', "contents must be a nonempty array"),
        ('{"key": "x", "request": {"contents": []}}\n', "nonempty array"),
    ],
)
async def test_invalid_input_fails_with_line_number_before_http(
    tmp_path: Path, line: str, message: str
):
    client = StubClient()
    provider = GeminiBatchProvider(client=client)
    input_file = tmp_path / "bad.jsonl"
    input_file.write_text(line)

    with pytest.raises(ValueError, match=message) as caught:
        await provider.submit(input_file, intent_id="x", model="gemini-3.6-flash")

    assert f"{input_file}:1" in str(caught.value)
    assert client.batches.create_calls == 0


async def test_duplicate_keys_fail_before_http(tmp_path: Path):
    client = StubClient()
    provider = GeminiBatchProvider(client=client)
    input_file = tmp_path / "duplicate.jsonl"
    write_valid(input_file, "same")
    with input_file.open("a") as handle:
        handle.write(input_file.read_text())

    with pytest.raises(ValueError, match="duplicate key"):
        await provider.submit(input_file, intent_id="x", model="gemini-3.6-flash")
    assert client.batches.create_calls == 0


async def test_evolving_request_parameters_are_left_to_gemini(tmp_path: Path):
    client = StubClient()
    provider = GeminiBatchProvider(client=client)
    input_file = tmp_path / "future.jsonl"
    input_file.write_text(
        json.dumps(
            {
                "key": "row-1",
                "request": {
                    "contents": [{"parts": [{"text": "hi"}]}],
                    "future_generation_parameter": {"mode": "new"},
                },
            }
        )
        + "\n"
    )

    submitted = await provider.submit(
        input_file, intent_id="x", model="gemini-3.6-flash"
    )

    assert submitted.batch_id == "batches/abc123"
    assert client.batches.create_calls == 1


@pytest.mark.parametrize(
    ("wire_state", "expected"),
    [
        ("JOB_STATE_QUEUED", JobState.PENDING),
        ("JOB_STATE_PENDING", JobState.PENDING),
        ("JOB_STATE_RUNNING", JobState.RUNNING),
        ("JOB_STATE_CANCELLING", JobState.CANCELLING),
        ("JOB_STATE_SUCCEEDED", JobState.COMPLETED),
        ("JOB_STATE_FAILED", JobState.FAILED),
        ("JOB_STATE_EXPIRED", JobState.EXPIRED),
        ("JOB_STATE_CANCELLED", JobState.CANCELLED),
        # Gemini's wire is mid-rename; both spellings appear.
        ("BATCH_STATE_SUCCEEDED", JobState.COMPLETED),
        ("BATCH_STATE_RUNNING", JobState.RUNNING),
    ],
)
async def test_status_maps_both_wire_spellings(wire_state: str, expected: JobState):
    client = StubClient(raw_body=operation(wire_state))
    status = await GeminiBatchProvider(client=client).status("batches/abc123")

    assert status.state == expected
    assert status.provider_status == wire_state
    assert status.completed_count == 3
    assert status.failed_count == 1
    assert status.total_count == 6


@pytest.mark.parametrize(
    "wire_state",
    ["JOB_STATE_PAUSED", "JOB_STATE_UPDATING", "JOB_STATE_SOMETHING_NEW"],
)
async def test_unmapped_states_are_preserved_not_guessed(wire_state: str):
    client = StubClient(raw_body=operation(wire_state))
    status = await GeminiBatchProvider(client=client).status("batches/abc123")

    assert status.state is None
    assert status.provider_status == wire_state
    assert not status.is_terminal


async def test_partially_succeeded_is_terminal_but_not_remote_failure():
    client = StubClient(
        raw_body=operation(
            "JOB_STATE_PARTIALLY_SUCCEEDED",
            batchStats=stats(successfulRequestCount="2", failedRequestCount="4"),
        )
    )
    status = await GeminiBatchProvider(client=client).status("batches/abc123")

    assert status.state == JobState.COMPLETED
    assert status.is_terminal
    assert status.failed_count == 4
    assert status.error_summary == "4 failed"


async def test_failed_job_surfaces_operation_error_message():
    client = StubClient(
        raw_body=operation(
            "JOB_STATE_FAILED",
            error={"code": 13, "message": "internal error while batching"},
        )
    )
    status = await GeminiBatchProvider(client=client).status("batches/abc123")

    assert status.state == JobState.FAILED
    assert status.error_summary == "internal error while batching"


async def test_status_reads_counts_from_the_raw_wire_not_the_typed_job():
    client = StubClient(raw_body=operation("JOB_STATE_RUNNING"))
    await GeminiBatchProvider(client=client).status("batches/abc123")

    assert client._api_client.requests == [("GET", "batches/abc123")]


async def test_file_results_are_split_atomically(tmp_path: Path):
    rows = [
        {"key": "ok-1", "response": {"candidates": [{"content": {"parts": []}}]}},
        {"key": "ok-2", "response": {"candidates": [{"content": {"parts": []}}]}},
        {
            "key": "bad",
            "error": {"code": 400, "message": "x", "status": "INVALID_ARGUMENT"},
        },
    ]
    files = StubFiles()
    files.download_blob = "".join(json.dumps(r) + "\n" for r in rows).encode()
    batches = StubBatches(
        typed_job(dest=genai_types.BatchJobDestination(file_name="files/out-1"))
    )
    provider = GeminiBatchProvider(client=StubClient(batches=batches, files=files))

    results = await provider.fetch_results("batches/abc123", tmp_path)

    assert results.output_path == tmp_path / "abc123_results.jsonl"
    assert results.error_path == tmp_path / "abc123_errors.jsonl"
    success_keys = [
        json.loads(line)["key"] for line in results.output_path.read_text().splitlines()
    ]
    error_keys = [
        json.loads(line)["key"] for line in results.error_path.read_text().splitlines()
    ]
    assert success_keys == ["ok-1", "ok-2"]
    assert error_keys == ["bad"]
    assert results.error_summary == "1 INVALID_ARGUMENT"
    assert files.downloaded == ["files/out-1"]
    assert list(tmp_path.glob("*.part")) == []


async def test_interrupted_download_preserves_existing_files(tmp_path: Path):
    output = tmp_path / "abc123_results.jsonl"
    errors = tmp_path / "abc123_errors.jsonl"
    output.write_text("old output\n")
    errors.write_text("old errors\n")
    files = StubFiles()
    files.download_blob = ConnectionError("download interrupted")
    batches = StubBatches(
        typed_job(dest=genai_types.BatchJobDestination(file_name="files/out-1"))
    )
    provider = GeminiBatchProvider(client=StubClient(batches=batches, files=files))

    with pytest.raises(ConnectionError, match="interrupted"):
        await provider.fetch_results("batches/abc123", tmp_path)

    assert output.read_text() == "old output\n"
    assert errors.read_text() == "old errors\n"
    assert list(tmp_path.glob("*.part")) == []


async def test_missing_result_file_is_permanently_unavailable(tmp_path: Path):
    files = StubFiles()
    files.download_blob = genai_errors.ClientError(
        404,
        {"error": {"code": 404, "message": "File not found", "status": "NOT_FOUND"}},
    )
    batches = StubBatches(
        typed_job(dest=genai_types.BatchJobDestination(file_name="files/out-1"))
    )
    provider = GeminiBatchProvider(client=StubClient(batches=batches, files=files))

    with pytest.raises(ArtifactUnavailableError, match="no longer available"):
        await provider.fetch_results("batches/abc123", tmp_path)


async def test_other_download_errors_stay_retryable(tmp_path: Path):
    files = StubFiles()
    files.download_blob = genai_errors.ClientError(
        429,
        {"error": {"code": 429, "message": "quota", "status": "RESOURCE_EXHAUSTED"}},
    )
    batches = StubBatches(
        typed_job(dest=genai_types.BatchJobDestination(file_name="files/out-1"))
    )
    provider = GeminiBatchProvider(client=StubClient(batches=batches, files=files))

    with pytest.raises(genai_errors.ClientError):
        await provider.fetch_results("batches/abc123", tmp_path)


async def test_inline_results_keep_wire_native_field_names(tmp_path: Path):
    inline = [
        genai_types.InlinedResponse.model_validate(
            {
                "metadata": {"key": "row-1"},
                "response": {
                    "candidates": [
                        {"content": {"parts": [{"text": "ok"}], "role": "model"}}
                    ],
                    "usageMetadata": {"promptTokenCount": 4},
                },
            }
        ),
        genai_types.InlinedResponse.model_validate(
            {"metadata": {"key": "row-2"}, "error": {"code": 400, "message": "bad"}}
        ),
    ]
    batches = StubBatches(
        typed_job(dest=genai_types.BatchJobDestination(inlined_responses=inline))
    )
    provider = GeminiBatchProvider(client=StubClient(batches=batches))

    results = await provider.fetch_results("batches/abc123", tmp_path)

    success = json.loads(results.output_path.read_text())
    assert "usageMetadata" in success["response"]  # camelCase, not snake_case
    error_row = json.loads(results.error_path.read_text())
    assert error_row["error"]["code"] == 400


async def test_terminal_job_without_destination_collects_nothing(tmp_path: Path):
    provider = GeminiBatchProvider(client=StubClient(StubBatches(typed_job(dest=None))))

    results = await provider.fetch_results("batches/abc123", tmp_path)

    assert results.output_path is None
    assert results.error_path is None


async def test_cancel_requests_then_reports_immediate_status():
    client = StubClient(raw_body=operation("JOB_STATE_CANCELLING"))
    provider = GeminiBatchProvider(client=client)

    status = await provider.cancel("batches/abc123")

    assert client.batches.cancelled == ["batches/abc123"]
    assert status.state == JobState.CANCELLING
    assert status.provider_status == "JOB_STATE_CANCELLING"


async def test_list_jobs_parses_intents_from_display_names():
    batches = StubBatches()
    batches.list_items = [
        typed_job(name="batches/one", display_name="batchwizard-intent1"),
        typed_job(name="batches/two", display_name="my-own-job"),
    ]
    provider = GeminiBatchProvider(client=StubClient(batches=batches))

    jobs = await provider.list_jobs(limit=5)

    assert [job.batch_id for job in jobs] == ["batches/one", "batches/two"]
    assert jobs[0].intent_id == "intent1"
    assert jobs[1].intent_id is None
    assert jobs[0].provider_status == "JOB_STATE_SUCCEEDED"
    assert jobs[0].created_at == datetime(2026, 7, 30, 9, tzinfo=UTC)


async def test_close_closes_the_async_client():
    client = StubClient()
    await GeminiBatchProvider(client=client).close()
    assert client.closed
