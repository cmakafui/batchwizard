from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from anthropic import AnthropicError, NotFoundError

import batchwizard.providers.anthropic as anthropic_provider
from batchwizard.models import JobState
from batchwizard.providers.anthropic import (
    ANTHROPIC_BATCH_ENDPOINT,
    AnthropicBatchProvider,
)
from batchwizard.providers.base import ArtifactUnavailableError


def counts(**overrides):
    values = dict(processing=0, succeeded=3, errored=1, canceled=1, expired=1)
    values.update(overrides)
    return SimpleNamespace(**values)


def make_batch(**overrides):
    values = dict(
        id="msgbatch_1",
        processing_status="ended",
        request_counts=counts(),
        created_at=datetime(2026, 7, 18, tzinfo=UTC),
        ended_at=datetime(2026, 7, 18, 1, tzinfo=UTC),
        expires_at=datetime(2026, 7, 19, tzinfo=UTC),
        cancel_initiated_at=None,
        results_url="https://api.anthropic.test/results",
        archived_at=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


class StubItem:
    def __init__(self, custom_id: str, result_type: str, error_type: str | None = None):
        self.custom_id = custom_id
        error = None
        if error_type:
            error = SimpleNamespace(error=SimpleNamespace(type=error_type))
        self.result = SimpleNamespace(type=result_type, error=error)
        self._data = {
            "custom_id": custom_id,
            "result": {"type": result_type},
        }

    def to_dict(self):
        return self._data


class StubDecoder:
    def __init__(self, items, fail_after: int | None = None):
        self.items = items
        self.fail_after = fail_after
        self.closed = False

    async def __aiter__(self):
        for index, item in enumerate(self.items):
            if self.fail_after is not None and index == self.fail_after:
                raise ConnectionError("result stream interrupted")
            yield item

    async def close(self):
        self.closed = True


class StubPage:
    def __init__(self, items, first_page_size: int | None = None):
        self.items = items
        self.data = items[:first_page_size]

    async def __aiter__(self):
        for item in self.items:
            yield item


class StubBatches:
    def __init__(self, batch=None, decoder=None):
        self.batch = batch or make_batch()
        self.decoder = decoder or StubDecoder([])
        self.created_requests = []
        self.create_calls = 0
        self.results_error: Exception | None = None
        self.list_items = [self.batch]

    async def create(self, *, requests):
        self.create_calls += 1
        self.created_requests = requests
        return make_batch(processing_status="in_progress")

    async def retrieve(self, batch_id):
        return self.batch

    async def cancel(self, batch_id):
        return make_batch(processing_status="canceling")

    async def results(self, batch_id):
        if self.results_error:
            raise self.results_error
        return self.decoder

    async def list(self, limit):
        return StubPage(self.list_items, first_page_size=1)


class StubClient:
    def __init__(self, batches=None):
        self.batches = batches or StubBatches()
        self.messages = SimpleNamespace(batches=self.batches)
        self.closed = False

    async def close(self):
        self.closed = True


def write_valid(path: Path, custom_id: str = "row-1") -> None:
    path.write_text(
        json.dumps(
            {
                "custom_id": custom_id,
                "params": {
                    "model": "claude-opus-4-8",
                    "max_tokens": 32,
                    "messages": [{"role": "user", "content": "hello"}],
                },
            }
        )
        + "\n"
    )


def test_default_client_uses_aiohttp_without_hidden_sdk_retries(monkeypatch):
    transport = object()
    captured = {}
    client = StubClient()
    monkeypatch.setattr(anthropic_provider, "DefaultAioHttpClient", lambda: transport)

    def make_client(**kwargs):
        captured.update(kwargs)
        return client

    monkeypatch.setattr(anthropic_provider, "AsyncAnthropic", make_client)
    monkeypatch.setattr(
        anthropic_provider,
        "config",
        SimpleNamespace(get_api_key=lambda name: "key"),
    )

    provider = AnthropicBatchProvider()

    assert provider.client is client
    assert captured["http_client"] is transport
    assert captured["max_retries"] == 0


async def test_submit_parses_provider_native_jsonl(tmp_path: Path):
    client = StubClient()
    provider = AnthropicBatchProvider(client=client)
    input_file = tmp_path / "input.jsonl"
    write_valid(input_file)

    submitted = await provider.submit(input_file)

    assert submitted.batch_id == "msgbatch_1"
    assert submitted.provider_status == "in_progress"
    assert submitted.endpoint == ANTHROPIC_BATCH_ENDPOINT
    assert client.batches.created_requests[0]["custom_id"] == "row-1"
    assert client.batches.created_requests[0]["params"]["max_tokens"] == 32


@pytest.mark.parametrize(
    ("line", "message"),
    [
        ("not-json\n", "invalid JSON"),
        ("[]\n", "must be a JSON object"),
        ('{"params": {}}\n', "custom_id"),
        ('{"custom_id": "x"}\n', "params"),
        (
            '{"custom_id":"x","params":{"model":"m","max_tokens":1}}\n',
            "messages",
        ),
        (
            '{"custom_id":"x","params":{"model":"m","max_tokens":0,"messages":[{}]}}\n',
            "max_tokens",
        ),
        (
            '{"custom_id":"x","params":{"model":"m","max_tokens":1,'
            '"messages":[{}],"stream":true}}\n',
            "stream=true",
        ),
        (
            '{"custom_id":"not allowed","params":{"model":"m",'
            '"max_tokens":1,"messages":[{}]}}\n',
            "letters, numbers, hyphens",
        ),
    ],
)
async def test_invalid_input_fails_with_line_number_before_http(
    tmp_path: Path, line: str, message: str
):
    client = StubClient()
    provider = AnthropicBatchProvider(client=client)
    input_file = tmp_path / "bad.jsonl"
    input_file.write_text(line)

    with pytest.raises(ValueError, match=message) as caught:
        await provider.submit(input_file)

    assert f"{input_file}:1" in str(caught.value)
    assert client.batches.create_calls == 0


async def test_duplicate_ids_fail_before_http(tmp_path: Path):
    client = StubClient()
    provider = AnthropicBatchProvider(client=client)
    input_file = tmp_path / "duplicate.jsonl"
    write_valid(input_file, "same")
    with input_file.open("a") as handle:
        handle.write(input_file.read_text())

    with pytest.raises(ValueError, match="duplicate custom_id"):
        await provider.submit(input_file)
    assert client.batches.create_calls == 0


async def test_count_and_serialized_size_limits_fail_before_http(
    tmp_path: Path, monkeypatch
):
    client = StubClient()
    provider = AnthropicBatchProvider(client=client)
    input_file = tmp_path / "large.jsonl"
    write_valid(input_file, "one")
    first = input_file.read_text()
    input_file.write_text(first + first.replace("one", "two"))
    monkeypatch.setattr(anthropic_provider, "MAX_BATCH_REQUESTS", 1)
    with pytest.raises(ValueError, match="100,000-request limit"):
        await provider.submit(input_file)

    write_valid(input_file)
    monkeypatch.setattr(anthropic_provider, "MAX_BATCH_REQUESTS", 100_000)
    monkeypatch.setattr(
        anthropic_provider, "MAX_BATCH_BYTES", input_file.stat().st_size + 1
    )
    with pytest.raises(ValueError, match="serialized Anthropic request"):
        await provider.submit(input_file)
    assert client.batches.create_calls == 0


async def test_anthropic_rejects_openai_endpoint_before_http(tmp_path: Path):
    client = StubClient()
    provider = AnthropicBatchProvider(client=client)
    input_file = tmp_path / "input.jsonl"
    write_valid(input_file)

    with pytest.raises(ValueError, match="OpenAI-specific"):
        await provider.submit(input_file, "/v1/responses")
    assert client.batches.create_calls == 0


async def test_evolving_message_parameters_are_left_to_anthropic(tmp_path: Path):
    client = StubClient()
    provider = AnthropicBatchProvider(client=client)
    input_file = tmp_path / "future.jsonl"
    write_valid(input_file)
    request = json.loads(input_file.read_text())
    request["params"]["future_batch_parameter"] = {"mode": "new"}
    input_file.write_text(json.dumps(request) + "\n")

    await provider.submit(input_file)

    assert client.batches.created_requests[0]["params"]["future_batch_parameter"] == {
        "mode": "new"
    }


async def test_status_maps_lifecycle_and_all_request_outcomes():
    for raw, expected in (
        ("in_progress", JobState.RUNNING),
        ("canceling", JobState.CANCELLING),
        ("ended", JobState.COMPLETED),
    ):
        provider = AnthropicBatchProvider(
            client=StubClient(StubBatches(make_batch(processing_status=raw)))
        )
        status = await provider.status("msgbatch_1")
        assert status.state == expected
        assert status.completed_count == 3
        assert status.failed_count == 1
        assert status.cancelled_count == 1
        assert status.expired_count == 1
        assert status.total_count == 6

    provider = AnthropicBatchProvider(
        client=StubClient(
            StubBatches(make_batch(processing_status="future_provider_state"))
        )
    )
    unknown = await provider.status("msgbatch_1")
    assert unknown.provider_status == "future_provider_state"
    assert unknown.state is None


async def test_ended_with_every_request_errored_is_not_remote_failure():
    batch = make_batch(
        processing_status="ended",
        request_counts=counts(succeeded=0, errored=4, canceled=0, expired=0),
    )
    status = await AnthropicBatchProvider(client=StubClient(StubBatches(batch))).status(
        "msgbatch_1"
    )
    assert status.state == JobState.COMPLETED
    assert status.failed_count == 4
    assert status.error_summary == "4 errored"


async def test_results_are_split_atomically_and_decoder_is_closed(tmp_path: Path):
    decoder = StubDecoder(
        [
            StubItem("ok", "succeeded"),
            StubItem("bad", "errored", "invalid_request_error"),
            StubItem("late", "expired"),
            StubItem("stopped", "canceled"),
        ]
    )
    provider = AnthropicBatchProvider(client=StubClient(StubBatches(decoder=decoder)))

    results = await provider.fetch_results("msgbatch_1", tmp_path)

    success_lines = [
        json.loads(line) for line in results.output_path.read_text().splitlines()
    ]
    error_lines = [
        json.loads(line) for line in results.error_path.read_text().splitlines()
    ]
    assert [line["custom_id"] for line in success_lines] == ["ok"]
    assert [line["custom_id"] for line in error_lines] == ["bad", "late", "stopped"]
    assert results.error_summary == ("1 invalid_request_error, 1 expired, 1 canceled")
    assert decoder.closed
    assert list(tmp_path.glob("*.part")) == []


async def test_interrupted_result_stream_preserves_existing_files(tmp_path: Path):
    output = tmp_path / "msgbatch_1_results.jsonl"
    errors = tmp_path / "msgbatch_1_errors.jsonl"
    output.write_text("old output\n")
    errors.write_text("old errors\n")
    decoder = StubDecoder(
        [StubItem("ok", "succeeded"), StubItem("bad", "errored")],
        fail_after=1,
    )
    provider = AnthropicBatchProvider(client=StubClient(StubBatches(decoder=decoder)))

    with pytest.raises(ConnectionError, match="interrupted"):
        await provider.fetch_results("msgbatch_1", tmp_path)

    assert output.read_text() == "old output\n"
    assert errors.read_text() == "old errors\n"
    assert decoder.closed
    assert list(tmp_path.glob("*.part")) == []


async def test_archived_results_are_permanently_unavailable(tmp_path: Path):
    batch = make_batch(archived_at=datetime(2026, 8, 18, tzinfo=UTC), results_url=None)
    batches = StubBatches(batch)
    batches.results_error = AnthropicError("No results_url")
    provider = AnthropicBatchProvider(client=StubClient(batches))

    with pytest.raises(ArtifactUnavailableError, match="archived"):
        await provider.fetch_results("msgbatch_1", tmp_path)


@pytest.mark.parametrize(
    ("path", "permanently_unavailable"),
    [
        ("/v1/messages/batches/msgbatch_1/results", True),
        ("/v1/messages/batches/missing", False),
    ],
)
async def test_only_a_result_artifact_404_is_permanently_unavailable(
    tmp_path: Path, path: str, permanently_unavailable: bool
):
    request = httpx.Request("GET", f"https://api.anthropic.com{path}")
    response = httpx.Response(404, request=request)
    missing = NotFoundError("not found", response=response, body=None)
    batches = StubBatches()
    batches.results_error = missing
    provider = AnthropicBatchProvider(client=StubClient(batches))

    expected = ArtifactUnavailableError if permanently_unavailable else NotFoundError
    with pytest.raises(expected):
        await provider.fetch_results("msgbatch_1", tmp_path)


async def test_cancel_and_list_use_provider_neutral_models():
    batch = make_batch()
    client = StubClient(StubBatches(batch))
    provider = AnthropicBatchProvider(client=client)

    canceled = await provider.cancel("msgbatch_1")
    jobs = await provider.list_jobs(limit=5)

    assert canceled.state == JobState.CANCELLING
    assert jobs[0].created_at == batch.created_at
    assert jobs[0].completed_count == 3
    assert jobs[0].cancelled_count == 1


async def test_list_jobs_iterates_beyond_first_sdk_page():
    batches = StubBatches()
    batches.list_items = [make_batch(id=f"msgbatch_{index}") for index in range(3)]
    provider = AnthropicBatchProvider(client=StubClient(batches))

    jobs = await provider.list_jobs(limit=3)

    assert [job.batch_id for job in jobs] == [
        "msgbatch_0",
        "msgbatch_1",
        "msgbatch_2",
    ]
