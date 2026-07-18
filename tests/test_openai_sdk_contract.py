"""Thin contract tests at the boundary with the installed OpenAI SDK.

Most provider tests use a small stub so they stay fast and focused.  This file
keeps the assumptions discovered while probing the real SDK executable.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
from openai import AsyncOpenAI

from batchwizard.providers.openai import OpenAIBatchProvider


def _batch(status: str, batch_id: str = "batch_1") -> dict:
    return {
        "id": batch_id,
        "object": "batch",
        "endpoint": "/v1/responses",
        "input_file_id": "file_in_1",
        "completion_window": "24h",
        "status": status,
        "created_at": 1_714_508_499,
        "metadata": {"batchwizard_intent": "intent-sdk-contract"},
        "request_counts": {"completed": 0, "failed": 0, "total": 1},
    }


async def test_real_sdk_supports_responses_and_preserves_future_status(
    tmp_path: Path,
):
    requests: list[tuple[str, str, bytes]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        requests.append((request.method, request.url.path, body))
        if request.method == "POST" and request.url.path == "/v1/files":
            return httpx.Response(
                200,
                json={
                    "id": "file_in_1",
                    "object": "file",
                    "bytes": len(body),
                    "created_at": 1_714_508_499,
                    "filename": "input.jsonl",
                    "purpose": "batch",
                    "status": "processed",
                },
            )
        if request.method == "POST" and request.url.path == "/v1/batches":
            return httpx.Response(200, json=_batch("validating", "batch_new"))
        if request.method == "GET" and request.url.path == "/v1/batches":
            return httpx.Response(
                200,
                json={
                    "object": "list",
                    "data": [_batch("validating", "batch_new")],
                    "first_id": "batch_new",
                    "last_id": "batch_new",
                    "has_more": False,
                },
            )
        if request.method == "GET" and request.url.path == "/v1/batches/batch_new":
            return httpx.Response(
                200, json=_batch("future_provider_state", "batch_new")
            )
        return httpx.Response(404, json={"error": {"message": "unexpected request"}})

    transport = httpx.MockTransport(handle)
    http_client = httpx.AsyncClient(transport=transport)
    sdk = AsyncOpenAI(
        api_key="test-key",
        base_url="https://api.openai.test/v1",
        http_client=http_client,
        max_retries=0,
    )
    provider = OpenAIBatchProvider(client=sdk)
    input_file = tmp_path / "input.jsonl"
    input_file.write_text(
        '{"custom_id":"1","method":"POST","url":"/v1/responses",'
        '"body":{"model":"gpt-5.4","input":"hello"}}\n'
    )

    try:
        submitted = await provider.submit(
            input_file, "/v1/responses", intent_id="intent-sdk-contract"
        )
        status = await provider.status(submitted.batch_id)
        listed = await provider.list_jobs(limit=1)
    finally:
        await provider.close()

    batch_request = next(
        body for method, path, body in requests if path == "/v1/batches"
    )
    assert json.loads(batch_request) == {
        "completion_window": "24h",
        "endpoint": "/v1/responses",
        "input_file_id": "file_in_1",
        "metadata": {"batchwizard_intent": "intent-sdk-contract"},
    }
    upload_request = next(
        body for method, path, body in requests if path == "/v1/files"
    )
    assert b'filename="input.jsonl"' in upload_request
    assert b'"url":"/v1/responses"' in upload_request
    assert listed[0].intent_id == "intent-sdk-contract"
    assert status.provider_status == "future_provider_state"
    assert status.state is None
