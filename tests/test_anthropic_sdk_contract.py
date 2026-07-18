"""Executable assumptions at the boundary with the current Anthropic SDK."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import anthropic
import httpx
from anthropic import AsyncAnthropic


def _batch(status: str, batch_id: str) -> dict:
    ended = status == "ended"
    return {
        "id": batch_id,
        "type": "message_batch",
        "processing_status": status,
        "request_counts": {
            "processing": 0 if ended else 1,
            "succeeded": 1 if ended else 0,
            "errored": 0,
            "canceled": 0,
            "expired": 0,
        },
        "ended_at": "2026-07-18T08:00:00Z" if ended else None,
        "created_at": "2026-07-18T07:00:00Z",
        "expires_at": "2026-07-19T07:00:00Z",
        "cancel_initiated_at": None,
        "results_url": (
            f"https://api.anthropic.test/v1/messages/batches/{batch_id}/results"
            if ended
            else None
        ),
        "archived_at": None,
    }


async def test_real_sdk_ga_async_batch_contract():
    calls: list[tuple[str, str, bytes]] = []
    result = {
        "custom_id": "row-1",
        "result": {
            "type": "succeeded",
            "message": {
                "id": "msg_1",
                "type": "message",
                "role": "assistant",
                "model": "claude-opus-4-8",
                "content": [{"type": "text", "text": "hello"}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 4, "output_tokens": 2},
            },
        },
    }

    async def handle(request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        calls.append((request.method, request.url.path, body))
        path = request.url.path
        if request.method == "POST" and path == "/v1/messages/batches":
            return httpx.Response(200, json=_batch("in_progress", "msgbatch_new"))
        if request.method == "POST" and path.endswith("/msgbatch_new/cancel"):
            return httpx.Response(200, json=_batch("canceling", "msgbatch_new"))
        if request.method == "GET" and path == "/v1/messages/batches":
            return httpx.Response(
                200,
                json={
                    "data": [_batch("in_progress", "msgbatch_new")],
                    "has_more": False,
                    "first_id": "msgbatch_new",
                    "last_id": "msgbatch_new",
                },
            )
        if path == "/v1/messages/batches/msgbatch_new":
            return httpx.Response(
                200, json=_batch("future_provider_state", "msgbatch_new")
            )
        if path == "/v1/messages/batches/msgbatch_results":
            return httpx.Response(200, json=_batch("ended", "msgbatch_results"))
        if path.endswith("/msgbatch_results/results"):
            return httpx.Response(
                200,
                text=json.dumps(result) + "\n",
                headers={"content-type": "application/x-jsonl"},
            )
        return httpx.Response(404, json={"error": {"message": "unexpected"}})

    client = AsyncAnthropic(
        api_key="test-key",
        base_url="https://api.anthropic.test",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        max_retries=0,
    )
    try:
        created = await client.messages.batches.create(
            requests=[
                {
                    "custom_id": "row-1",
                    "params": {
                        "model": "claude-opus-4-8",
                        "max_tokens": 32,
                        "messages": [{"role": "user", "content": "hello"}],
                    },
                }
            ]
        )
        future = await client.messages.batches.retrieve(created.id)
        canceled = await client.messages.batches.cancel(created.id)
        page = await client.messages.batches.list(limit=20)
        decoder = await client.messages.batches.results("msgbatch_results")
        decoded = [item async for item in decoder]
        await decoder.close()
    finally:
        await client.close()

    create_body = next(
        body for method, path, body in calls if path == "/v1/messages/batches"
    )
    assert json.loads(create_body)["requests"][0]["custom_id"] == "row-1"
    assert future.processing_status == "future_provider_state"
    assert canceled.processing_status == "canceling"
    assert page.data[0].id == "msgbatch_new"
    assert any(path.endswith("/msgbatch_new/cancel") for _, path, _ in calls)
    assert decoded[0].result.type == "succeeded"
    assert decoded[0].custom_id == "row-1"
    assert created.created_at == datetime(2026, 7, 18, 7, tzinfo=UTC)
    assert tuple(int(part) for part in anthropic.__version__.split(".")[:2]) >= (0, 117)
