"""Executable assumptions at the boundary with the current google-genai SDK.

The Gemini adapter leans on behaviors the SDK does not document: the
Operation envelope mapping, the typed BatchJob dropping batchStats, the
private raw-request escape hatch, and enum pseudo-members for unknown
states. Each is pinned here so an SDK upgrade that changes them fails
loudly instead of corrupting the manifest.
"""

from __future__ import annotations

import json

import google.genai as genai_package
import httpx
from google import genai
from google.genai import types


def _operation(name: str, state: str, *, output: dict | None = None) -> dict:
    metadata = {
        "@type": (
            "type.googleapis.com/google.ai.generativelanguage.v1beta"
            ".GenerateContentBatch"
        ),
        "name": name,
        "displayName": "batchwizard-intent1",
        "model": "models/gemini-3.6-flash",
        "state": state,
        "createTime": "2026-07-30T09:00:00Z",
        "updateTime": "2026-07-30T09:30:00Z",
        "batchStats": {
            "requestCount": "3",
            "successfulRequestCount": "2",
            "failedRequestCount": "1",
        },
    }
    if output is not None:
        metadata["output"] = output
    return {"name": name, "metadata": metadata, "done": output is not None}


async def test_real_sdk_batch_contract():
    calls: list[tuple[str, str, bytes]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        path = request.url.path
        calls.append((request.method, path, body))
        if request.headers.get("x-goog-upload-command") == "start":
            return httpx.Response(
                200,
                headers={
                    "x-goog-upload-url": (
                        "https://generativelanguage.fake/upload/v1beta/files?s=1"
                    ),
                    "x-goog-upload-status": "active",
                },
            )
        if "upload" in (request.headers.get("x-goog-upload-command") or ""):
            return httpx.Response(
                200,
                headers={"x-goog-upload-status": "final"},
                json={
                    "file": {
                        "name": "files/in-1",
                        "mimeType": "application/jsonl",
                        "sizeBytes": str(len(body)),
                        "state": "ACTIVE",
                    }
                },
            )
        if request.method == "POST" and path.endswith(":batchGenerateContent"):
            return httpx.Response(
                200, json=_operation("batches/new", "JOB_STATE_PENDING")
            )
        if request.method == "POST" and path.endswith(":cancel"):
            return httpx.Response(200, json={})
        if path.endswith("/batches/new"):
            return httpx.Response(
                200,
                json=_operation(
                    "batches/new",
                    "JOB_STATE_SUCCEEDED",
                    output={"responsesFile": "files/out-1"},
                ),
            )
        if path.endswith("/batches/legacy"):
            return httpx.Response(
                200, json=_operation("batches/legacy", "BATCH_STATE_SUCCEEDED")
            )
        if path.endswith("/batches/future"):
            return httpx.Response(
                200, json=_operation("batches/future", "JOB_STATE_NEW_IN_2027")
            )
        if path.endswith("/batches/inline"):
            return httpx.Response(
                200,
                json=_operation(
                    "batches/inline",
                    "JOB_STATE_SUCCEEDED",
                    output={
                        "inlinedResponses": {
                            "inlinedResponses": [
                                {
                                    "metadata": {"key": "row-1"},
                                    "response": {
                                        "candidates": [
                                            {
                                                "content": {
                                                    "parts": [{"text": "ok"}],
                                                    "role": "model",
                                                }
                                            }
                                        ],
                                        "usageMetadata": {"promptTokenCount": 4},
                                    },
                                },
                                {
                                    "metadata": {"key": "row-2"},
                                    "error": {"code": 400, "message": "bad"},
                                },
                            ]
                        }
                    },
                ),
            )
        if ":download" in path:
            return httpx.Response(
                200,
                content=b'{"key":"row-1","response":{}}\n',
                headers={"content-type": "application/jsonl"},
            )
        return httpx.Response(
            404, json={"error": {"code": 404, "message": f"unexpected {path}"}}
        )

    client = genai.Client(
        api_key="test-key",
        http_options=types.HttpOptions(
            base_url="https://generativelanguage.fake",
            async_client_args={"transport": httpx.MockTransport(handle)},
        ),
    )
    try:
        # Submission: upload is a two-step resumable protocol, and creation
        # carries the display name that BatchWizard uses as its intent.
        uploaded = await client.aio.files.upload(
            file=__file__,
            config=types.UploadFileConfig(
                display_name="input.jsonl", mime_type="jsonl"
            ),
        )
        created = await client.aio.batches.create(
            model="gemini-3.6-flash",
            src=uploaded.name,
            config=types.CreateBatchJobConfig(display_name="batchwizard-intent1"),
        )
        succeeded = await client.aio.batches.get(name="batches/new")
        legacy = await client.aio.batches.get(name="batches/legacy")
        future = await client.aio.batches.get(name="batches/future")
        inline = await client.aio.batches.get(name="batches/inline")
        cancel_result = await client.aio.batches.cancel(name="batches/new")
        downloaded = await client.aio.files.download(file="files/out-1")
        raw = await client._api_client.async_request(
            http_method="GET", path="batches/new", request_dict={}
        )
    finally:
        await client.aio.aclose()

    create_body = json.loads(
        next(
            body
            for method, path, body in calls
            if path.endswith(":batchGenerateContent")
        )
    )
    assert create_body["batch"]["displayName"] == "batchwizard-intent1"
    assert create_body["batch"]["inputConfig"]["fileName"] == "files/in-1"
    assert created.name == "batches/new"

    # The Operation envelope maps metadata.output -> dest, and results live
    # either in a file reference or inline in the job object.
    assert succeeded.dest.file_name == "files/out-1"
    assert inline.dest.file_name is None
    assert inline.dest.inlined_responses[0].response is not None
    assert inline.dest.inlined_responses[1].error.code == 400

    # The typed BatchJob drops batchStats on the Developer API; the counts
    # exist only on the raw wire. The adapter's status() depends on both
    # halves of this: the gap and the private escape hatch that bridges it.
    assert succeeded.completion_stats is None
    raw_body = json.loads(raw.body)
    assert raw_body["metadata"]["batchStats"]["successfulRequestCount"] == "2"

    # The wire is mid-rename: BATCH_STATE_* is translated to JOB_STATE_*.
    assert legacy.state == types.JobState.JOB_STATE_SUCCEEDED

    # An unknown state becomes an enum pseudo-member instead of crashing,
    # and str() on it is NOT the wire value -- adapters must read .value.
    assert isinstance(future.state, types.JobState)
    assert future.state.value == "JOB_STATE_NEW_IN_2027"
    assert str(future.state) != future.state.value

    # cancel returns None (the adapter must re-fetch for the status), and
    # download returns a fully buffered payload, not a streaming handle.
    assert cancel_result is None
    assert any(path.endswith(":cancel") for _, path, _ in calls)
    assert isinstance(downloaded, bytes)

    # Inline responses can be re-serialized wire-native for JSONL storage.
    dumped = inline.dest.inlined_responses[0].model_dump(
        mode="json", exclude_none=True, by_alias=True
    )
    assert "usageMetadata" in dumped["response"]

    version = tuple(int(part) for part in genai_package.__version__.split(".")[:2])
    assert version >= (2, 16)
