from __future__ import annotations

import json
import os
import tempfile
from collections import Counter
from pathlib import Path
from typing import cast

from anthropic import AnthropicError, AsyncAnthropic, NotFoundError
from anthropic.types.messages.batch_create_params import Request
from loguru import logger

from ..config import config
from ..models import (
    BatchStatus,
    DownloadedResults,
    JobState,
    ProviderJobSummary,
    SubmittedBatch,
)
from .base import ArtifactUnavailableError

MAX_BATCH_REQUESTS = 100_000
MAX_BATCH_BYTES = 256 * 1024 * 1024
ANTHROPIC_BATCH_ENDPOINT = "/v1/messages/batches"

_UNSUPPORTED_PARAMS = frozenset(
    {
        "speed",
        "store",
        "previous_thread_event_id",
        "cache_hint",
        "context_hint",
        "research_preview_2026_02",
    }
)

_STATE_MAP = {
    "in_progress": JobState.RUNNING,
    "canceling": JobState.CANCELLING,
    # Anthropic's neutral terminal state. Per-request outcomes remain separate.
    "ended": JobState.COMPLETED,
}


class AnthropicBatchProvider:
    name = "anthropic"

    def __init__(self, client: AsyncAnthropic | None = None):
        self.client = client or AsyncAnthropic(api_key=config.get_api_key("anthropic"))

    async def submit(
        self, input_file: Path, endpoint: str | None = None
    ) -> SubmittedBatch:
        if endpoint is not None:
            raise ValueError(
                "--endpoint is OpenAI-specific and cannot be used with Anthropic"
            )
        requests = _load_requests(input_file)
        batch = await self.client.messages.batches.create(
            requests=cast(list[Request], requests)
        )
        logger.info(f"Submitted {input_file.name} as Anthropic batch {batch.id}")
        return SubmittedBatch(
            batch_id=batch.id,
            provider_status=str(batch.processing_status).lower(),
            endpoint=ANTHROPIC_BATCH_ENDPOINT,
        )

    async def status(self, batch_id: str) -> BatchStatus:
        batch = await self.client.messages.batches.retrieve(batch_id)
        return _normalize_status(batch)

    async def fetch_results(self, batch_id: str, output_dir: Path) -> DownloadedResults:
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            decoder = await self.client.messages.batches.results(batch_id)
        except NotFoundError as error:
            if _is_results_artifact_request(error):
                raise ArtifactUnavailableError(
                    f"Anthropic results for {batch_id} are no longer available"
                ) from error
            raise
        except AnthropicError as error:
            batch = await self.client.messages.batches.retrieve(batch_id)
            if getattr(batch, "archived_at", None) is not None:
                raise ArtifactUnavailableError(
                    f"Anthropic archived the results for {batch_id} after retention"
                ) from error
            raise

        output_path = output_dir / f"{batch_id}_results.jsonl"
        error_path = output_dir / f"{batch_id}_errors.jsonl"
        output_temporary = _temporary_path(output_path)
        try:
            error_temporary = _temporary_path(error_path)
        except BaseException:
            output_temporary.unlink(missing_ok=True)
            raise
        error_types: Counter[str] = Counter()

        try:
            with (
                output_temporary.open("w", encoding="utf-8") as successes,
                error_temporary.open("w", encoding="utf-8") as failures,
            ):
                try:
                    async for item in decoder:
                        result_type = str(item.result.type)
                        line = json.dumps(
                            item.to_dict(),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        if result_type == "succeeded":
                            successes.write(f"{line}\n")
                        else:
                            failures.write(f"{line}\n")
                            error_types[_result_error_type(item)] += 1
                finally:
                    await decoder.close()
                for handle in (successes, failures):
                    handle.flush()
                    os.fsync(handle.fileno())
            os.replace(output_temporary, output_path)
            os.replace(error_temporary, error_path)
        except BaseException:
            output_temporary.unlink(missing_ok=True)
            error_temporary.unlink(missing_ok=True)
            raise

        logger.info(f"Collected Anthropic results for {batch_id} in {output_dir}")
        return DownloadedResults(
            output_path=output_path,
            error_path=error_path,
            error_summary=_format_error_types(error_types),
        )

    async def cancel(self, batch_id: str) -> BatchStatus:
        batch = await self.client.messages.batches.cancel(batch_id)
        return _normalize_status(batch)

    async def list_jobs(self, limit: int = 20) -> list[ProviderJobSummary]:
        page = await self.client.messages.batches.list(limit=limit)
        jobs = []
        for batch in page.data[:limit]:
            counts = batch.request_counts
            jobs.append(
                ProviderJobSummary(
                    batch_id=batch.id,
                    provider_status=batch.processing_status,
                    created_at=batch.created_at,
                    completed_count=counts.succeeded,
                    failed_count=counts.errored,
                    cancelled_count=counts.canceled,
                    expired_count=counts.expired,
                    total_count=_request_total(counts),
                )
            )
        return jobs

    async def close(self) -> None:
        await self.client.close()


def _load_requests(input_file: Path) -> list[dict]:
    if input_file.stat().st_size > MAX_BATCH_BYTES:
        raise ValueError(
            f"{input_file}: exceeds Anthropic's 256 MB batch request limit"
        )

    requests: list[dict] = []
    custom_ids: set[str] = set()
    try:
        lines = input_file.open(encoding="utf-8")
    except UnicodeError as error:
        raise ValueError(f"{input_file}: input must be UTF-8 JSONL") from error

    try:
        with lines:
            for line_number, line in enumerate(lines, start=1):
                if len(requests) >= MAX_BATCH_REQUESTS:
                    raise ValueError(
                        f"{input_file}: exceeds Anthropic's 100,000-request limit"
                    )
                requests.append(
                    _parse_request_line(line, line_number, custom_ids, input_file)
                )
    except UnicodeError as error:
        raise ValueError(f"{input_file}: input must be UTF-8 JSONL") from error

    if not requests:
        raise ValueError(f"{input_file}: batch input must contain at least one request")

    serialized_size = len(
        json.dumps(
            {"requests": requests}, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
    )
    if serialized_size > MAX_BATCH_BYTES:
        raise ValueError(f"{input_file}: serialized Anthropic request exceeds 256 MB")
    return requests


def _parse_request_line(
    line: str, line_number: int, custom_ids: set[str], input_file: Path
) -> dict:
    prefix = f"{input_file}:{line_number}"
    if not line.strip():
        raise ValueError(f"{prefix}: blank lines are not valid JSONL requests")
    try:
        request = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(f"{prefix}: invalid JSON: {error.msg}") from error
    if not isinstance(request, dict):
        raise ValueError(f"{prefix}: each request must be a JSON object")

    custom_id = request.get("custom_id")
    if not isinstance(custom_id, str) or not 1 <= len(custom_id) <= 64:
        raise ValueError(f"{prefix}: custom_id must be a 1-64 character string")
    if custom_id in custom_ids:
        raise ValueError(f"{prefix}: duplicate custom_id {custom_id!r}")
    custom_ids.add(custom_id)

    params = request.get("params")
    if not isinstance(params, dict):
        raise ValueError(f"{prefix}: params must be a JSON object")
    if not isinstance(params.get("model"), str) or not params["model"]:
        raise ValueError(f"{prefix}: params.model must be a nonempty string")
    if not isinstance(params.get("messages"), list) or not params["messages"]:
        raise ValueError(f"{prefix}: params.messages must be a nonempty array")
    max_tokens = params.get("max_tokens")
    if (
        isinstance(max_tokens, bool)
        or not isinstance(max_tokens, int)
        or max_tokens < 1
    ):
        raise ValueError(f"{prefix}: params.max_tokens must be an integer >= 1")
    if params.get("stream") is True:
        raise ValueError(f"{prefix}: params.stream=true is not supported in batches")
    unsupported = sorted(_UNSUPPORTED_PARAMS.intersection(params))
    if unsupported:
        raise ValueError(
            f"{prefix}: unsupported Anthropic batch parameter(s): "
            f"{', '.join(unsupported)}"
        )
    return request


def _normalize_status(batch) -> BatchStatus:
    provider_status = str(batch.processing_status).lower()
    counts = batch.request_counts
    return BatchStatus(
        provider_status=provider_status,
        state=_STATE_MAP.get(provider_status),
        completed_count=counts.succeeded,
        failed_count=counts.errored,
        cancelled_count=counts.canceled,
        expired_count=counts.expired,
        total_count=_request_total(counts),
        error_summary=_format_request_counts(counts),
    )


def _request_total(counts) -> int:
    return sum(
        (
            counts.processing,
            counts.succeeded,
            counts.errored,
            counts.canceled,
            counts.expired,
        )
    )


def _format_request_counts(counts) -> str | None:
    parts = []
    for value, label in (
        (counts.errored, "errored"),
        (counts.expired, "expired"),
        (counts.canceled, "canceled"),
    ):
        if value:
            parts.append(f"{value} {label}")
    return ", ".join(parts) or None


def _result_error_type(item) -> str:
    result_type = str(item.result.type)
    if result_type != "errored":
        return result_type
    error = getattr(item.result, "error", None)
    detail = getattr(error, "error", None)
    return str(getattr(detail, "type", None) or "errored")


def _format_error_types(error_types: Counter[str]) -> str | None:
    if not error_types:
        return None
    return ", ".join(
        f"{count} {error_type}" for error_type, count in error_types.most_common(3)
    )


def _is_results_artifact_request(error: NotFoundError) -> bool:
    """Distinguish an expired result artifact from an unknown batch ID."""
    return error.response.request.url.path.rstrip("/").endswith("/results")


def _temporary_path(destination: Path) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".part",
    )
    os.close(descriptor)
    return Path(temporary_name)
