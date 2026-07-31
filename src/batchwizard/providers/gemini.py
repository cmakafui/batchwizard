# providers/gemini.py
from __future__ import annotations

import json
import os
import tempfile
from collections import Counter
from pathlib import Path

from google import genai
from google.genai import errors as genai_errors
from google.genai import types as genai_types
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

# Gemini's file-based Batch API accepts input files up to 2 GB.
MAX_BATCH_BYTES = 2 * 1024 * 1024 * 1024

GEMINI_BATCH_PREFIX = "batches/"

# Gemini exposes no batch metadata, but display_name round-trips through
# create and list, so it carries the submission intent for reconciliation.
_INTENT_DISPLAY_PREFIX = "batchwizard-"

# The wire currently mixes two spellings (JOB_STATE_* and BATCH_STATE_*) for
# the same lifecycle, so states are mapped by suffix. JOB_STATE_PAUSED,
# JOB_STATE_UPDATING, and JOB_STATE_UNSPECIFIED are deliberately unmapped:
# they are neither progressing nor terminal, so the job stays actionable with
# its raw status preserved rather than being guessed.
_STATE_MAP = {
    "QUEUED": JobState.PENDING,
    "PENDING": JobState.PENDING,
    "RUNNING": JobState.RUNNING,
    "CANCELLING": JobState.CANCELLING,
    "SUCCEEDED": JobState.COMPLETED,
    # Neutral terminal like Anthropic's `ended`: per-request outcomes stay
    # separate, and a partially failed batch is not a remote provider failure.
    "PARTIALLY_SUCCEEDED": JobState.COMPLETED,
    "FAILED": JobState.FAILED,
    "EXPIRED": JobState.EXPIRED,
    "CANCELLED": JobState.CANCELLED,
}


class GeminiBatchProvider:
    name = "gemini"

    def __init__(self, client: genai.Client | None = None):
        self.client = client or genai.Client(api_key=config.get_api_key("gemini"))

    async def submit(
        self,
        input_file: Path,
        endpoint: str | None = None,
        intent_id: str | None = None,
        model: str | None = None,
    ) -> SubmittedBatch:
        if endpoint is not None:
            raise ValueError(
                "--endpoint is OpenAI-specific and cannot be used with Gemini"
            )
        if not model:
            raise ValueError(
                "Gemini batches take the model as a batch-level argument; "
                "pass --model (e.g. --model gemini-3.6-flash)"
            )
        _validate_input(input_file)

        uploaded = await self.client.aio.files.upload(
            file=input_file,
            config=genai_types.UploadFileConfig(
                display_name=input_file.name, mime_type="jsonl"
            ),
        )
        display_name = (
            f"{_INTENT_DISPLAY_PREFIX}{intent_id}" if intent_id else input_file.name
        )
        job = await self.client.aio.batches.create(
            model=model,
            src=uploaded.name,
            config=genai_types.CreateBatchJobConfig(display_name=display_name),
        )
        logger.info(f"Submitted {input_file.name} as Gemini batch {job.name}")
        return SubmittedBatch(
            batch_id=job.name,
            provider_status=_state_value(job.state),
            endpoint=None,
        )

    async def status(self, batch_id: str) -> BatchStatus:
        return _normalize_status(await self._raw_batch(batch_id))

    async def _raw_batch(self, batch_id: str) -> dict:
        """Fetch the raw batch Operation instead of the typed BatchJob.

        The typed Developer-API mapper drops `metadata.batchStats` and the
        Operation-level `error`, so the per-request counts BatchWizard tracks
        exist only on the wire. tests/test_gemini_sdk_contract.py pins both
        this gap and the private request surface used to bridge it.
        """
        response = await self.client._api_client.async_request(
            http_method="GET", path=batch_id, request_dict={}
        )
        body = response.body
        if isinstance(body, (str, bytes)):
            return json.loads(body) if body else {}
        return body or {}

    async def fetch_results(self, batch_id: str, output_dir: Path) -> DownloadedResults:
        job = await self.client.aio.batches.get(name=batch_id)
        output_dir.mkdir(parents=True, exist_ok=True)
        rows = await self._result_rows(batch_id, job.dest)
        if rows is None:
            # A failed, expired, or cancelled batch may legitimately have no
            # output destination at all.
            return DownloadedResults()

        slug = batch_id.rsplit("/", 1)[-1]
        output_path = output_dir / f"{slug}_results.jsonl"
        error_path = output_dir / f"{slug}_errors.jsonl"
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
                for row in rows:
                    line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                    if _is_error_row(row):
                        failures.write(f"{line}\n")
                        error_types[_row_error_type(row)] += 1
                    else:
                        successes.write(f"{line}\n")
                for handle in (successes, failures):
                    handle.flush()
                    os.fsync(handle.fileno())
            os.replace(output_temporary, output_path)
            os.replace(error_temporary, error_path)
        except BaseException:
            output_temporary.unlink(missing_ok=True)
            error_temporary.unlink(missing_ok=True)
            raise

        logger.info(f"Collected Gemini results for {batch_id} in {output_dir}")
        return DownloadedResults(
            output_path=output_path,
            error_path=error_path,
            error_summary=_format_error_types(error_types),
        )

    async def _result_rows(
        self, batch_id: str, dest: genai_types.BatchJobDestination | None
    ) -> list[dict] | None:
        """Return native result rows from either destination shape.

        Inline batches embed results in the job object; file batches point at
        a results file. The SDK's download returns the whole payload as bytes
        (no streaming handle), so atomicity is preserved at the write side:
        final files are exposed only after a complete payload.
        """
        if dest is None:
            return None
        if dest.inlined_responses is not None:
            return [
                item.model_dump(mode="json", exclude_none=True, by_alias=True)
                for item in dest.inlined_responses
            ]
        if not dest.file_name:
            return None
        try:
            blob = await self.client.aio.files.download(file=dest.file_name)
        except genai_errors.ClientError as error:
            if getattr(error, "code", None) == 404:
                # dest.file_name came from the provider itself, so a missing
                # file is retention loss (results are kept for six weeks),
                # not a bad reference.
                raise ArtifactUnavailableError(
                    f"Gemini results for {batch_id} are no longer available"
                ) from error
            raise
        if isinstance(blob, bytes):
            blob = blob.decode("utf-8")
        return [json.loads(line) for line in blob.splitlines() if line.strip()]

    async def cancel(self, batch_id: str) -> BatchStatus:
        # The SDK's cancel returns None, so re-fetch for the immediate status.
        await self.client.aio.batches.cancel(name=batch_id)
        return await self.status(batch_id)

    async def list_jobs(self, limit: int = 20) -> list[ProviderJobSummary]:
        if limit < 1:
            return []
        pager = await self.client.aio.batches.list(
            config=genai_types.ListBatchJobsConfig(page_size=min(limit, 100))
        )
        jobs: list[ProviderJobSummary] = []
        async for job in pager:
            display_name = job.display_name or ""
            jobs.append(
                ProviderJobSummary(
                    batch_id=job.name or "",
                    provider_status=_state_value(job.state),
                    endpoint=job.model,
                    intent_id=(
                        display_name.removeprefix(_INTENT_DISPLAY_PREFIX)
                        if display_name.startswith(_INTENT_DISPLAY_PREFIX)
                        else None
                    ),
                    created_at=job.create_time,
                )
            )
            if len(jobs) >= limit:
                break
        return jobs

    async def close(self) -> None:
        await self.client.aio.aclose()


def _state_value(state: genai_types.JobState | None) -> str:
    """Read the wire value; str() on the enum yields 'JobState.X', not 'X'."""
    return getattr(state, "value", None) or ""


def _normalize_state(provider_status: str) -> JobState | None:
    for prefix in ("JOB_STATE_", "BATCH_STATE_"):
        if provider_status.startswith(prefix):
            return _STATE_MAP.get(provider_status.removeprefix(prefix))
    return None


def _normalize_status(body: dict) -> BatchStatus:
    metadata = body.get("metadata") or {}
    provider_status = str(metadata.get("state") or "")
    stats = metadata.get("batchStats") or {}

    def count(name: str) -> int:
        try:
            return int(stats.get(name) or 0)
        except (TypeError, ValueError):
            return 0

    failed = count("failedRequestCount")
    error = body.get("error") or {}
    if error.get("message"):
        error_summary = str(error["message"])
    elif failed:
        error_summary = f"{failed} failed"
    else:
        error_summary = None
    return BatchStatus(
        provider_status=provider_status,
        state=_normalize_state(provider_status),
        completed_count=count("successfulRequestCount"),
        failed_count=failed,
        total_count=count("requestCount"),
        error_summary=error_summary,
    )


def _is_error_row(row: dict) -> bool:
    # A result row carries either a response or an error/status object;
    # anything without a response is preserved on the error side.
    return "error" in row or "response" not in row


def _row_error_type(row: dict) -> str:
    error = row.get("error") or {}
    return str(error.get("status") or error.get("code") or "error")


def _format_error_types(error_types: Counter[str]) -> str | None:
    if not error_types:
        return None
    return ", ".join(
        f"{count} {error_type}" for error_type, count in error_types.most_common(3)
    )


def _validate_input(input_file: Path) -> None:
    if input_file.stat().st_size > MAX_BATCH_BYTES:
        raise ValueError(f"{input_file}: exceeds Gemini's 2 GB batch input limit")

    keys: set[str] = set()
    count = 0
    try:
        with input_file.open(encoding="utf-8") as lines:
            for line_number, line in enumerate(lines, start=1):
                _parse_request_line(line, line_number, keys, input_file)
                count += 1
    except UnicodeError as error:
        raise ValueError(f"{input_file}: input must be UTF-8 JSONL") from error

    if not count:
        raise ValueError(f"{input_file}: batch input must contain at least one request")


def _parse_request_line(
    line: str, line_number: int, keys: set[str], input_file: Path
) -> None:
    prefix = f"{input_file}:{line_number}"
    if not line.strip():
        raise ValueError(f"{prefix}: blank lines are not valid JSONL requests")
    try:
        row = json.loads(line)
    except json.JSONDecodeError as error:
        raise ValueError(f"{prefix}: invalid JSON: {error.msg}") from error
    if not isinstance(row, dict):
        raise ValueError(f"{prefix}: each request must be a JSON object")

    key = row.get("key")
    if not isinstance(key, str) or not key:
        raise ValueError(f"{prefix}: key must be a nonempty string")
    if key in keys:
        raise ValueError(f"{prefix}: duplicate key {key!r}")
    keys.add(key)

    request = row.get("request")
    if not isinstance(request, dict):
        raise ValueError(f"{prefix}: request must be a JSON object")
    if not isinstance(request.get("contents"), list) or not request["contents"]:
        raise ValueError(f"{prefix}: request.contents must be a nonempty array")
    # The GenerateContentRequest surface evolves independently; Gemini
    # validates the rest asynchronously and reports per-row errors.


def _temporary_path(destination: Path) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.name}.",
        suffix=".part",
    )
    os.close(descriptor)
    return Path(temporary_name)
