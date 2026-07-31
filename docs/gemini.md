# Gemini Batch API

BatchWizard uses the file-based Batch API surface of the official `google-genai`
Python SDK against the Gemini Developer API. Inputs and saved result rows stay
provider-native; only lifecycle, request counts, and local artifact locations
are normalized.

## Input

Unlike OpenAI and Anthropic, Gemini takes the model as a batch-level submission
argument rather than reading it from each row, so a Gemini submission requires
`--model`. Each nonempty UTF-8 JSONL line must contain a unique `key` and a
`GenerateContentRequest` under `request`:

```jsonl
{"key":"ticket-42","request":{"contents":[{"parts":[{"text":"Classify this ticket."}],"role":"user"}]}}
```

```bash
batchwizard submit --provider gemini --model gemini-3.6-flash input.jsonl
```

BatchWizard validates before making an HTTP request:

- an input file no greater than Gemini's 2 GB limit
- nonempty, unique string `key` values
- an object-valued `request` with a nonempty `contents` array

The `GenerateContentRequest` surface (generation config, tools, structured
output, cached content) evolves independently. Gemini validates those
parameters asynchronously and reports invalid rows as per-row errors in the
result file; BatchWizard does not claim to reproduce that server validator.

Submission uploads the file through the Files API, then creates the batch
against the uploaded file. Inline (request-embedded) submission exists in the
API but is capped at 20 MB and offers nothing over a file for a CLI whose
inputs are already files, so BatchWizard always submits file-based batches.

## Lifecycle and outcomes

The SDK's `JobState` enum carries twelve states; only some occur on the
Developer API, and the wire is mid-rename between `BATCH_STATE_*` and
`JOB_STATE_*` spellings. BatchWizard maps both spellings by suffix:

| Gemini | BatchWizard |
| --- | --- |
| `JOB_STATE_QUEUED`, `JOB_STATE_PENDING` | `pending` |
| `JOB_STATE_RUNNING` | `running` |
| `JOB_STATE_CANCELLING` | `cancelling` |
| `JOB_STATE_SUCCEEDED`, `JOB_STATE_PARTIALLY_SUCCEEDED` | terminal `completed` |
| `JOB_STATE_FAILED` | terminal `failed` |
| `JOB_STATE_EXPIRED` | terminal `expired` |
| `JOB_STATE_CANCELLED` | terminal `cancelled` |

`JOB_STATE_PAUSED`, `JOB_STATE_UPDATING`, and any future state are deliberately
unmapped: they are neither progressing nor terminal, so the job stays
actionable with its raw status preserved rather than being guessed.

`PARTIALLY_SUCCEEDED` is a neutral terminal state like Anthropic's `ended`: the
batch can contain a mixture of succeeded and failed rows, whose counts are
recorded independently, so a partially failed batch is not rewritten as a
remote provider failure. A batch that has been running or pending for more
than 48 hours expires.

The typed `BatchJob` returned by the SDK drops the per-request counters
(`batchStats`) and the Operation-level error on the Developer API, so
BatchWizard reads the raw batch Operation over the SDK's request layer for
status checks. That private surface, and the typed gap that forces it, are
pinned in `tests/test_gemini_sdk_contract.py`.

Cancellation in the SDK returns nothing, so BatchWizard requests cancellation
and then re-fetches the immediate status. Partial results may still exist for
a cancelled batch.

## Results and retention

A terminal batch exposes results in one of two shapes: a results file
(file-based submissions) or responses embedded in the job object (inline
submissions created outside BatchWizard). Both are handled; every native row
is preserved and routed by outcome:

- rows with a `response` → `<batch_id>_results.jsonl`
- rows with an `error` or no response → `<batch_id>_errors.jsonl`

Local filenames use the bare batch ID (`batches/abc123` → `abc123_*.jsonl`).
The SDK's file download returns the entire payload as bytes rather than a
stream, so collection buffers the payload in memory; destination files are
still built as temporary files and exposed only after the complete payload is
written. An interruption leaves previous final files untouched and keeps
collection actionable.

Gemini retains batch results for six weeks. A 404 on the result file the
provider itself referenced records `collection_state=unavailable` rather than
retrying a permanent loss forever; any other download failure stays retryable.

## Submission recovery

Batch creation is not idempotent and Gemini exposes no batch metadata, but the
batch display name round-trips through creation and listing, so BatchWizard
submits each batch with its intent ID as `batchwizard-INTENT`. After a crash
or uncertain connection failure, `batchwizard reconcile INTENT` finds the
matching batch automatically, the same recovery path as OpenAI.

## Credentials

The API key is read from `GEMINI_API_KEY`, falling back to `GOOGLE_API_KEY`,
or from `batchwizard configure --provider gemini --set-key`.

See the current [Batch API guide](https://ai.google.dev/gemini-api/docs/batch-api)
and [python-genai SDK](https://github.com/googleapis/python-genai).
