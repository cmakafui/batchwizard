# Anthropic Message Batches

BatchWizard uses the GA async Message Batches surface in the official Anthropic
Python SDK. Inputs and saved result rows stay provider-native; only lifecycle,
request counts, and local artifact locations are normalized.

## Input

Each nonempty UTF-8 JSONL line must contain a unique `custom_id` and a Messages
API `params` object:

```jsonl
{"custom_id":"ticket-42","params":{"model":"claude-opus-4-8","max_tokens":256,"messages":[{"role":"user","content":"Classify this ticket."}]}}
```

```bash
batchwizard submit --provider anthropic input.jsonl
```

BatchWizard validates the batch envelope before making an HTTP request:

- 1–100,000 requests and a serialized request size no greater than 256 MB
- unique 1–64 character `custom_id` values
- object-valued `params` with `model`, `messages`, and `max_tokens >= 1`
- parameters that the current Message Batches API explicitly does not support

The complete Messages parameter surface evolves independently. Anthropic
validates those parameters asynchronously and returns invalid rows as
`result.type == "errored"`; BatchWizard does not claim to reproduce that server
validator.

## Lifecycle and outcomes

Anthropic exposes three batch lifecycle statuses:

| Anthropic | BatchWizard |
| --- | --- |
| `in_progress` | `running` |
| `canceling` | `cancelling` |
| `ended` | terminal `completed` |

`ended` is neutral: the batch may contain any mixture of `succeeded`, `errored`,
`canceled`, and `expired` requests. Those counters are recorded independently, so
an all-errored batch is not mislabeled as a provider-level failure.

Cancellation first returns `canceling`. The eventual batch status is `ended`, and
partial successful results may still exist.

## Results and retention

The SDK streams an unordered JSONL result sequence. BatchWizard preserves every
native object and routes by row result type:

- `succeeded` → `<batch_id>_results.jsonl`
- `errored`, `expired`, or `canceled` → `<batch_id>_errors.jsonl`

Both destination files are built as temporary files and exposed only after the
entire decoder finishes. A network or disk interruption leaves previous final
files untouched and keeps collection actionable. The decoder is explicitly
closed on both success and failure.

Anthropic retains results for 29 days from batch creation. Once the provider
reports them archived, collection becomes `unavailable` rather than retrying a
permanent loss forever.

See the current [Message Batches guide](https://platform.claude.com/docs/en/build-with-claude/batch-processing),
[Python SDK documentation](https://platform.claude.com/docs/en/cli-sdks-libraries/sdks/python),
and [Batch API reference](https://platform.claude.com/docs/en/api/python/messages/batches).
