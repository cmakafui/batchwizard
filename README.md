# BatchWizard

BatchWizard is one durable CLI for OpenAI and Anthropic batch jobs. Submit
provider-native JSONL, close the terminal, and come back later: a local SQLite
manifest remembers what is running, what still needs to be downloaded, and what
needs your attention.

It deliberately does not translate prompts between APIs. OpenAI and Anthropic
have different request formats and capabilities; BatchWizard gives them one
operational lifecycle without pretending they are the same protocol.

## Quickstart

Install BatchWizard as an isolated Python 3.11+ tool:

```bash
uv tool install batchwizard
```

Provide one or both standard API-key environment variables:

```bash
export OPENAI_API_KEY="..."
export ANTHROPIC_API_KEY="..."
```

Submit native JSONL to either provider:

```bash
batchwizard submit openai.jsonl --endpoint /v1/responses
batchwizard submit --provider anthropic anthropic.jsonl
```

The submit process can exit once the providers accept the work. One later
command resumes every actionable job, grouped by provider:

```bash
batchwizard watch --output-directory ./results
batchwizard status --all
```

`watch` is intentionally provider-free. The manifest records which adapter owns
each batch, so a single invocation can resume a mixture of OpenAI and Anthropic
jobs. Missing credentials or a temporary download failure for one provider do
not erase work or prevent other provider groups from advancing.

## Input formats

BatchWizard accepts each provider's native JSONL rather than maintaining a lossy
common prompt schema.

| | OpenAI | Anthropic |
| --- | --- | --- |
| Select with | default, or `--provider openai` | `--provider anthropic` |
| JSONL row | `{"custom_id", "method", "url", "body"}` | `{"custom_id", "params"}` |
| Model location | `body.model` | `params.model` |
| Submission | file upload, then Batch creation | requests sent inline to Message Batches |
| Provider status | Batch `status` | Message Batch `processing_status` |
| Results | provider output and error files | unordered result stream split by outcome |
| Recovery after an uncertain submit | automatic intent matching through Batch metadata | manual attachment by batch ID |

### OpenAI

```jsonl
{"custom_id":"ticket-1","method":"POST","url":"/v1/responses","body":{"model":"gpt-5.4","input":"Classify this ticket as billing, technical, or other."}}
```

The endpoint passed to BatchWizard must match the `url` in every row:

```bash
batchwizard submit openai.jsonl --endpoint /v1/responses
```

`/v1/chat/completions` is the default. BatchWizard also accepts the Responses,
Embeddings, Completions, Moderations, Images, and Videos Batch API endpoints.

### Anthropic

```jsonl
{"custom_id":"ticket-1","params":{"model":"claude-opus-4-8","max_tokens":256,"messages":[{"role":"user","content":"Classify this ticket as billing, technical, or other."}]}}
```

```bash
batchwizard submit --provider anthropic anthropic.jsonl
```

BatchWizard validates the batch envelope, `custom_id` syntax and uniqueness,
batch size, `max_tokens`, and the non-streaming requirement before submission.
Anthropic remains responsible for validating its evolving Messages parameter
surface. See [Anthropic Message Batches](docs/anthropic.md) for result routing,
prompt caching, cancellation, and retention behavior.

## Durable jobs and recovery

The manifest separates remote execution from local artifact collection. A
provider can report a batch complete while its result files are still pending
locally; that row remains actionable until collection succeeds or the provider
confirms that the artifacts are no longer available.

Submission is durable too. BatchWizard writes an intent before making provider
requests. If a connection fails after the remote service may have accepted the
batch, it keeps that intent instead of blindly submitting a duplicate:

```bash
batchwizard reconcile
batchwizard reconcile INTENT
batchwizard reconcile INTENT --batch-id MSGBATCH_ID
```

OpenAI intents can be matched through Batch metadata. Anthropic does not expose
equivalent batch metadata, so its uncertain submissions are attached after the
matching batch is identified with `list-jobs`.

The lifecycle and SQLite migrations are documented in
[Job lifecycle](docs/job-lifecycle.md).

## Result files

Provider output stays JSONL and is written atomically:

- `<batch_id>_results.jsonl` contains successful rows.
- `<batch_id>_errors.jsonl` contains request-level failures when present.

Anthropic result rows can arrive in any order. BatchWizard preserves each raw row
and routes `succeeded` results to the results file and `errored`, `expired`, or
`canceled` results to the errors file.

## Credentials and configuration

Environment variables are the preferred way to supply credentials. Keys can
also be persisted for convenience:

```bash
batchwizard configure --provider openai --set-key "$OPENAI_API_KEY"
batchwizard configure --provider anthropic --set-key "$ANTHROPIC_API_KEY"
batchwizard configure --show --provider openai
```

Persisted keys are plaintext JSON. BatchWizard writes the configuration
atomically with owner-only file permissions, but it does not use the operating
system keychain in v0.5. Treat that file as a secret; keyring-backed storage is
deferred to v0.6.

## Commands

| Command | Purpose |
| --- | --- |
| `submit` | Submit files and return after provider acceptance. |
| `watch` | Resume actionable jobs and collect terminal artifacts. |
| `process` | Submit, watch, and collect in one invocation. |
| `status` | Inspect the local SQLite manifest. |
| `reconcile` | Recover a submission with an uncertain provider outcome. |
| `list-jobs` | List recent jobs directly from one provider. |
| `cancel` | Request cancellation for a tracked or provider-native batch ID. |
| `download` | Collect result artifacts for one batch. |
| `configure` | Store, inspect, or reset local settings. |

Use `batchwizard <command> --help` for the exact arguments and options.

## Development

```bash
uv sync --all-groups
uv run pytest
uv run ruff format --check .
uv run ruff check .
uv build
```

The supported runtime matrix is Python 3.11 through 3.14. The lockfile is
committed; update it intentionally with `uv lock --upgrade-package <package>`.

## License

[MIT](LICENSE)
