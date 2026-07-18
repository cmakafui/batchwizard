# BatchWizard

BatchWizard runs OpenAI and Anthropic batch jobs without requiring a terminal to
stay open. It records submissions in SQLite, reconnects to active jobs, and
collects provider-native result files when they finish.

BatchWizard does not translate request bodies between providers. OpenAI inputs
remain OpenAI Batch JSONL; Anthropic inputs remain Message Batches JSONL.

## Install

BatchWizard requires Python 3.11 or newer. Install it as an isolated CLI with
[`uv`](https://docs.astral.sh/uv/):

```bash
uv tool install batchwizard
batchwizard --help
```

Upgrade an existing installation with:

```bash
uv tool upgrade batchwizard
```

## Configure credentials

BatchWizard reads the standard provider environment variables:

```bash
export OPENAI_API_KEY="..."
export ANTHROPIC_API_KEY="..."
```

Keys can instead be stored in BatchWizard's local configuration file:

```bash
batchwizard configure --provider openai --set-key "$OPENAI_API_KEY"
batchwizard configure --provider anthropic --set-key "$ANTHROPIC_API_KEY"
```

The configuration file is replaced atomically and written with owner-only
permissions.

## Submit work

### OpenAI

OpenAI uses its native Batch API JSONL envelope:

```jsonl
{"custom_id":"ticket-1","method":"POST","url":"/v1/responses","body":{"model":"gpt-5.4","input":"Classify this ticket as billing, technical, or other."}}
```

The endpoint passed to BatchWizard must match the `url` in every input row:

```bash
batchwizard submit openai.jsonl --endpoint /v1/responses
```

`/v1/chat/completions` remains the default when `--endpoint` is omitted. Other
supported endpoints include `/v1/embeddings`, `/v1/completions`,
`/v1/moderations`, `/v1/images/generations`, `/v1/images/edits`, and
`/v1/videos`.

### Anthropic

Anthropic uses the native Message Batches format:

```jsonl
{"custom_id":"ticket-1","params":{"model":"claude-opus-4-8","max_tokens":256,"messages":[{"role":"user","content":"Classify this ticket as billing, technical, or other."}]}}
```

```bash
batchwizard submit --provider anthropic anthropic.jsonl
```

BatchWizard validates the batch envelope, `custom_id` syntax and uniqueness,
batch size, `max_tokens`, and the non-streaming requirement. Anthropic remains
responsible for validating the evolving Messages parameter surface.

See [Anthropic Message Batches](docs/anthropic.md) for lifecycle, cancellation,
result routing, and retention details.

## Watch and collect

`submit` returns after the provider accepts the jobs. A later process can resume
all outstanding work:

```bash
batchwizard watch --output-directory ./results
```

`watch` groups jobs by provider and polls those groups concurrently. A missing
credential for one provider does not block the other provider's jobs.

Submission intents are written to the local manifest before network I/O. If a
connection fails after the provider may have accepted a batch, BatchWizard keeps
the intent instead of risking a duplicate submission. Inspect and recover those
rows with `batchwizard reconcile`; OpenAI intents are matched through batch
metadata, while Anthropic batches can be attached with `--batch-id` after they
are identified with `list-jobs`.

Remote execution and local artifact collection are tracked separately. A
completed provider job remains actionable until its files are collected. Failed
downloads are retried by the next `watch`; artifacts removed after a provider's
retention window are marked `unavailable` and are not retried forever.

Inspect the local manifest at any time:

```bash
batchwizard status
batchwizard status --all
```

By default, `status` shows active jobs and terminal jobs that still need artifact
collection. `--all` also includes finished jobs.

## Results

Provider output is preserved as JSONL. Anthropic's streamed, unordered rows are
split without changing their contents:

- `<batch_id>_results.jsonl` contains `succeeded` rows.
- `<batch_id>_errors.jsonl` contains `errored`, `expired`, and `canceled` rows.

Files are written through temporary files and renamed only after the result
stream completes, so an interrupted download does not expose a partial final
file.

## Commands

| Command | Purpose |
| --- | --- |
| `submit` | Submit files and return immediately. |
| `watch` | Resume actionable jobs and collect terminal artifacts. |
| `process` | Submit, wait, and collect in one invocation. |
| `status` | Inspect jobs in the local SQLite manifest. |
| `reconcile` | Recover a submission whose provider outcome was uncertain. |
| `list-jobs` | List recent jobs directly from one provider. |
| `cancel` | Request cancellation; provider inference works for tracked jobs. |
| `download` | Collect one batch's result artifacts. |
| `configure` | Store, inspect, or reset local configuration. |

Use `batchwizard <command> --help` for command-specific options.

## Development

Create the project environment and run commands through `uv`:

```bash
uv sync --all-groups
uv run batchwizard --help
uv run pytest
uv run ruff format --check .
uv run ruff check .
uv build
```

The supported runtime matrix is Python 3.11 through 3.14. The dependency lockfile
is committed; update it intentionally with `uv lock --upgrade-package <package>`.

The provider contract, recovery invariants, and manifest migrations are described
in [Job lifecycle](docs/job-lifecycle.md).

## License

[MIT](LICENSE)
