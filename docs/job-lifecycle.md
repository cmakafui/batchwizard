# Job lifecycle

BatchWizard treats a provider batch and the CLI process watching it as separate
things. A submitted job remains in the SQLite manifest until its remote lifecycle
and local artifacts can be inspected independently.

## State dimensions

`JobRecord` deliberately records four different concerns:

| Concern | Manifest fields | Meaning |
| --- | --- | --- |
| Remote lifecycle | `state`, `provider_status` | The normalized lifecycle and the provider's exact status string |
| Request outcomes | `completed_count`, `failed_count`, `cancelled_count`, `expired_count`, `total_count` | Per-request results; a terminal job can contain a mixture |
| Artifact collection | `collection_state`, `output_path`, `error_path` | Whether all currently available provider artifacts are durable locally |
| Local health | `last_local_error`, `poll_failures` | Connectivity, authentication, or filesystem problems that must not rewrite remote truth |

The normalized remote states are:

```text
pending -> running -> completed | failed | expired
                   -> cancelling -> cancelled
```

Providers may skip states. An unknown provider status is preserved in
`provider_status`; BatchWizard does not guess whether it is terminal.

Artifact collection progresses independently:

```text
not_ready -> pending -> collected
                   \-> failed -> pending (on the next retry)
                   \-> unavailable
```

`collected` means the collection attempt completed successfully. A provider may
legitimately have no output or error file for a terminal job.
`unavailable` means the provider has permanently removed or archived artifacts;
it is terminal and is not retried.

## Actionable jobs

A job is actionable when either:

1. Its remote state is `pending`, `running`, or `cancelling`; or
2. Its remote state is terminal and artifact collection is `pending` or `failed`.

`batchwizard watch` operates on actionable jobs. This is why a temporary download
failure survives process exit and is retried by a later invocation.

## Failure invariants

- A polling, authentication, rate-limit, SDK, or network error is a local
  observation failure. It never changes the remote job to `failed`.
- Retryable polling errors are attempted a bounded number of times in one watch
  invocation. The job remains actionable after the watcher pauses.
- A provider-reported terminal status is stored before artifact collection begins.
- Downloads are streamed to a temporary file in the destination directory and
  atomically renamed only after the stream completes.
- A failed collection records `collection_state=failed` and remains actionable.
- A provider-confirmed retention loss records `collection_state=unavailable` and
  stops retrying without pretending the artifacts were collected.
- Request-level errors do not change a successfully ended provider job into a
  provider failure; their counts and error artifact are recorded separately.
- A cancellation request is not equivalent to cancellation completion. The
  provider's immediate `cancelling` state remains active until confirmed terminal.

## Provider contract

A provider adapter owns submission mechanics and native statuses, but exposes a
small operational contract:

- Submit a provider-native input file.
- Return a normalized status snapshot while preserving the raw status.
- Fetch all available success and error artifacts idempotently.
- Request cancellation and return its immediate status.
- Return provider-neutral summaries for `list-jobs`.

Input and result payloads remain provider-native. BatchWizard normalizes lifecycle
and artifact locations, not model-specific request semantics.

OpenAI exposes job-level terminal statuses. Anthropic exposes the neutral terminal
status `ended` and independent `succeeded`, `errored`, `canceled`, and `expired`
request counts. BatchWizard maps `ended` to a terminal normalized lifecycle while
preserving those row outcomes; an all-errored Anthropic batch is not rewritten as
a remote provider failure.

## Schema migrations

The SQLite manifest uses `PRAGMA user_version`. Opening a v0.4 manifest migrates it
through schema version 1 and then to version 2:

- Active jobs start with artifact state `not_ready`.
- Terminal jobs with an existing output or error path become `collected`.
- Terminal jobs without a known local path become `pending` so collection is
  conservatively retried.
- Version 2 makes `(provider, batch_id)` the durable identity. Provider-native IDs
  no longer have to be globally unique across different providers.

A manifest with a newer schema version is rejected rather than silently modified
by an older BatchWizard release.

## Known boundary

Submission still crosses two systems: the provider must accept the batch before
its provider-assigned ID can be inserted into SQLite. A process failure in that
narrow interval can leave a remote batch without a local manifest row. Provider
metadata and reconciliation are the intended solution, but they are outside this
lifecycle PR; `list-jobs` remains the manual recovery surface.
