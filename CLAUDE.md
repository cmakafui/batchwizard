# CLAUDE.md

BatchWizard is a CLI that gives OpenAI and Anthropic batch jobs one durable
operational lifecycle. It does not translate prompts between providers; input
and result JSONL stay provider-native.

## Layout

- `src/batchwizard/cli.py` - Typer entrypoint and commands.
- `src/batchwizard/processor.py` - submit/watch/collect orchestration.
- `src/batchwizard/providers/` - `base.py` defines the provider contract;
  `openai.py` and `anthropic.py` implement it.
- `src/batchwizard/store.py` - SQLite manifest (schema, migrations, queries).
- `src/batchwizard/models.py` - Pydantic models, including `JobRecord`.
- `src/batchwizard/config.py` - credential storage and settings.
- `src/batchwizard/ui.py` - Rich output formatting.
- `docs/job-lifecycle.md` - state machine and manifest schema.
- `docs/anthropic.md` - Anthropic Message Batches specifics.

## Commands

```bash
uv sync --all-groups
uv run pytest
uv run ruff format --check .
uv run ruff check .
```

Python 3.11-3.14. The lockfile is committed; update it deliberately with
`uv lock --upgrade-package <package>`, not as a side effect of other changes.

## Working in this codebase

- Read `docs/job-lifecycle.md` before touching `store.py` or `models.py`. The
  remote lifecycle, request outcomes, and artifact collection are tracked as
  separate state dimensions on purpose; don't collapse them.
- A new provider adapter must implement the contract in `providers/base.py`
  and keep results provider-native rather than mapping into a shared schema.
- Any change to the SQLite schema needs a new `PRAGMA user_version` migration
  in `store.py`, documented in `docs/job-lifecycle.md`.
- Match the existing docs style: plain technical prose, no marketing
  language, no emoji.
