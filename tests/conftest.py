from __future__ import annotations

from pathlib import Path

import pytest
import typer.rich_utils

from batchwizard.store import JobStore


@pytest.fixture(autouse=True, scope="session")
def plain_cli_output():
    """Render CLI output the same way locally and under CI.

    Typer forces terminal mode when GITHUB_ACTIONS, FORCE_COLOR, or PY_COLORS
    is set, and Rich then styles an option as several spans, so a rendered
    ``--model`` holds no literal ``--model`` substring. Without this,
    assertions on CLI output pass on a developer machine and fail only on CI.
    ``_TYPER_FORCE_DISABLE_TERMINAL`` is Typer's opt-out, but it is read at
    import time, so the constant it sets is patched directly instead.
    """
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(typer.rich_utils, "FORCE_TERMINAL", False)
        yield


@pytest.fixture
def store(tmp_path: Path) -> JobStore:
    s = JobStore(tmp_path / "jobs.db")
    yield s
    s.close()


@pytest.fixture
def jsonl_dir(tmp_path: Path) -> Path:
    """A directory with two JSONL input files and one distractor."""
    d = tmp_path / "inputs"
    d.mkdir()
    (d / "a.jsonl").write_text('{"custom_id": "1"}\n')
    (d / "b.jsonl").write_text('{"custom_id": "2"}\n')
    (d / "notes.txt").write_text("not a batch input")
    return d
