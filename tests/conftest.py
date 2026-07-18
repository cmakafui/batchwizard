from __future__ import annotations

from pathlib import Path

import pytest

from batchwizard.store import JobStore


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
