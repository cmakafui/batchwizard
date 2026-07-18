from __future__ import annotations

from batchwizard.models import JobRecord, JobState
from batchwizard.store import JobStore


def make_job(batch_id: str = "batch_1") -> JobRecord:
    return JobRecord(batch_id=batch_id, input_path="/tmp/a.jsonl")


def test_add_and_get_roundtrip(store: JobStore):
    job = store.add(make_job())
    assert job.id is not None

    loaded = store.get("batch_1")
    assert loaded is not None
    assert loaded.batch_id == "batch_1"
    assert loaded.state == JobState.PENDING
    assert loaded.provider == "openai"
    assert loaded.endpoint == "/v1/chat/completions"


def test_update_persists_all_fields(store: JobStore):
    job = store.add(make_job())
    job.state = JobState.FAILED
    job.provider_status = "failed"
    job.error_summary = "invalid_request: bad model (line 3)"
    job.error_path = "/tmp/out/batch_1_errors.jsonl"
    store.update(job)

    loaded = store.get("batch_1")
    assert loaded.state == JobState.FAILED
    assert loaded.error_summary == "invalid_request: bad model (line 3)"
    assert loaded.error_path == "/tmp/out/batch_1_errors.jsonl"
    assert loaded.updated_at >= loaded.created_at


def test_pending_excludes_terminal_jobs(store: JobStore):
    store.add(make_job("batch_pending"))
    done = store.add(make_job("batch_done"))
    done.state = JobState.COMPLETED
    store.update(done)

    pending = store.pending()
    assert [j.batch_id for j in pending] == ["batch_pending"]

    everything = store.list()
    assert len(everything) == 2


def test_duplicate_batch_id_rejected(store: JobStore):
    import sqlite3

    import pytest

    store.add(make_job("batch_dup"))
    with pytest.raises(sqlite3.IntegrityError):
        store.add(make_job("batch_dup"))


def test_store_survives_reopen(tmp_path):
    path = tmp_path / "jobs.db"
    s1 = JobStore(path)
    s1.add(make_job("batch_persist"))
    s1.close()

    s2 = JobStore(path)
    assert s2.get("batch_persist") is not None
    s2.close()
