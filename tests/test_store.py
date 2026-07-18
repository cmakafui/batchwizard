from __future__ import annotations

import sqlite3

import pytest

from batchwizard.models import CollectionState, JobRecord, JobState
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
    job.collection_state = CollectionState.COLLECTED
    job.completed_count = 7
    job.failed_count = 2
    job.total_count = 9
    job.last_local_error = "previous download failure"
    job.poll_failures = 3
    store.update(job)

    loaded = store.get("batch_1")
    assert loaded.state == JobState.FAILED
    assert loaded.error_summary == "invalid_request: bad model (line 3)"
    assert loaded.error_path == "/tmp/out/batch_1_errors.jsonl"
    assert loaded.collection_state == CollectionState.COLLECTED
    assert loaded.completed_count == 7
    assert loaded.failed_count == 2
    assert loaded.total_count == 9
    assert loaded.last_local_error == "previous download failure"
    assert loaded.poll_failures == 3
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


def test_actionable_includes_active_and_failed_collection(store: JobStore):
    active = store.add(make_job("batch_active"))
    collecting = store.add(make_job("batch_collect"))
    collecting.state = JobState.COMPLETED
    collecting.collection_state = CollectionState.FAILED
    collecting.last_local_error = "disk full"
    store.update(collecting)
    collected = store.add(make_job("batch_done"))
    collected.state = JobState.COMPLETED
    collected.collection_state = CollectionState.COLLECTED
    store.update(collected)

    assert [job.batch_id for job in store.pending()] == [active.batch_id]
    assert [job.batch_id for job in store.actionable()] == [
        "batch_active",
        "batch_collect",
    ]


def test_v04_manifest_migrates_conservatively(tmp_path):
    path = tmp_path / "jobs.db"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            batch_id TEXT NOT NULL UNIQUE,
            input_path TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            state TEXT NOT NULL,
            provider_status TEXT NOT NULL DEFAULT '',
            output_path TEXT,
            error_path TEXT,
            error_summary TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        INSERT INTO jobs (
            provider, batch_id, input_path, endpoint, state, provider_status,
            output_path, error_path, error_summary, created_at, updated_at
        ) VALUES
            ('openai', 'active', '/tmp/a.jsonl', '/v1/responses',
             'pending', 'in_progress', NULL, NULL, NULL, '2026', '2026'),
            ('openai', 'with_files', '/tmp/b.jsonl', '/v1/responses',
             'completed', 'completed', '/tmp/results.jsonl', NULL, NULL,
             '2026', '2026'),
            ('openai', 'missing_files', '/tmp/c.jsonl', '/v1/responses',
             'completed', 'completed', NULL, NULL, NULL, '2026', '2026');
        """
    )
    connection.close()

    store = JobStore(path)

    assert store.conn.execute("PRAGMA user_version").fetchone()[0] == 1
    assert store.get("active").collection_state == CollectionState.NOT_READY
    assert store.get("with_files").collection_state == CollectionState.COLLECTED
    assert store.get("missing_files").collection_state == CollectionState.PENDING
    assert [job.batch_id for job in store.actionable()] == [
        "active",
        "missing_files",
    ]
    store.close()


def test_newer_manifest_schema_is_rejected(tmp_path):
    path = tmp_path / "jobs.db"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE jobs (id INTEGER PRIMARY KEY)")
    connection.execute("PRAGMA user_version = 999")
    connection.commit()
    connection.close()

    with pytest.raises(RuntimeError, match="newer than this BatchWizard supports"):
        JobStore(path)
