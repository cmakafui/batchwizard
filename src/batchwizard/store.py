# store.py
from __future__ import annotations

import sqlite3
from pathlib import Path

from .models import JobRecord, JobState, utcnow

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
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
"""

_FIELDS = [
    "provider",
    "batch_id",
    "input_path",
    "endpoint",
    "state",
    "provider_status",
    "output_path",
    "error_path",
    "error_summary",
    "created_at",
    "updated_at",
]


class JobStore:
    """Local manifest of submitted batch jobs, so `watch` can reattach after exit."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(_SCHEMA)
        self.conn.commit()

    def add(self, job: JobRecord) -> JobRecord:
        cur = self.conn.execute(
            f"INSERT INTO jobs ({', '.join(_FIELDS)}) VALUES ({', '.join('?' * len(_FIELDS))})",
            [getattr(job, f) for f in _FIELDS],
        )
        self.conn.commit()
        job.id = cur.lastrowid
        return job

    def update(self, job: JobRecord) -> None:
        job.updated_at = utcnow()
        assignments = ", ".join(f"{f} = ?" for f in _FIELDS)
        self.conn.execute(
            f"UPDATE jobs SET {assignments} WHERE batch_id = ?",
            [getattr(job, f) for f in _FIELDS] + [job.batch_id],
        )
        self.conn.commit()

    def get(self, batch_id: str) -> JobRecord | None:
        row = self.conn.execute(
            "SELECT * FROM jobs WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        return JobRecord(**dict(row)) if row else None

    def list(self, states: set[JobState] | None = None) -> list[JobRecord]:
        if states:
            placeholders = ", ".join("?" * len(states))
            rows = self.conn.execute(
                f"SELECT * FROM jobs WHERE state IN ({placeholders}) ORDER BY id",
                [s.value for s in states],
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
        return [JobRecord(**dict(row)) for row in rows]

    def pending(self) -> list[JobRecord]:
        return self.list({JobState.PENDING})

    def close(self) -> None:
        self.conn.close()
