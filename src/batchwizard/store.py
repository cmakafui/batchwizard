# store.py
from __future__ import annotations

import sqlite3
from collections.abc import Set
from pathlib import Path

from .models import (
    ACTIONABLE_COLLECTION_STATES,
    ACTIVE_STATES,
    TERMINAL_STATES,
    CollectionState,
    JobRecord,
    JobState,
    utcnow,
)

_LATEST_SCHEMA_VERSION = 2

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    provider TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    input_path TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    state TEXT NOT NULL,
    provider_status TEXT NOT NULL DEFAULT '',
    collection_state TEXT NOT NULL DEFAULT 'not_ready',
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    cancelled_count INTEGER NOT NULL DEFAULT 0,
    expired_count INTEGER NOT NULL DEFAULT 0,
    total_count INTEGER NOT NULL DEFAULT 0,
    output_path TEXT,
    error_path TEXT,
    error_summary TEXT,
    last_local_error TEXT,
    poll_failures INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(provider, batch_id)
);
"""

_FIELDS = [
    "provider",
    "batch_id",
    "input_path",
    "endpoint",
    "state",
    "provider_status",
    "collection_state",
    "completed_count",
    "failed_count",
    "cancelled_count",
    "expired_count",
    "total_count",
    "output_path",
    "error_path",
    "error_summary",
    "last_local_error",
    "poll_failures",
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
        self._migrate()

    def _migrate(self) -> None:
        """Create or transactionally migrate the manifest schema."""
        exists = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
        ).fetchone()
        if not exists:
            with self.conn:
                self.conn.execute(_SCHEMA)
                self.conn.execute(f"PRAGMA user_version = {_LATEST_SCHEMA_VERSION}")
            return

        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version > _LATEST_SCHEMA_VERSION:
            self.conn.close()
            raise RuntimeError(
                f"Job manifest schema {version} is newer than this BatchWizard "
                f"supports ({_LATEST_SCHEMA_VERSION})"
            )
        if version == 0:
            self._migrate_v0_to_v1()
            version = 1
        if version == 1:
            self._migrate_v1_to_v2()

    def _migrate_v0_to_v1(self) -> None:
        columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(jobs)")}
        additions = {
            "collection_state": "TEXT NOT NULL DEFAULT 'not_ready'",
            "completed_count": "INTEGER NOT NULL DEFAULT 0",
            "failed_count": "INTEGER NOT NULL DEFAULT 0",
            "cancelled_count": "INTEGER NOT NULL DEFAULT 0",
            "expired_count": "INTEGER NOT NULL DEFAULT 0",
            "total_count": "INTEGER NOT NULL DEFAULT 0",
            "last_local_error": "TEXT",
            "poll_failures": "INTEGER NOT NULL DEFAULT 0",
        }
        with self.conn:
            for name, declaration in additions.items():
                if name not in columns:
                    self.conn.execute(
                        f"ALTER TABLE jobs ADD COLUMN {name} {declaration}"
                    )
            terminal = tuple(state.value for state in TERMINAL_STATES)
            placeholders = ", ".join("?" for _ in terminal)
            self.conn.execute(
                f"""
                UPDATE jobs
                SET collection_state = CASE
                    WHEN state IN ({placeholders})
                         AND (output_path IS NOT NULL OR error_path IS NOT NULL)
                        THEN ?
                    WHEN state IN ({placeholders}) THEN ?
                    ELSE ?
                END
                """,
                [
                    *terminal,
                    CollectionState.COLLECTED.value,
                    *terminal,
                    CollectionState.PENDING.value,
                    CollectionState.NOT_READY.value,
                ],
            )
            self.conn.execute("PRAGMA user_version = 1")

    def _migrate_v1_to_v2(self) -> None:
        """Make provider + batch ID the durable identity."""
        fields = ", ".join(["id", *_FIELDS])
        with self.conn:
            self.conn.execute("ALTER TABLE jobs RENAME TO jobs_v1")
            self.conn.execute(_SCHEMA)
            self.conn.execute(
                f"INSERT INTO jobs ({fields}) SELECT {fields} FROM jobs_v1"
            )
            self.conn.execute("DROP TABLE jobs_v1")
            self.conn.execute("PRAGMA user_version = 2")

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
        values = [getattr(job, f) for f in _FIELDS]
        if job.id is not None:
            self.conn.execute(
                f"UPDATE jobs SET {assignments} WHERE id = ?", values + [job.id]
            )
        else:
            self.conn.execute(
                f"UPDATE jobs SET {assignments} WHERE provider = ? AND batch_id = ?",
                values + [job.provider, job.batch_id],
            )
        self.conn.commit()

    def get(self, batch_id: str, provider: str | None = None) -> JobRecord | None:
        if provider is not None:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE provider = ? AND batch_id = ?",
                (provider, batch_id),
            ).fetchone()
            return JobRecord(**dict(row)) if row else None

        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE batch_id = ? ORDER BY id", (batch_id,)
        ).fetchall()
        if len(rows) > 1:
            providers = ", ".join(row["provider"] for row in rows)
            raise AmbiguousJobError(
                f"Batch ID {batch_id!r} exists for multiple providers: {providers}. "
                "Specify --provider."
            )
        return JobRecord(**dict(rows[0])) if rows else None

    def list(self, states: Set[JobState] | None = None) -> list[JobRecord]:
        if states:
            values = [state.value for state in states]
            placeholders = ", ".join("?" for _ in values)
            rows = self.conn.execute(
                f"SELECT * FROM jobs WHERE state IN ({placeholders}) ORDER BY id",
                values,
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
        return [JobRecord(**dict(row)) for row in rows]

    def pending(self) -> list[JobRecord]:
        """Return jobs whose remote execution is still active."""
        return self.list(ACTIVE_STATES)

    def actionable(self) -> list[JobRecord]:
        """Return remote-active jobs plus terminal jobs needing collection."""
        active = [state.value for state in ACTIVE_STATES]
        terminal = [state.value for state in TERMINAL_STATES]
        collection = [state.value for state in ACTIONABLE_COLLECTION_STATES]
        active_marks = ", ".join("?" for _ in active)
        terminal_marks = ", ".join("?" for _ in terminal)
        collection_marks = ", ".join("?" for _ in collection)
        rows = self.conn.execute(
            f"""
            SELECT * FROM jobs
            WHERE state IN ({active_marks})
               OR (state IN ({terminal_marks})
                   AND collection_state IN ({collection_marks}))
            ORDER BY id
            """,
            [*active, *terminal, *collection],
        ).fetchall()
        return [JobRecord(**dict(row)) for row in rows]

    def close(self) -> None:
        self.conn.close()


class AmbiguousJobError(ValueError):
    """A provider is required to select a non-unique native batch ID."""
