from __future__ import annotations

"""
SQLiteMemoryRepo — lightweight episodic memory for the POST script path.

Stores verified fix episodes in a local SQLite file so they persist across
DAGMan workflow runs without requiring a PostgreSQL/SQLAlchemy stack.

Interface mirrors MemoryRepository (repository.py) so either can be injected
as svc["memory_repo"] in the graph config.
"""

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID


_DDL = """
CREATE TABLE IF NOT EXISTS memory_episodes (
    id              TEXT PRIMARY KEY,
    project_id      TEXT NOT NULL,
    failure_type    TEXT NOT NULL,
    transformation  TEXT,
    execution_site  TEXT,
    fix_action      TEXT NOT NULL,
    outcome         TEXT NOT NULL,
    config_delta    TEXT NOT NULL,          -- JSON
    source_incident TEXT NOT NULL,
    confidence      REAL NOT NULL,
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mem_project  ON memory_episodes(project_id);
CREATE INDEX IF NOT EXISTS idx_mem_ft       ON memory_episodes(failure_type);
CREATE INDEX IF NOT EXISTS idx_mem_xform    ON memory_episodes(transformation);
"""


class SQLiteMemoryRepo:
    """
    Async-compatible episodic memory backed by a local SQLite file.

    All DB calls are synchronous under the hood (sqlite3 is blocking) but
    wrapped in an async interface so graph nodes can await them uniformly
    alongside aiosqlite-backed repos.  The POST script runs in a single
    asyncio event loop with no concurrent DB writers, so blocking sqlite3
    is safe here.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        if db_path is None:
            db_path = Path.home() / ".pegasus_healer_memory.db"
        self._db_path = str(db_path)
        self._init_db()

    # ── Initialisation ────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with sqlite3.connect(self._db_path) as con:
            con.executescript(_DDL)
            con.commit()

    # ── Public async API ──────────────────────────────────────────────────────

    async def store_episode(
        self,
        project_id: str,
        failure_type: str,
        transformation: str | None,
        execution_site: str | None,
        fix_action: str,
        outcome: str,
        configuration_delta: dict[str, Any],
        source_incident_id: UUID,
        confidence: float,
    ) -> None:
        """
        Persist a verified fix episode.

        Increments occurrence_count when an identical
        (project_id, failure_type, fix_action, outcome) record exists.
        """
        now = datetime.now(timezone.utc).isoformat()
        delta_json = json.dumps(configuration_delta)

        with sqlite3.connect(self._db_path) as con:
            row = con.execute(
                """
                SELECT id, occurrence_count, confidence
                FROM memory_episodes
                WHERE project_id=? AND failure_type=? AND fix_action=? AND outcome=?
                LIMIT 1
                """,
                (project_id, failure_type, fix_action, outcome),
            ).fetchone()

            if row:
                new_count = row[1] + 1
                new_conf = max(row[2], confidence)
                con.execute(
                    """
                    UPDATE memory_episodes
                    SET occurrence_count=?, confidence=?, updated_at=?
                    WHERE id=?
                    """,
                    (new_count, new_conf, now, row[0]),
                )
            else:
                con.execute(
                    """
                    INSERT INTO memory_episodes
                        (id, project_id, failure_type, transformation,
                         execution_site, fix_action, outcome, config_delta,
                         source_incident, confidence, occurrence_count,
                         created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?)
                    """,
                    (
                        str(uuid.uuid4()),
                        project_id,
                        failure_type,
                        transformation,
                        execution_site,
                        fix_action,
                        outcome,
                        delta_json,
                        str(source_incident_id),
                        confidence,
                        now,
                        now,
                    ),
                )
            con.commit()

    async def find_similar(
        self,
        project_id: str,
        failure_type: str | None,
        transformation: str | None,
        workflow_id: str,       # kept for API parity; unused in SQLite impl
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """
        Retrieve similar past episodes, ranked by occurrence_count DESC.
        """
        query = "SELECT * FROM memory_episodes WHERE project_id=?"
        params: list[Any] = [project_id]

        if failure_type:
            query += " AND failure_type=?"
            params.append(failure_type)
        if transformation:
            query += " AND transformation=?"
            params.append(transformation)

        query += " ORDER BY occurrence_count DESC, confidence DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self._db_path) as con:
            con.row_factory = sqlite3.Row
            rows = con.execute(query, params).fetchall()

        return [
            {
                "failure_type": r["failure_type"],
                "fix_action": r["fix_action"],
                "outcome": r["outcome"],
                "confidence": r["confidence"],
                "occurrence_count": r["occurrence_count"],
                "configuration_delta": json.loads(r["config_delta"]),
            }
            for r in rows
        ]
