from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path


class ActiveDatasetStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS user_active_datasets (
                    user_id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_active_datasets_dataset_id ON user_active_datasets(dataset_id)"
            )
            connection.commit()

    def get_active_dataset_id(self, user_id: str) -> str | None:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT dataset_id
                FROM user_active_datasets
                WHERE user_id = ?
                """,
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        return str(row["dataset_id"])

    def set_active_dataset(self, user_id: str, dataset_id: str) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO user_active_datasets (user_id, dataset_id, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        dataset_id = excluded.dataset_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        user_id,
                        dataset_id,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )
                connection.commit()

    def clear_active_dataset(self, user_id: str) -> None:
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    DELETE FROM user_active_datasets
                    WHERE user_id = ?
                    """,
                    (user_id,),
                )
                connection.commit()
