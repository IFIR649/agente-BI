from __future__ import annotations

import logging
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from backend.app.core.auth import AuthContext
    from backend.app.models.dataset import DatasetCatalog

logger = logging.getLogger(__name__)


@dataclass
class Session:
    token: str
    user_id: str
    principal_id: int
    api_key_id: int
    created_at: float
    last_heartbeat: float
    actor_user_name: str | None = None
    client_id: str | None = None
    app_session_id: str | None = None
    dataset_id: str | None = None
    catalog: DatasetCatalog | None = None
    csv_path: Path | None = None
    catalog_path: Path | None = None
    duckdb_conn: duckdb.DuckDBPyConnection | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def has_dataset(self) -> bool:
        return self.dataset_id is not None and self.duckdb_conn is not None


class SessionStore:
    """
    Almacen en memoria de sesiones activas.

    Cada sesion tiene exactamente un CSV cargado en DuckDB en memoria.
    El SQLite (audit.db) registra las rutas de archivos para limpiar
    huerfanos despues de un crash.
    """

    def __init__(self, db_path: Path) -> None:
        self._sessions: dict[str, Session] = {}
        self._lock = threading.Lock()
        self._db_path = db_path
        self._init_db()
        self._cleanup_orphans()

    # ------------------------------------------------------------------ #
    # Ciclo de vida                                                        #
    # ------------------------------------------------------------------ #

    def create_session(self, auth: AuthContext | str) -> str:
        if isinstance(auth, str):
            from backend.app.core.auth import AuthContext

            auth = AuthContext(
                principal_id=0,
                api_key_id=0,
                actor_user_id=auth,
                actor_user_name=None,
                client_id=None,
                app_session_id=None,
                key_prefix="legacy",
            )
        token = str(uuid.uuid4())
        now = time.monotonic()
        session = Session(
            token=token,
            user_id=auth.actor_user_id,
            principal_id=auth.principal_id,
            api_key_id=auth.api_key_id,
            created_at=now,
            last_heartbeat=now,
            actor_user_name=auth.actor_user_name,
            client_id=auth.client_id,
            app_session_id=auth.app_session_id,
        )
        with self._lock:
            self._sessions[token] = session
        self._persist_session(session, csv_path=None, catalog_path=None)
        logger.info("session created token=%s user=%s", token, auth.actor_user_id)
        return token

    def attach_dataset(
        self,
        token: str,
        *,
        dataset_id: str,
        catalog: DatasetCatalog,
        csv_path: Path,
        catalog_path: Path,
        duckdb_conn: duckdb.DuckDBPyConnection,
    ) -> bool:
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return False
            session.dataset_id = dataset_id
            session.catalog = catalog
            session.csv_path = csv_path
            session.catalog_path = catalog_path
            session.duckdb_conn = duckdb_conn
        self._persist_session(session, csv_path=csv_path, catalog_path=catalog_path)
        logger.info("dataset attached token=%s dataset=%s", token, dataset_id)
        return True

    def get_session(self, token: str) -> Session | None:
        with self._lock:
            return self._sessions.get(token)

    def heartbeat(self, token: str) -> bool:
        with self._lock:
            session = self._sessions.get(token)
            if session is None:
                return False
            session.last_heartbeat = time.monotonic()
        return True

    def destroy_session(self, token: str) -> None:
        with self._lock:
            session = self._sessions.pop(token, None)
        if session is None:
            return
        self._teardown(session)
        self._remove_persisted(token)
        logger.info("session destroyed token=%s", token)

    def cleanup_expired(self, timeout_seconds: int) -> int:
        now = time.monotonic()
        expired: list[Session] = []
        with self._lock:
            for token, session in list(self._sessions.items()):
                if now - session.last_heartbeat > timeout_seconds:
                    expired.append(session)
                    del self._sessions[token]
        for session in expired:
            self._teardown(session)
            self._remove_persisted(session.token)
            logger.info("session expired token=%s user=%s", session.token, session.user_id)
        return len(expired)

    def destroy_all(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            self._teardown(session)
        self._clear_all_persisted()
        logger.info("all sessions destroyed count=%d", len(sessions))

    def active_count(self) -> int:
        with self._lock:
            return len(self._sessions)

    # ------------------------------------------------------------------ #
    # Internos                                                             #
    # ------------------------------------------------------------------ #

    def _teardown(self, session: Session) -> None:
        if session.duckdb_conn is not None:
            try:
                session.duckdb_conn.close()
            except Exception:
                pass
        if session.csv_path is not None:
            try:
                session.csv_path.unlink(missing_ok=True)
            except Exception:
                pass
        if session.catalog_path is not None:
            try:
                session.catalog_path.unlink(missing_ok=True)
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # Persistencia SQLite (solo rutas, para limpieza post-crash)          #
    # ------------------------------------------------------------------ #

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_sessions (
                    token TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    csv_path TEXT,
                    catalog_path TEXT,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._ensure_columns(
                conn,
                "active_sessions",
                {
                    "principal_id": "INTEGER",
                    "api_key_id": "INTEGER",
                    "actor_user_name": "TEXT",
                    "client_id": "TEXT",
                    "app_session_id": "TEXT",
                    "dataset_id": "TEXT",
                },
            )

    def _ensure_columns(self, conn: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
        existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        for name, sql_type in columns.items():
            if name not in existing:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {sql_type}")

    def _persist_session(self, session: Session, csv_path: Path | None, catalog_path: Path | None) -> None:
        from datetime import datetime, timezone
        try:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO active_sessions (
                        token,
                        user_id,
                        csv_path,
                        catalog_path,
                        created_at,
                        principal_id,
                        api_key_id,
                        actor_user_name,
                        client_id,
                        app_session_id,
                        dataset_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(token) DO UPDATE SET
                        csv_path = excluded.csv_path,
                        catalog_path = excluded.catalog_path,
                        principal_id = excluded.principal_id,
                        api_key_id = excluded.api_key_id,
                        actor_user_name = excluded.actor_user_name,
                        client_id = excluded.client_id,
                        app_session_id = excluded.app_session_id,
                        dataset_id = excluded.dataset_id
                    """,
                    (
                        session.token,
                        session.user_id,
                        str(csv_path) if csv_path else None,
                        str(catalog_path) if catalog_path else None,
                        datetime.now(timezone.utc).isoformat(),
                        session.principal_id,
                        session.api_key_id,
                        session.actor_user_name,
                        session.client_id,
                        session.app_session_id,
                        session.dataset_id,
                    ),
                )
        except Exception as exc:
            logger.warning("failed to persist session token=%s: %s", session.token, exc)

    def _remove_persisted(self, token: str) -> None:
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM active_sessions WHERE token = ?", (token,))
        except Exception as exc:
            logger.warning("failed to remove persisted session token=%s: %s", token, exc)

    def _clear_all_persisted(self) -> None:
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM active_sessions")
        except Exception as exc:
            logger.warning("failed to clear all persisted sessions: %s", exc)

    def _cleanup_orphans(self) -> None:
        """Limpia archivos de sesiones que quedaron del run anterior (post-crash)."""
        try:
            with self._connect() as conn:
                rows = conn.execute("SELECT token, csv_path, catalog_path FROM active_sessions").fetchall()
            if not rows:
                return
            logger.info("cleaning %d orphan sessions from previous run", len(rows))
            for row in rows:
                if row["csv_path"]:
                    Path(row["csv_path"]).unlink(missing_ok=True)
                if row["catalog_path"]:
                    Path(row["catalog_path"]).unlink(missing_ok=True)
            with self._connect() as conn:
                conn.execute("DELETE FROM active_sessions")
        except Exception as exc:
            logger.warning("orphan cleanup failed: %s", exc)
