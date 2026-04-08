from __future__ import annotations

import hashlib
import hmac
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Header, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


@dataclass(frozen=True)
class AuthContext:
    principal_id: int
    api_key_id: int
    actor_user_id: str
    actor_user_name: str | None
    client_id: str | None
    app_session_id: str | None
    key_prefix: str


class AuthStore:
    def __init__(self, db_path: Path, api_key: str) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._api_key = api_key.strip()
        if not self._api_key:
            raise ValueError("AGENT_API_KEY es obligatorio para proteger la API.")
        self._key_hash = self._hash_api_key(self._api_key)
        self._key_prefix = self._api_key[:8]
        self._principal_key = "global-api-client"
        self._lock = threading.Lock()
        self._principal_id = 0
        self._api_key_id = 0
        self._init_db()
        self.sync_from_env()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS api_principals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    principal_key TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    last_seen_at TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS api_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    principal_id INTEGER NOT NULL,
                    key_prefix TEXT NOT NULL,
                    key_hash TEXT NOT NULL UNIQUE,
                    description TEXT,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at TEXT NOT NULL,
                    expires_at TEXT,
                    revoked_at TEXT,
                    last_used_at TEXT,
                    last_used_ip TEXT,
                    FOREIGN KEY (principal_id) REFERENCES api_principals(id)
                )
                """
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_principal_id ON api_keys(principal_id)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_status ON api_keys(status)")
            connection.commit()

    def sync_from_env(self) -> None:
        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as connection:
                row = connection.execute(
                    "SELECT id FROM api_principals WHERE principal_key = ?",
                    (self._principal_key,),
                ).fetchone()
                if row is None:
                    cursor = connection.execute(
                        """
                        INSERT INTO api_principals (principal_key, display_name, kind, status, created_at, last_seen_at)
                        VALUES (?, ?, ?, 'active', ?, ?)
                        """,
                        (self._principal_key, "Global Internal API", "service", now, now),
                    )
                    self._principal_id = int(cursor.lastrowid)
                else:
                    self._principal_id = int(row["id"])
                    connection.execute(
                        "UPDATE api_principals SET status = 'active', last_seen_at = ? WHERE id = ?",
                        (now, self._principal_id),
                    )

                key_row = connection.execute(
                    "SELECT id FROM api_keys WHERE key_hash = ?",
                    (self._key_hash,),
                ).fetchone()
                if key_row is None:
                    cursor = connection.execute(
                        """
                        INSERT INTO api_keys (
                            principal_id, key_prefix, key_hash, description, status, created_at, last_used_at
                        ) VALUES (?, ?, ?, ?, 'active', ?, ?)
                        """,
                        (self._principal_id, self._key_prefix, self._key_hash, "Clave global activa", now, now),
                    )
                    self._api_key_id = int(cursor.lastrowid)
                else:
                    self._api_key_id = int(key_row["id"])
                    connection.execute(
                        """
                        UPDATE api_keys
                        SET principal_id = ?, key_prefix = ?, description = ?, status = 'active'
                        WHERE id = ?
                        """,
                        (self._principal_id, self._key_prefix, "Clave global activa", self._api_key_id),
                    )

                connection.execute(
                    "UPDATE api_keys SET status = 'inactive' WHERE key_hash <> ?",
                    (self._key_hash,),
                )
                connection.commit()

    def authenticate(
        self,
        *,
        provided_api_key: str | None,
        actor_user_id: str | None,
        actor_user_name: str | None,
        client_id: str | None,
        app_session_id: str | None,
        client_ip: str | None,
    ) -> AuthContext:
        if not provided_api_key:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Falta X-API-Key.")
        if not actor_user_id or not actor_user_id.strip():
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Falta X-User-Id.")
        if not hmac.compare_digest(provided_api_key, self._api_key):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="X-API-Key invalida.")

        now = datetime.now(timezone.utc).isoformat()
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    "UPDATE api_principals SET last_seen_at = ? WHERE id = ?",
                    (now, self._principal_id),
                )
                connection.execute(
                    "UPDATE api_keys SET last_used_at = ?, last_used_ip = ? WHERE id = ?",
                    (now, client_ip, self._api_key_id),
                )
                connection.commit()

        return AuthContext(
            principal_id=self._principal_id,
            api_key_id=self._api_key_id,
            actor_user_id=actor_user_id.strip(),
            actor_user_name=(actor_user_name or "").strip() or None,
            client_id=(client_id or "").strip() or None,
            app_session_id=(app_session_id or "").strip() or None,
            key_prefix=self._key_prefix,
        )

    @staticmethod
    def _hash_api_key(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()


def get_auth_context(request: Request) -> AuthContext:
    auth = getattr(request.state, "auth", None)
    if auth is None:
        raise RuntimeError("No hay contexto de autenticacion en request.state.")
    return auth


async def require_api_access(
    request: Request,
    x_api_key: str | None = Security(api_key_header),
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
    x_user_name: str | None = Header(default=None, alias="X-User-Name"),
    x_client_id: str | None = Header(default=None, alias="X-Client-Id"),
    x_app_session_id: str | None = Header(default=None, alias="X-App-Session-Id"),
) -> AuthContext:
    auth_store: AuthStore = request.app.state.auth_store
    auth = auth_store.authenticate(
        provided_api_key=x_api_key,
        actor_user_id=x_user_id,
        actor_user_name=x_user_name,
        client_id=x_client_id,
        app_session_id=x_app_session_id,
        client_ip=request.client.host if request.client else None,
    )
    request.state.auth = auth
    return auth
