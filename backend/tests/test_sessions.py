from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.main import create_app
from backend.app.models.intent import AgentDecision, QueryPlan, StructuredAgentDecision
from backend.tests.conftest import (
    GENERIC_SHAPE_CSV,
    FixedFXResolver,
    build_settings,
    install_fake_gemini,
    queue_fake_gemini,
)

# ------------------------------------------------------------------ #
# Fixtures                                                             #
# ------------------------------------------------------------------ #

@pytest.fixture
def app(tmp_path: Path):
    app = create_app(build_settings(tmp_path))
    install_fake_gemini(app)
    app.state.fx_resolver = FixedFXResolver()
    app.state.audit_logger.fx_resolver = app.state.fx_resolver
    return app


@pytest.fixture
def client(app):
    with TestClient(app, raise_server_exceptions=True) as test_client:
        test_client.headers.update({"X-API-Key": "test-api-key", "X-User-Id": "test-user"})
        yield test_client


# CSV minimo reutilizable
_CSV = GENERIC_SHAPE_CSV


def _create_session(client: TestClient, user_id: str = "test-user") -> str:
    resp = client.post("/sessions", json={"user_id": user_id})
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert "token" in data
    assert data["expires_in"] > 0
    return data["token"]


def _upload_csv(client: TestClient, token: str, csv: str = _CSV) -> dict:
    resp = client.post(
        f"/sessions/{token}/upload",
        files={"file": ("datos.csv", csv, "text/csv")},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


# ------------------------------------------------------------------ #
# Crear sesion                                                         #
# ------------------------------------------------------------------ #

def test_create_session_returns_token(client: TestClient) -> None:
    resp = client.post("/sessions", json={"user_id": "usuario1"})
    assert resp.status_code == 201
    data = resp.json()
    assert "token" in data
    assert isinstance(data["token"], str) and len(data["token"]) == 36  # UUID


def test_create_session_via_header(client: TestClient) -> None:
    resp = client.post("/sessions", headers={"X-User-Id": "usuario-header"})
    assert resp.status_code == 201
    assert "token" in resp.json()


def test_create_session_no_body(client: TestClient) -> None:
    resp = client.post("/sessions")
    assert resp.status_code == 201
    assert "token" in resp.json()


def test_max_concurrent_sessions(app, tmp_path: Path) -> None:
    """Al alcanzar el limite, el servidor devuelve 503."""
    settings = build_settings(tmp_path)
    settings.max_concurrent_sessions = 2
    limited_app = create_app(settings)
    with TestClient(limited_app) as c:
        c.headers.update({"X-API-Key": "test-api-key", "X-User-Id": "u1"})
        c.post("/sessions", json={"user_id": "u1"})
        c.headers.update({"X-User-Id": "u2"})
        c.post("/sessions", json={"user_id": "u2"})
        c.headers.update({"X-User-Id": "u3"})
        resp = c.post("/sessions", json={"user_id": "u3"})
        assert resp.status_code == 503


# ------------------------------------------------------------------ #
# Upload CSV                                                           #
# ------------------------------------------------------------------ #

def test_upload_csv_success(client: TestClient) -> None:
    token = _create_session(client)
    dataset = _upload_csv(client, token)
    assert "id" in dataset
    assert dataset["filename"].endswith(".csv")


def test_upload_csv_invalid_token(client: TestClient) -> None:
    resp = client.post(
        "/sessions/token-falso/upload",
        files={"file": ("datos.csv", _CSV, "text/csv")},
    )
    assert resp.status_code == 401


def test_upload_csv_not_csv(client: TestClient) -> None:
    token = _create_session(client)
    resp = client.post(
        f"/sessions/{token}/upload",
        files={"file": ("datos.txt", b"hello", "text/plain")},
    )
    assert resp.status_code == 400


def test_upload_csv_empty(client: TestClient) -> None:
    token = _create_session(client)
    resp = client.post(
        f"/sessions/{token}/upload",
        files={"file": ("datos.csv", b"", "text/csv")},
    )
    assert resp.status_code == 400


def test_upload_csv_duplicate_rejected(client: TestClient) -> None:
    token = _create_session(client)
    _upload_csv(client, token)
    # segundo upload en la misma sesion
    resp = client.post(
        f"/sessions/{token}/upload",
        files={"file": ("otro.csv", _CSV, "text/csv")},
    )
    assert resp.status_code == 409


def test_upload_creates_duckdb_table(client: TestClient, app) -> None:
    """Verifica que el CSV queda en memoria de DuckDB (no como VIEW)."""
    token = _create_session(client)
    _upload_csv(client, token)
    session = app.state.session_store.get_session(token)
    assert session is not None
    assert session.duckdb_conn is not None
    # La tabla "dataset" debe existir en la conexion
    result = session.duckdb_conn.execute("SELECT count(*) FROM dataset").fetchone()
    assert result[0] > 0


# ------------------------------------------------------------------ #
# Heartbeat                                                            #
# ------------------------------------------------------------------ #

def test_heartbeat_ok(client: TestClient) -> None:
    token = _create_session(client)
    resp = client.post(f"/sessions/{token}/heartbeat")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_heartbeat_invalid_token(client: TestClient) -> None:
    resp = client.post("/sessions/no-existe/heartbeat")
    assert resp.status_code == 401


def test_heartbeat_updates_timestamp(client: TestClient, app) -> None:
    token = _create_session(client)
    session = app.state.session_store.get_session(token)
    t_before = session.last_heartbeat
    time.sleep(0.01)
    client.post(f"/sessions/{token}/heartbeat")
    assert session.last_heartbeat > t_before


# ------------------------------------------------------------------ #
# Logout / Destroy                                                     #
# ------------------------------------------------------------------ #

def test_delete_session_destroys_files(client: TestClient, app) -> None:
    token = _create_session(client)
    dataset = _upload_csv(client, token)
    session = app.state.session_store.get_session(token)
    csv_path = session.csv_path
    catalog_path = session.catalog_path
    assert csv_path and csv_path.exists()
    assert catalog_path and catalog_path.exists()

    resp = client.delete(f"/sessions/{token}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    assert not csv_path.exists(), "CSV debe eliminarse al cerrar sesion"
    assert not catalog_path.exists(), "Catalogo debe eliminarse al cerrar sesion"
    assert app.state.session_store.get_session(token) is None


def test_logout_via_post(client: TestClient) -> None:
    token = _create_session(client)
    _upload_csv(client, token)
    resp = client.post(f"/sessions/{token}/logout")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_delete_idempotent(client: TestClient) -> None:
    token = _create_session(client)
    client.delete(f"/sessions/{token}")
    resp = client.delete(f"/sessions/{token}")  # segunda vez
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


# ------------------------------------------------------------------ #
# Query                                                                #
# ------------------------------------------------------------------ #

def _make_plan() -> StructuredAgentDecision:
    return StructuredAgentDecision(
        kind="query",
        plan=QueryPlan(
            intent="aggregate_report",
            metrics=["measure_axis_sum"],
            dimensions=["group_axis"],
            visualization="bar",
            confidence=0.9,
        ),
    )


def test_session_query_no_dataset(client: TestClient) -> None:
    """Query antes de subir CSV devuelve 412."""
    token = _create_session(client)
    resp = client.post(f"/sessions/{token}/query", json={"question": "ventas totales"})
    assert resp.status_code == 412


def test_session_query_invalid_token(client: TestClient) -> None:
    resp = client.post("/sessions/no-existe/query", json={"question": "ventas"})
    assert resp.status_code == 401


def test_session_query_success(client: TestClient, app) -> None:
    """Flujo completo: crear sesion → subir CSV → consultar."""
    install_fake_gemini(app)
    token = _create_session(client)
    _upload_csv(client, token)

    queue_fake_gemini(app, structured=[_make_plan()], texts=["Resumen de prueba."])

    resp = client.post(
        f"/sessions/{token}/query",
        json={"question": "total de ventas por grupo"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "ok"
    assert "summary" in data
    assert data["meta"]["dataset_id"] is not None
    assert data["meta"]["cached"] is False


def test_session_query_cached(client: TestClient, app) -> None:
    """Segunda consulta identica usa cache."""
    install_fake_gemini(app)
    token = _create_session(client)
    _upload_csv(client, token)

    queue_fake_gemini(app, structured=[_make_plan()], texts=["Resumen de prueba."])

    question = "total de ventas por grupo"
    client.post(f"/sessions/{token}/query", json={"question": question})

    resp2 = client.post(f"/sessions/{token}/query", json={"question": question})
    assert resp2.status_code == 200
    assert resp2.json()["meta"]["cached"] is True


def test_session_query_audit_supports_session_filters(client: TestClient, app) -> None:
    install_fake_gemini(app)
    token = _create_session(client)
    _upload_csv(client, token)

    queue_fake_gemini(app, structured=[_make_plan()], texts=["Resumen de prueba."])

    response = client.post(
        f"/sessions/{token}/query",
        json={"question": "total de ventas por grupo"},
        headers={
            "X-API-Key": "test-api-key",
            "X-User-Id": "test-user",
            "X-Client-Id": "cliente-sesiones",
            "X-App-Session-Id": "app-session-001",
            "X-User-Name": "Tester Sesiones",
        },
    )
    assert response.status_code == 200, response.text

    metrics = client.get(
        "/metrics/queries",
        params={
            "session_token": token,
            "client_id": "cliente-sesiones",
            "app_session_id": "app-session-001",
            "user_id": "test-user",
        },
    )
    assert metrics.status_code == 200, metrics.text
    payload = metrics.json()
    assert payload["total"] == 1
    assert payload["items"][0]["session_token"] == token
    assert payload["items"][0]["client_id"] == "cliente-sesiones"
    assert payload["items"][0]["app_session_id"] == "app-session-001"


# ------------------------------------------------------------------ #
# Cleanup automatico de sesiones expiradas                             #
# ------------------------------------------------------------------ #

def test_cleanup_expired_sessions(app, tmp_path: Path) -> None:
    """SessionStore.cleanup_expired elimina sesiones sin heartbeat reciente."""
    store = app.state.session_store
    token = store.create_session("usuario-test")
    session = store.get_session(token)
    assert session is not None

    # Forzar que la sesion aparezca como expirada
    session.last_heartbeat = time.monotonic() - 9999

    expired = store.cleanup_expired(timeout_seconds=1)
    assert expired == 1
    assert store.get_session(token) is None


def test_cleanup_does_not_remove_active_sessions(app) -> None:
    store = app.state.session_store
    token = store.create_session("usuario-activo")

    expired = store.cleanup_expired(timeout_seconds=300)
    assert expired == 0
    assert store.get_session(token) is not None


# ------------------------------------------------------------------ #
# Crash recovery: limpieza de huerfanos al iniciar                    #
# ------------------------------------------------------------------ #

def test_orphan_cleanup_on_startup(tmp_path: Path) -> None:
    """Archivos registrados en SQLite de un run anterior se borran al iniciar."""
    from backend.app.core.session import SessionStore
    import sqlite3

    db_path = tmp_path / "logs" / "audit.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # CSV huerfano simulado
    csv_file = tmp_path / "data" / "uploads" / "huerfano.csv"
    csv_file.parent.mkdir(parents=True, exist_ok=True)
    csv_file.write_text("col1,col2\n1,2\n")

    catalog_file = tmp_path / "data" / "catalogs" / "huerfano.json"
    catalog_file.parent.mkdir(parents=True, exist_ok=True)
    catalog_file.write_text("{}")

    # Insertar entrada huerfana en SQLite como si viniera del run anterior
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS active_sessions (
            token TEXT PRIMARY KEY, user_id TEXT NOT NULL,
            csv_path TEXT, catalog_path TEXT, created_at TEXT NOT NULL
        )"""
    )
    conn.execute(
        "INSERT INTO active_sessions VALUES (?, ?, ?, ?, ?)",
        ("token-huerfano", "user", str(csv_file), str(catalog_file), "2026-01-01T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()

    assert csv_file.exists()
    assert catalog_file.exists()

    # Al crear SessionStore se dispara la limpieza
    SessionStore(db_path)

    assert not csv_file.exists(), "CSV huerfano debe haberse borrado"
    assert not catalog_file.exists(), "Catalogo huerfano debe haberse borrado"
