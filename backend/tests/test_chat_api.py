from __future__ import annotations

import json

from fastapi.testclient import TestClient

from backend.app.models.intent import QueryPlan, StructuredAgentDecision
from backend.tests.conftest import GENERIC_SHAPE_CSV, queue_fake_gemini


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


def _bootstrap_chat(client: TestClient, *, user_id: str = "test-user") -> dict:
    response = client.post(
        "/chat/bootstrap",
        files={"file": ("datos.csv", GENERIC_SHAPE_CSV, "text/csv")},
        data={"metadata": json.dumps({"display_name": "Dataset Bootstrap"})},
        headers={"X-User-Id": user_id},
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_chat_bootstrap_returns_session_and_ui_url(client: TestClient, app) -> None:
    payload = _bootstrap_chat(client)

    assert isinstance(payload["session_token"], str)
    assert payload["expires_in"] > 0
    assert payload["dataset"]["display_name"] == "Dataset Bootstrap"
    assert f"session_token={payload['session_token']}" in payload["ui_url"]
    assert "user_id=test-user" in payload["ui_url"]

    session = app.state.session_store.get_session(payload["session_token"])
    assert session is not None
    assert session.user_id == "test-user"
    assert session.dataset_id == payload["dataset"]["id"]


def test_chat_bootstrap_skips_gemini_column_labels(client: TestClient, app) -> None:
    payload = _bootstrap_chat(client)

    assert payload["dataset"]["columns"]["time_axis"]["label"] == "Time Axis"
    assert app.state._fake_gemini.structured_calls == []


def test_chat_bootstrap_requires_api_key(app) -> None:
    with TestClient(app, raise_server_exceptions=True) as raw_client:
        response = raw_client.post(
            "/chat/bootstrap",
            files={"file": ("datos.csv", GENERIC_SHAPE_CSV, "text/csv")},
            data={"metadata": json.dumps({"display_name": "Dataset Bootstrap"})},
            headers={"X-User-Id": "test-user"},
        )
    assert response.status_code == 401


def test_chat_bootstrap_rejects_invalid_api_key(app) -> None:
    with TestClient(app, raise_server_exceptions=True) as raw_client:
        response = raw_client.post(
            "/chat/bootstrap",
            files={"file": ("datos.csv", GENERIC_SHAPE_CSV, "text/csv")},
            data={"metadata": json.dumps({"display_name": "Dataset Bootstrap"})},
            headers={"X-API-Key": "bad-key", "X-User-Id": "test-user"},
        )
    assert response.status_code == 403


def test_chat_session_and_message_work_without_api_key(client: TestClient, app) -> None:
    payload = _bootstrap_chat(client)
    token = payload["session_token"]

    queue_fake_gemini(app, structured=[_make_plan()], texts=["Resumen bootstrap."])

    api_key = client.headers.pop("X-API-Key", None)
    try:
        response = client.get(
            "/chat/session",
            headers={"X-Session-Token": token, "X-User-Id": "test-user"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["dataset"]["id"] == payload["dataset"]["id"]

        message = client.post(
            "/chat/message",
            headers={"X-Session-Token": token, "X-User-Id": "test-user"},
            json={"question": "total de ventas por grupo", "history": []},
        )
        assert message.status_code == 200, message.text
        data = message.json()
        assert data["status"] == "ok"
        assert data["meta"]["dataset_id"] == payload["dataset"]["id"]
        assert data["meta"]["cached"] is False
    finally:
        if api_key is not None:
            client.headers["X-API-Key"] = api_key


def test_chat_session_rejects_user_mismatch(client: TestClient) -> None:
    payload = _bootstrap_chat(client)
    response = client.get(
        "/chat/session",
        headers={"X-Session-Token": payload["session_token"], "X-User-Id": "otro-usuario"},
    )
    assert response.status_code == 403


def test_chat_heartbeat_and_logout_destroy_session(client: TestClient, app) -> None:
    payload = _bootstrap_chat(client)
    token = payload["session_token"]

    api_key = client.headers.pop("X-API-Key", None)
    try:
        heartbeat = client.post(
            "/chat/heartbeat",
            headers={"X-Session-Token": token, "X-User-Id": "test-user"},
        )
        assert heartbeat.status_code == 200, heartbeat.text
        assert heartbeat.json()["ok"] is True

        logout = client.post(
            "/chat/logout",
            headers={"X-Session-Token": token, "X-User-Id": "test-user"},
        )
        assert logout.status_code == 200, logout.text
        assert logout.json()["ok"] is True
        assert app.state.session_store.get_session(token) is None

        after = client.get(
            "/chat/session",
            headers={"X-Session-Token": token, "X-User-Id": "test-user"},
        )
        assert after.status_code == 401
    finally:
        if api_key is not None:
            client.headers["X-API-Key"] = api_key
