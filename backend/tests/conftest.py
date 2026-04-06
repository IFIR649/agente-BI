from __future__ import annotations

import json
from collections import deque
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.app.config import Settings
from backend.app.core.gemini_client import GeminiCallResult, GeminiClientError, GeminiResponseFormatError, GeminiUsageMetrics
from backend.app.main import create_app
from backend.app.models.telemetry import FXRateRecord


GENERIC_SHAPE_CSV = """time_axis,group_axis,measure_axis,record_code
2025-10-15,alpha,80.5,R-001
2025-11-10,beta,70.25,R-002
2025-12-20,alpha,90.75,R-003
2026-01-05,alpha,100.5,R-004
2026-02-07,beta,150.25,R-005
2026-03-20,alpha,100.5,R-006
"""

MULTI_TIME_SHAPE_CSV = """open_time,close_time,segment_axis,measure_axis
2026-01-03,2026-01-15,alpha,10
2026-02-05,2026-02-18,alpha,12
2026-02-20,2026-02-28,beta,8
2026-03-02,,beta,14
"""

SEMICOLON_SHAPE_CSV = """time_text;id_text;measure_text;flag_text;category_text
01/02/2026 0:00;1001;6084,48;1;alpha
01/02/2026 0:00;1002;1543,1;0;beta
02/02/2026 0:00;1003;100;1;alpha
03/02/2026 0:00;1004;0;0;beta
"""

WEEKDAY_SHAPE_CSV = """time_axis,group_axis,measure_axis
2026-01-09,alpha,50.75
2026-01-05,alpha,10.5
2026-01-12,beta,20.25
2026-01-11,beta,70.25
2026-01-06,alpha,30.5
2026-01-08,beta,60.25
2026-01-07,alpha,40.5
2026-01-10,beta,80.5
"""


class QueuedGemini:
    def __init__(self) -> None:
        self.structured_queue: deque[Any] = deque()
        self.text_queue: deque[Any] = deque()
        self.structured_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []
        self.cache_create_calls: list[dict[str, Any]] = []

    def queue_structured(self, *items: Any) -> None:
        self.structured_queue.extend(items)

    def queue_text(self, *items: Any) -> None:
        self.text_queue.extend(items)

    def generate_structured(self, **_: object) -> Any:
        return self.generate_structured_result(**_).payload

    def generate_structured_result(self, **_: object) -> GeminiCallResult:
        self.structured_calls.append(dict(_))
        if not self.structured_queue:
            raise AssertionError("No hay respuesta estructurada en cola para Gemini.")
        item = self.structured_queue.popleft()
        if isinstance(item, Exception):
            raise item
        return GeminiCallResult(
            payload=item,
            metrics=GeminiUsageMetrics(
                model=str(_.get("model") or "gemini-2.5-flash"),
                latency_ms=37,
                prompt_token_count=120,
                output_token_count=28,
                total_token_count=148,
            ),
        )

    def create_cached_content(self, **_: object) -> str:
        self.cache_create_calls.append(dict(_))
        return f"cachedContents/fake-{len(self.cache_create_calls)}"

    def generate_text(self, **_: object) -> str:
        return self.generate_text_result(**_).payload

    def generate_text_result(self, **_: object) -> GeminiCallResult:
        self.text_calls.append(dict(_))
        if not self.text_queue:
            item = "Resumen generado por Gemini de prueba."
        else:
            item = self.text_queue.popleft()
            if isinstance(item, Exception):
                raise item
        return GeminiCallResult(
            payload=str(item),
            metrics=GeminiUsageMetrics(
                model=str(_.get("model") or "gemini-2.5-flash"),
                latency_ms=29,
                prompt_token_count=90,
                output_token_count=18,
                total_token_count=108,
            ),
        )


class FixedFXResolver:
    def __init__(self, rate: float = 17.0) -> None:
        self.rate = rate

    def resolve(self, requested_date) -> FXRateRecord:
        return FXRateRecord(
            fx_date=requested_date,
            usd_to_mxn_rate=self.rate,
            fx_source="Banxico FIX",
        )


def build_settings(tmp_path: Path, *, rate_limit_requests: int = 20) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        uploads_dir=tmp_path / "data" / "uploads",
        catalogs_dir=tmp_path / "data" / "catalogs",
        logs_dir=tmp_path / "logs",
        audit_db_path=tmp_path / "logs" / "audit.db",
        rate_limit_requests=rate_limit_requests,
        rate_limit_window_seconds=60,
        gemini_api_key="test-gemini-key",
        gemini_flash_model="gemini-2.5-flash",
        gemini_pro_model="gemini-2.5-flash",
        gemini_lite_model="gemini-2.5-flash-lite",
        allow_local_gemini_fallback=False,
    )


def install_fake_gemini(app) -> QueuedGemini:
    fake = QueuedGemini()
    app.state._fake_gemini = fake
    app.state.gemini_client.create_cached_content = fake.create_cached_content
    app.state.gemini_client.generate_structured = fake.generate_structured
    app.state.gemini_client.generate_structured_result = fake.generate_structured_result
    app.state.gemini_client.generate_text = fake.generate_text
    app.state.gemini_client.generate_text_result = fake.generate_text_result
    return fake


def queue_fake_gemini(app, *, structured: list[Any] | None = None, texts: list[Any] | None = None) -> QueuedGemini:
    fake: QueuedGemini = app.state._fake_gemini
    if structured:
        fake.queue_structured(*structured)
    if texts:
        fake.queue_text(*texts)
    return fake


@pytest.fixture
def app(tmp_path: Path):
    app = create_app(build_settings(tmp_path))
    install_fake_gemini(app)
    app.state.fx_resolver = FixedFXResolver()
    app.state.audit_logger.fx_resolver = app.state.fx_resolver
    return app


@pytest.fixture
def client(app):
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def uploaded_shape_dataset(client: TestClient):
    response = client.post(
        "/datasets/upload",
        files={"file": ("shape.csv", GENERIC_SHAPE_CSV, "text/csv")},
        data={"metadata": json.dumps({"display_name": "Shape Dataset"})},
    )
    assert response.status_code == 201, response.text
    return response.json()


@pytest.fixture
def uploaded_multi_time_dataset(client: TestClient):
    response = client.post(
        "/datasets/upload",
        files={"file": ("multi_time.csv", MULTI_TIME_SHAPE_CSV, "text/csv")},
        data={"metadata": json.dumps({"display_name": "Multi Time"})},
    )
    assert response.status_code == 201, response.text
    return response.json()


@pytest.fixture
def uploaded_semicolon_dataset(client: TestClient):
    response = client.post(
        "/datasets/upload",
        files={"file": ("mixed.csv", SEMICOLON_SHAPE_CSV, "text/csv")},
        data={"metadata": json.dumps({"display_name": "Mixed Shape"})},
    )
    assert response.status_code == 201, response.text
    return response.json()


@pytest.fixture
def uploaded_weekday_dataset(client: TestClient):
    response = client.post(
        "/datasets/upload",
        files={"file": ("weekday.csv", WEEKDAY_SHAPE_CSV, "text/csv")},
        data={"metadata": json.dumps({"display_name": "Weekday Shape"})},
    )
    assert response.status_code == 201, response.text
    return response.json()
