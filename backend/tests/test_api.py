from __future__ import annotations

import json
import sqlite3
import time
from datetime import date
from types import SimpleNamespace

from fastapi.testclient import TestClient

from backend.app.core.fx import BanxicoFxResolver
from backend.app.core.gemini_client import GeminiClient
from backend.app.core.telemetry import enrich_llm_call, estimate_call_cost_usd
from backend.app.core.gemini_client import GeminiClientError, GeminiResponseFormatError
from backend.app.main import create_app
from backend.app.models.intent import AgentDecision, QueryFilter, QueryPlan, SortSpec, StructuredAgentDecision
from backend.app.models.telemetry import FXRateRecord, LLMCallTelemetry
from backend.app.services.dataset_profiler import ColumnLabelsResponse
from backend.tests.conftest import build_settings, install_fake_gemini, queue_fake_gemini


def test_root_endpoint(client: TestClient) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Agente de Analisis CSV" in response.text
    assert "/static/app.js" in response.text


def test_analytics_endpoint(client: TestClient) -> None:
    response = client.get("/analytics")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Metricas de consumo" in response.text
    assert "/static/analytics.js" in response.text


def test_api_info_endpoint(client: TestClient) -> None:
    response = client.get("/api-info")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["ui_url"] == "/"
    assert payload["analytics_ui_url"] == "/analytics"
    assert payload["docs_url"] == "/docs"
    assert payload["metrics_queries_url"] == "/metrics/queries"
    assert payload["metrics_summary_url"] == "/metrics/summary"
    assert payload["metrics_timeseries_url"] == "/metrics/timeseries"


def test_gemini_usage_metadata_is_normalized(tmp_path) -> None:
    client = GeminiClient(build_settings(tmp_path))
    metrics = client._build_metrics(
        model="gemini-2.5-flash",
        response=SimpleNamespace(
            usage_metadata=SimpleNamespace(
                prompt_token_count=11,
                candidates_token_count=7,
                thoughts_token_count=3,
                tool_use_prompt_token_count=2,
                cached_content_token_count=1,
                total_token_count=24,
            ),
            model_version="gemini-2.5-flash-001",
        ),
        started_at=time.perf_counter(),
    )

    assert metrics.model == "gemini-2.5-flash-001"
    assert metrics.prompt_token_count == 11
    assert metrics.output_token_count == 7
    assert metrics.thoughts_token_count == 3
    assert metrics.tool_use_prompt_token_count == 2
    assert metrics.cached_content_token_count == 1
    assert metrics.total_token_count == 24


def test_estimated_cost_uses_model_pricing() -> None:
    flash_lite_call = LLMCallTelemetry(
        stage="intent",
        model="gemini-2.5-flash-lite",
        prompt_token_count=120,
        output_token_count=28,
        total_token_count=148,
    )
    flash_call = LLMCallTelemetry(
        stage="intent",
        model="gemini-2.5-flash",
        prompt_token_count=120,
        output_token_count=28,
        total_token_count=148,
    )

    assert estimate_call_cost_usd(flash_lite_call) == 0.0000232
    assert estimate_call_cost_usd(flash_call) == 0.000106


def test_thinking_cost_uses_same_rate_as_output() -> None:
    call = enrich_llm_call(
        LLMCallTelemetry(
            stage="summary",
            model="gemini-2.5-flash-lite",
            prompt_token_count=0,
            output_token_count=10,
            thoughts_token_count=5,
            total_token_count=15,
        ),
        FXRateRecord(fx_date=date(2026, 4, 1), usd_to_mxn_rate=17.0, fx_source="Banxico FIX"),
    )

    assert call.output_cost_usd == 0.000004
    assert call.thinking_cost_usd == 0.000002
    assert call.total_cost_mxn == 0.000102


def test_banxico_resolver_falls_back_to_previous_business_day_and_uses_cache(tmp_path) -> None:
    calls: list[date] = []

    def fetcher(requested_date: date) -> str:
        calls.append(requested_date)
        if requested_date == date(2026, 4, 3):
            return """
            <td class="renglonPar">03/04/2026</td>
            <td class="renglonPar">18.2345</td>
            <td class="renglonPar">18.3345</td>
            <td class="renglonPar">18.3000</td>
            """
        return "<html>sin datos</html>"

    resolver = BanxicoFxResolver(
        db_path=tmp_path / "fx.db",
        service_url="https://example.test/banxico",
        fetcher=fetcher,
    )

    weekend_record = resolver.resolve(date(2026, 4, 5))
    cached_record = resolver.resolve(date(2026, 4, 5))

    assert weekend_record.fx_date == date(2026, 4, 3)
    assert weekend_record.usd_to_mxn_rate == 18.2345
    assert cached_record.fx_date == date(2026, 4, 3)
    assert len(calls) == 3


def test_static_assets_are_served(client: TestClient) -> None:
    response = client.get("/static/app.js")
    assert response.status_code == 200
    assert "javascript" in response.headers["content-type"]

    analytics = client.get("/static/analytics.js")
    assert analytics.status_code == 200
    assert "javascript" in analytics.headers["content-type"]


def test_upload_and_list_datasets(client: TestClient, uploaded_shape_dataset: dict) -> None:
    dataset_id = uploaded_shape_dataset["id"]
    metric_names = {metric["name"] for metric in uploaded_shape_dataset["metrics_allowed"]}

    assert uploaded_shape_dataset["default_date_column"] == "time_axis"
    assert uploaded_shape_dataset["default_metric"] == "measure_axis_sum"
    assert "measure_axis_sum" in metric_names
    assert "time_axis_month" in uploaded_shape_dataset["dimensions_allowed"]
    assert "group_axis" in uploaded_shape_dataset["suggested_dimensions"]

    response = client.get("/datasets")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["id"] == dataset_id


def test_multi_time_dataset_picks_structural_default_date(client: TestClient, uploaded_multi_time_dataset: dict) -> None:
    assert uploaded_multi_time_dataset["default_date_column"] == "open_time"


def test_structural_profiling_handles_semicolon_decimals_and_identifiers(
    client: TestClient,
    uploaded_semicolon_dataset: dict,
) -> None:
    metrics = {metric["name"] for metric in uploaded_semicolon_dataset["metrics_allowed"]}
    columns = uploaded_semicolon_dataset["columns"]

    assert uploaded_semicolon_dataset["default_date_column"] == "time_text"
    assert uploaded_semicolon_dataset["default_metric"] == "measure_text_sum"
    assert "measure_text_sum" in metrics
    assert "flag_text_sum" in metrics
    assert "id_text_sum" not in metrics
    assert columns["measure_text"]["semantic_role"] == "measure"
    assert columns["id_text"]["semantic_role"] == "identifier"
    assert columns["flag_text"]["semantic_role"] == "flag"
    assert columns["time_text"]["semantic_role"] == "time"


def test_upload_generates_labels_with_gemini(client: TestClient, app) -> None:
    queue_fake_gemini(
        app,
        structured=[
            ColumnLabelsResponse(
                labels={
                    "time_axis": "Fecha Venta",
                    "group_axis": "Sucursal",
                    "measure_axis": "Ventas Netas",
                    "record_code": "Codigo",
                }
            )
        ],
    )

    response = client.post(
        "/datasets/upload",
        files={"file": ("shape.csv", "time_axis,group_axis,measure_axis,record_code\n2026-01-01,norte,100,R-1\n", "text/csv")},
        data={"metadata": json.dumps({"display_name": "Etiquetas Gemini"})},
    )
    assert response.status_code == 201, response.text
    payload = response.json()

    assert payload["columns"]["time_axis"]["label"] == "Fecha Venta"
    assert payload["columns"]["measure_axis"]["label"] == "Ventas Netas"
    assert any(metric["label"] == "Total de Ventas Netas" for metric in payload["metrics_allowed"])

    catalog = app.state.dataset_profiler.get_catalog(payload["id"])
    assert catalog is not None
    assert catalog.dimension_definitions["time_axis_day_of_week"].label == "Fecha Venta por Dia de la Semana"


def test_upload_falls_back_when_label_generation_fails(client: TestClient, app) -> None:
    queue_fake_gemini(app, structured=[GeminiClientError("fallo generando labels")])

    response = client.post(
        "/datasets/upload",
        files={"file": ("shape.csv", "time_axis,group_axis,measure_axis\n2026-01-01,alpha,100\n", "text/csv")},
    )
    assert response.status_code == 201, response.text
    payload = response.json()

    assert payload["columns"]["measure_axis"]["label"] == "Measure Axis"
    assert any(metric["label"] == "Total de Measure Axis" for metric in payload["metrics_allowed"])


def test_query_uses_human_labels_everywhere(client: TestClient, app) -> None:
    queue_fake_gemini(
        app,
        structured=[
            ColumnLabelsResponse(
                labels={
                    "time_axis": "Fecha Venta",
                    "group_axis": "Sucursal",
                    "measure_axis": "Ventas Netas",
                    "record_code": "Codigo",
                }
            )
        ],
    )
    upload = client.post(
        "/datasets/upload",
        files={"file": ("shape.csv", "time_axis,group_axis,measure_axis,record_code\n2026-01-01,norte,100,R-1\n2026-01-02,sur,150,R-2\n", "text/csv")},
    )
    dataset_id = upload.json()["id"]

    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["group_axis"],
                    metrics=["measure_axis_sum"],
                    sort=SortSpec(field="measure_axis_sum", order="desc"),
                    visualization="bar",
                    confidence=0.96,
                ),
            )
        ],
        texts=["Resumen con labels."],
    )

    response = client.post(
        "/query",
        json={"dataset_id": dataset_id, "question": "ventas por sucursal"},
        headers={"X-User-Id": "usr_labels"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()

    assert payload["kpis"][0]["label"] == "Total de Ventas Netas"
    assert payload["table"]["columns"] == ["Sucursal", "Total de Ventas Netas"]
    assert payload["chart"]["series"][0]["name"] == "Total de Ventas Netas"

    prompt = app.state._fake_gemini.text_calls[-1]["prompt"]
    assert "measure_axis_sum -> Total de Ventas Netas" in prompt
    assert "group_axis -> Sucursal" in prompt


def test_query_aggregate_and_cache(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    dataset_id = uploaded_shape_dataset["id"]
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["group_axis"],
                    metrics=["measure_axis_sum"],
                    filters=[QueryFilter(field="time_axis", op="between", value=["2026-01-01", "2026-03-20"])],
                    sort=SortSpec(field="measure_axis_sum", order="desc"),
                    visualization="bar",
                    confidence=0.96,
                ),
            )
        ],
        texts=["Resumen generado por Gemini de prueba."],
    )

    payload = {"dataset_id": dataset_id, "question": "muestra measure_axis por group_axis"}
    headers = {"X-User-Id": "usr_001"}

    first = client.post("/query", json=payload, headers=headers)
    assert first.status_code == 200, first.text
    body = first.json()
    assert body["status"] == "ok"
    assert body["chart"]["type"] == "bar"
    assert body["meta"]["cached"] is False
    assert body["telemetry"]["status"] == "ok"
    assert body["telemetry"]["cache_hit"] is False
    assert body["telemetry"]["llm_totals"]["call_count"] == 2
    assert body["telemetry"]["llm_totals"]["input_token_count"] == 210
    assert body["telemetry"]["llm_totals"]["output_token_count"] == 46
    assert body["telemetry"]["llm_totals"]["thinking_token_count"] == 0
    assert body["telemetry"]["llm_totals"]["total_token_count"] == 256
    assert body["telemetry"]["llm_totals"]["total_cost_usd"] == 0.0000394
    assert body["telemetry"]["llm_totals"]["total_cost_mxn"] == 0.0006698
    assert len(body["telemetry"]["llm_calls"]) == 2

    second = client.post("/query", json=payload, headers=headers)
    assert second.status_code == 200, second.text
    cached_body = second.json()
    assert cached_body["meta"]["cached"] is True
    assert cached_body["telemetry"]["cache_hit"] is True
    assert cached_body["telemetry"]["llm_totals"]["call_count"] == 0
    assert cached_body["telemetry"]["llm_totals"]["total_token_count"] == 0
    assert cached_body["telemetry"]["llm_totals"]["total_cost_mxn"] == 0


def test_query_time_series_and_audit_log(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    dataset_id = uploaded_shape_dataset["id"]
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="time_series_report",
                    dimensions=["time_axis_month"],
                    metrics=["measure_axis_sum"],
                    filters=[QueryFilter(field="time_axis", op="between", value=["2026-01-01", "2026-03-20"])],
                    sort=SortSpec(field="time_axis_month", order="asc"),
                    time_granularity="month",
                    visualization="line",
                    confidence=0.95,
                ),
            )
        ],
        texts=["Resumen temporal de prueba."],
    )

    response = client.post(
        "/query",
        json={"dataset_id": dataset_id, "question": "evolucion mensual de measure_axis"},
        headers={"X-User-Id": "usr_002"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert body["chart"]["type"] == "line"
    assert body["meta"]["plan"]["intent"] == "time_series_report"
    assert body["telemetry"]["llm_totals"]["call_count"] == 2
    assert body["telemetry"]["llm_totals"]["total_cost_mxn"] > 0

    with sqlite3.connect(app.state.settings.audit_db_path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM query_audit").fetchone()[0]
        llm_count = connection.execute("SELECT COUNT(*) FROM query_llm_calls").fetchone()[0]
    assert count >= 1
    assert llm_count == 2


def test_meta_questions_return_assistant_message(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="assistant_message",
                message="Puedo resumir la medida principal, compararla por group_axis y mostrar su evolucion temporal.",
                reason="Guia basada en el catalogo del dataset.",
                hints=["resumen general de measure axis", "measure axis por group axis"],
                meta={"kind": "assistant_message"},
            )
        ],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_shape_dataset["id"], "question": "que tipo de analisis puedes generar"},
        headers={"X-User-Id": "usr_help"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "assistant_message"
    assert body["meta"]["kind"] == "assistant_message"
    assert len(body["hints"]) >= 2
    assert body["telemetry"]["llm_totals"]["call_count"] == 1
    assert body["telemetry"]["llm_totals"]["input_cost_mxn"] > 0


def test_ambiguous_query_returns_clarification(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="clarification",
                question="Necesito saber si quieres agrupar o ver un total general.",
                reason="La consulta es ambigua.",
                hints=["resumen general de measure axis", "measure axis por group axis"],
                meta={"kind": "clarification"},
            )
        ],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_shape_dataset["id"], "question": "analiza esto"},
        headers={"X-User-Id": "usr_clarify"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "needs_clarification"
    assert body["meta"]["kind"] == "clarification"
    assert body["telemetry"]["llm_totals"]["call_count"] == 1
    assert body["telemetry"]["llm_totals"]["total_cost_mxn"] > 0


def test_invalid_metric_from_model_becomes_clarification(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    metrics=["missing_metric_sum"],
                    confidence=0.95,
                ),
            )
        ],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_shape_dataset["id"], "question": "usa una metrica inexistente"},
        headers={"X-User-Id": "usr_invalid"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "needs_clarification"
    assert body["meta"]["kind"] == "metric"


def test_format_error_from_gemini_returns_generic_assistant_message(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            GeminiResponseFormatError("respuesta invalida"),
            GeminiResponseFormatError("respuesta invalida"),
        ],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_shape_dataset["id"], "question": "orientame"},
        headers={"X-User-Id": "usr_format"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "assistant_message"
    assert body["reason"]


def test_patch_labels_regenerates_catalog_and_invalidates_cache(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    dataset_id = uploaded_shape_dataset["id"]
    initial_version = uploaded_shape_dataset["catalog_version"]

    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["group_axis"],
                    metrics=["measure_axis_sum"],
                    sort=SortSpec(field="measure_axis_sum", order="desc"),
                    visualization="bar",
                    confidence=0.96,
                ),
            )
        ],
        texts=["Resumen inicial."],
    )
    first = client.post(
        "/query",
        json={"dataset_id": dataset_id, "question": "ventas por grupo"},
        headers={"X-User-Id": "usr_patch_labels"},
    )
    assert first.status_code == 200, first.text
    first_body = first.json()
    assert first_body["meta"]["cached"] is False
    assert first_body["kpis"][0]["label"] == "Total de Measure Axis"

    patch = client.patch(
        f"/datasets/{dataset_id}/labels",
        json={"column_labels": {"measure_axis": "Ventas Netas", "group_axis": "Sucursal"}},
    )
    assert patch.status_code == 200, patch.text
    patched = patch.json()
    assert patched["catalog_version"] != initial_version
    assert patched["columns"]["measure_axis"]["label"] == "Ventas Netas"

    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["group_axis"],
                    metrics=["measure_axis_sum"],
                    sort=SortSpec(field="measure_axis_sum", order="desc"),
                    visualization="bar",
                    confidence=0.96,
                ),
            )
        ],
        texts=["Resumen actualizado."],
    )
    second = client.post(
        "/query",
        json={"dataset_id": dataset_id, "question": "ventas por grupo"},
        headers={"X-User-Id": "usr_patch_labels"},
    )
    assert second.status_code == 200, second.text
    second_body = second.json()
    assert second_body["meta"]["cached"] is False
    assert second_body["kpis"][0]["label"] == "Total de Ventas Netas"
    assert second_body["table"]["columns"] == ["Sucursal", "Total de Ventas Netas"]


def test_day_of_week_dimension_is_available_and_sorted(client: TestClient, app, uploaded_weekday_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["time_axis_day_of_week"],
                    metrics=["measure_axis_sum"],
                    visualization="bar",
                    confidence=0.95,
                ),
            )
        ],
        texts=["Resumen por dia."],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_weekday_dataset["id"], "question": "ventas por dia de la semana"},
        headers={"X-User-Id": "usr_weekdays"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert [row[0] for row in body["table"]["rows"]] == [
        "Lunes",
        "Martes",
        "Miercoles",
        "Jueves",
        "Viernes",
        "Sabado",
        "Domingo",
    ]
    assert [row[1] for row in body["table"]["rows"]] == [30.75, 30.5, 40.5, 60.25, 50.75, 80.5, 70.25]


def test_query_can_filter_on_day_of_week_dimension(client: TestClient, app, uploaded_weekday_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["time_axis_day_of_week"],
                    metrics=["measure_axis_sum"],
                    filters=[QueryFilter(field="time_axis_day_of_week", op="in", value=["lunes", "viernes"])],
                    visualization="bar",
                    confidence=0.95,
                ),
            )
        ],
        texts=["Resumen filtrado."],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_weekday_dataset["id"], "question": "solo lunes y viernes"},
        headers={"X-User-Id": "usr_weekdays_filter"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert [row[0] for row in body["table"]["rows"]] == ["Lunes", "Viernes"]
    assert [row[1] for row in body["table"]["rows"]] == [30.75, 50.75]


def test_query_rejects_unsupported_operator_for_derived_dimension(client: TestClient, app, uploaded_weekday_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["time_axis_day_of_week"],
                    metrics=["measure_axis_sum"],
                    filters=[QueryFilter(field="time_axis_day_of_week", op="between", value=["Lunes", "Viernes"])],
                    visualization="bar",
                    confidence=0.95,
                ),
            )
        ],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_weekday_dataset["id"], "question": "between lunes y viernes"},
        headers={"X-User-Id": "usr_weekdays_invalid"},
    )
    assert response.status_code == 422
    payload = response.json()
    assert "solo soportan eq e in" in payload["detail"]
    assert payload["telemetry"]["status"] == "validation_error"
    assert payload["telemetry"]["llm_totals"]["call_count"] == 1
    assert payload["telemetry"]["llm_totals"]["total_cost_mxn"] > 0


def test_old_catalogs_refresh_labels_and_day_of_week(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    catalog_path = app.state.settings.catalogs_dir / f"{uploaded_shape_dataset['id']}.json"
    payload = json.loads(catalog_path.read_text(encoding="utf-8"))
    payload["columns"]["time_axis"]["label"] = ""
    payload["columns"]["group_axis"].pop("label", None)
    payload["columns"]["measure_axis"]["label"] = None
    payload["dimension_definitions"].pop("time_axis_day_of_week", None)
    payload.pop("manual_aliases", None)
    payload.pop("label_overrides", None)
    catalog_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    response = client.get("/datasets")
    assert response.status_code == 200, response.text
    refreshed = response.json()[0]

    assert refreshed["columns"]["time_axis"]["label"] == "Time Axis"
    catalog = app.state.dataset_profiler.get_catalog(uploaded_shape_dataset["id"])
    assert catalog is not None
    assert "time_axis_day_of_week" in catalog.dimension_definitions
    assert catalog.dimension_definitions["time_axis_day_of_week"].order_expression is not None


def test_gemini_structured_schema_does_not_include_additional_properties() -> None:
    schema = StructuredAgentDecision.model_json_schema()
    assert "additionalProperties" not in json.dumps(schema, ensure_ascii=True)


def test_rate_limit_is_enforced(tmp_path) -> None:
    app = create_app(build_settings(tmp_path, rate_limit_requests=1))
    install_fake_gemini(app)

    with TestClient(app) as client:
        upload = client.post(
            "/datasets/upload",
            files={"file": ("shape.csv", "time_axis,group_axis,measure_axis\n2026-01-01,alpha,100\n", "text/csv")},
        )
        dataset_id = upload.json()["id"]
        queue_fake_gemini(
            app,
            structured=[
                AgentDecision(
                    kind="query",
                    plan=QueryPlan(intent="aggregate_report", metrics=["measure_axis_sum"], confidence=0.95),
                )
            ],
            texts=["Resumen de prueba."],
        )

        first = client.post(
            "/query",
            json={"dataset_id": dataset_id, "question": "resumen general"},
            headers={"X-User-Id": "usr_limit"},
        )
        assert first.status_code == 200

        second = client.post(
            "/query",
            json={"dataset_id": dataset_id, "question": "resumen general"},
            headers={"X-User-Id": "usr_limit"},
        )
        assert second.status_code == 429
        assert second.json()["telemetry"]["status"] == "rate_limited"


def test_query_fails_when_gemini_is_not_configured(tmp_path) -> None:
    settings = build_settings(tmp_path)
    settings.gemini_api_key = None
    settings.allow_local_gemini_fallback = False
    app = create_app(settings)
    with TestClient(app) as client:
        upload = client.post(
            "/datasets/upload",
            files={"file": ("shape.csv", "time_axis,group_axis,measure_axis\n2026-01-01,alpha,100\n", "text/csv")},
        )
        dataset_id = upload.json()["id"]

        response = client.post(
            "/query",
            json={"dataset_id": dataset_id, "question": "resumen general"},
            headers={"X-User-Id": "usr_no_gemini"},
        )
        assert response.status_code == 503
        payload = response.json()
        assert "AGENT_GEMINI_API_KEY" in payload["detail"]
        assert payload["telemetry"]["status"] == "gemini_unavailable"


def test_metrics_endpoints_return_raw_and_summary(client: TestClient, app, uploaded_shape_dataset: dict) -> None:
    queue_fake_gemini(
        app,
        structured=[
            AgentDecision(
                kind="query",
                plan=QueryPlan(
                    intent="aggregate_report",
                    dimensions=["group_axis"],
                    metrics=["measure_axis_sum"],
                    sort=SortSpec(field="measure_axis_sum", order="desc"),
                    visualization="bar",
                    confidence=0.96,
                ),
            )
        ],
        texts=["Resumen con telemetria."],
    )

    response = client.post(
        "/query",
        json={"dataset_id": uploaded_shape_dataset["id"], "question": "ventas por grupo"},
        headers={"X-User-Id": "usr_metrics"},
    )
    assert response.status_code == 200

    raw = client.get("/metrics/queries", params={"dataset_id": uploaded_shape_dataset["id"], "user_id": "usr_metrics"})
    assert raw.status_code == 200, raw.text
    raw_payload = raw.json()
    assert raw_payload["total"] == 1
    assert raw_payload["items"][0]["query_id"]
    assert raw_payload["items"][0]["llm_totals"]["call_count"] == 2
    assert raw_payload["items"][0]["llm_totals"]["input_token_count"] == 210
    assert raw_payload["items"][0]["llm_totals"]["total_cost_mxn"] == 0.0006698
    assert len(raw_payload["items"][0]["llm_calls"]) == 2

    summary = client.get("/metrics/summary", params={"dataset_id": uploaded_shape_dataset["id"]})
    assert summary.status_code == 200, summary.text
    summary_payload = summary.json()
    assert summary_payload["query_count"] >= 1
    assert summary_payload["total_input_token_count"] >= 210
    assert summary_payload["total_token_count"] >= 256
    assert summary_payload["total_cost_mxn"] >= 0.0006698
    assert any(item["model"].startswith("gemini-2.5-flash") for item in summary_payload["by_model"])
    assert any(item["stage"] == "intent" for item in summary_payload["by_stage"])

    timeseries = client.get("/metrics/timeseries", params={"dataset_id": uploaded_shape_dataset["id"]})
    assert timeseries.status_code == 200, timeseries.text
    timeseries_payload = timeseries.json()
    assert len(timeseries_payload["items"]) == 1
    assert timeseries_payload["items"][0]["input_token_count"] == 210
    assert timeseries_payload["items"][0]["total_cost_mxn"] == 0.0006698


def test_audit_logger_migrates_legacy_schema(tmp_path) -> None:
    settings = build_settings(tmp_path)
    settings.ensure_directories()
    with sqlite3.connect(settings.audit_db_path) as connection:
        connection.execute(
            """
            CREATE TABLE query_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                user_id TEXT NOT NULL,
                dataset_id TEXT NOT NULL,
                question TEXT NOT NULL,
                status TEXT NOT NULL,
                intent_parsed TEXT,
                validation_passed INTEGER NOT NULL,
                columns_used TEXT,
                execution_ms INTEGER,
                response_summary TEXT,
                error_message TEXT
            )
            """
        )
        connection.commit()

    app = create_app(settings)
    with sqlite3.connect(settings.audit_db_path) as connection:
        audit_columns = {row[1] for row in connection.execute("PRAGMA table_info(query_audit)").fetchall()}
        llm_tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}

    assert {"query_id", "cache_hit", "total_latency_ms", "stages", "llm_totals"}.issubset(audit_columns)
    assert "query_llm_calls" in llm_tables
    assert "fx_rates" in llm_tables
