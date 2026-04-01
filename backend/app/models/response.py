from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.app.models.intent import QueryPlan
from backend.app.models.telemetry import QueryTelemetry


class KPI(BaseModel):
    label: str
    value: Any
    change: str | None = None
    direction: Literal["up", "down", "flat"] | None = None


class TableData(BaseModel):
    columns: list[str]
    rows: list[list[Any]] = Field(default_factory=list)


class ChartSeries(BaseModel):
    name: str
    data: list[Any] = Field(default_factory=list)


class PivotTableData(BaseModel):
    row_dimension: str
    col_dimension: str
    metric: str
    rows: list[Any] = Field(default_factory=list)
    cols: list[Any] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)
    row_totals: list[Any] = Field(default_factory=list)
    col_totals: list[Any] = Field(default_factory=list)
    grand_total: Any = None


class ChartData(BaseModel):
    type: Literal["bar", "line", "pie", "area", "scatter", "pivot_table", "table"]
    x: list[Any] = Field(default_factory=list)
    series: list[ChartSeries] = Field(default_factory=list)
    pivot: PivotTableData | None = None


class ResponseMeta(BaseModel):
    dataset_id: str
    execution_ms: int
    cached: bool = False
    comparison_applied: bool = False
    columns_used: list[str] = Field(default_factory=list)
    row_count: int = 0
    plan: QueryPlan


class QuerySuccessResponse(BaseModel):
    status: Literal["ok"] = "ok"
    summary: str
    kpis: list[KPI]
    table: TableData
    chart: ChartData
    meta: ResponseMeta
    telemetry: QueryTelemetry | None = None


class ClarificationResponse(BaseModel):
    status: Literal["needs_clarification"] = "needs_clarification"
    question: str
    reason: str
    hints: list[str] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)
    telemetry: QueryTelemetry | None = None


class AssistantMessageResponse(BaseModel):
    status: Literal["assistant_message"] = "assistant_message"
    message: str
    reason: str
    hints: list[str] = Field(default_factory=list)
    meta: dict[str, Any] = Field(default_factory=dict)
    telemetry: QueryTelemetry | None = None


class QueryErrorResponse(BaseModel):
    status: Literal[
        "validation_error",
        "rate_limited",
        "gemini_error",
        "gemini_unavailable",
        "not_found",
        "error",
    ]
    detail: str
    telemetry: QueryTelemetry
