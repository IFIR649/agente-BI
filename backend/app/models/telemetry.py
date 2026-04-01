from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


QueryTelemetryStatus = Literal[
    "ok",
    "assistant_message",
    "needs_clarification",
    "validation_error",
    "rate_limited",
    "gemini_error",
    "gemini_unavailable",
    "not_found",
    "error",
]

LLMCallStage = Literal["intent", "summary"]


class TelemetryStages(BaseModel):
    cache_lookup_ms: int = 0
    intent_ms: int = 0
    query_execution_ms: int = 0
    response_build_ms: int = 0
    summary_ms: int = 0


class FXRateRecord(BaseModel):
    fx_date: date
    usd_to_mxn_rate: float = 0.0
    fx_source: str = "Banxico FIX"


class LLMUsageTotals(BaseModel):
    call_count: int = 0
    models: list[str] = Field(default_factory=list)
    prompt_token_count: int = 0
    output_token_count: int = 0
    thoughts_token_count: int = 0
    tool_use_prompt_token_count: int = 0
    cached_content_token_count: int = 0
    input_token_count: int = 0
    thinking_token_count: int = 0
    total_token_count: int = 0
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    thinking_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    input_cost_mxn: float = 0.0
    output_cost_mxn: float = 0.0
    thinking_cost_mxn: float = 0.0
    cached_cost_mxn: float = 0.0
    total_cost_mxn: float = 0.0
    usd_to_mxn_rate: float = 0.0
    fx_date: date | None = None
    fx_source: str | None = None
    estimated_cost_usd: float = 0.0


class LLMCallTelemetry(BaseModel):
    stage: LLMCallStage
    model: str
    latency_ms: int = 0
    prompt_token_count: int = 0
    output_token_count: int = 0
    thoughts_token_count: int = 0
    tool_use_prompt_token_count: int = 0
    cached_content_token_count: int = 0
    input_token_count: int = 0
    thinking_token_count: int = 0
    total_token_count: int = 0
    input_cost_usd: float = 0.0
    output_cost_usd: float = 0.0
    thinking_cost_usd: float = 0.0
    cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    input_cost_mxn: float = 0.0
    output_cost_mxn: float = 0.0
    thinking_cost_mxn: float = 0.0
    cached_cost_mxn: float = 0.0
    total_cost_mxn: float = 0.0
    usd_to_mxn_rate: float = 0.0
    fx_date: date | None = None
    fx_source: str | None = None
    estimated_cost_usd: float = 0.0
    status: str = "ok"


class QueryTelemetry(BaseModel):
    query_id: str
    timestamp: datetime
    status: QueryTelemetryStatus
    cache_hit: bool = False
    total_latency_ms: int = 0
    stages: TelemetryStages = Field(default_factory=TelemetryStages)
    llm_totals: LLMUsageTotals = Field(default_factory=LLMUsageTotals)
    llm_calls: list[LLMCallTelemetry] = Field(default_factory=list)


class QueryAuditRecord(BaseModel):
    query_id: str
    timestamp: datetime
    user_id: str
    dataset_id: str
    question: str
    status: str
    cache_hit: bool = False
    validation_passed: bool
    columns_used: list[str] = Field(default_factory=list)
    execution_ms: int | None = None
    total_latency_ms: int | None = None
    stages: TelemetryStages = Field(default_factory=TelemetryStages)
    llm_totals: LLMUsageTotals = Field(default_factory=LLMUsageTotals)
    llm_calls: list[LLMCallTelemetry] = Field(default_factory=list)
    response_summary: str | None = None
    error_message: str | None = None


class QueryMetricsListResponse(BaseModel):
    items: list[QueryAuditRecord] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0


class StatusBreakdownItem(BaseModel):
    status: str
    query_count: int = 0
    total_token_count: int = 0
    total_cost_mxn: float = 0.0
    total_estimated_cost_usd: float = 0.0


class ModelBreakdownItem(BaseModel):
    model: str
    call_count: int = 0
    total_token_count: int = 0
    total_cost_mxn: float = 0.0
    total_estimated_cost_usd: float = 0.0


class StageBreakdownItem(BaseModel):
    stage: str
    call_count: int = 0
    avg_latency_ms: float = 0.0
    total_token_count: int = 0
    total_cost_mxn: float = 0.0
    total_estimated_cost_usd: float = 0.0


class QueryMetricsSummaryResponse(BaseModel):
    query_count: int = 0
    cache_hit_count: int = 0
    error_count: int = 0
    avg_total_latency_ms: float = 0.0
    p95_total_latency_ms: int = 0
    total_prompt_token_count: int = 0
    total_output_token_count: int = 0
    total_thoughts_token_count: int = 0
    total_tool_use_prompt_token_count: int = 0
    total_cached_content_token_count: int = 0
    total_input_token_count: int = 0
    total_thinking_token_count: int = 0
    total_token_count: int = 0
    total_input_cost_usd: float = 0.0
    total_output_cost_usd: float = 0.0
    total_thinking_cost_usd: float = 0.0
    total_cached_cost_usd: float = 0.0
    total_cost_usd: float = 0.0
    total_input_cost_mxn: float = 0.0
    total_output_cost_mxn: float = 0.0
    total_thinking_cost_mxn: float = 0.0
    total_cached_cost_mxn: float = 0.0
    total_cost_mxn: float = 0.0
    total_estimated_cost_usd: float = 0.0
    by_status: list[StatusBreakdownItem] = Field(default_factory=list)
    by_model: list[ModelBreakdownItem] = Field(default_factory=list)
    by_stage: list[StageBreakdownItem] = Field(default_factory=list)


class QueryMetricsTimeseriesItem(BaseModel):
    date: date
    query_count: int = 0
    input_token_count: int = 0
    output_token_count: int = 0
    thinking_token_count: int = 0
    total_token_count: int = 0
    input_cost_mxn: float = 0.0
    output_cost_mxn: float = 0.0
    thinking_cost_mxn: float = 0.0
    total_cost_mxn: float = 0.0


class QueryMetricsTimeseriesResponse(BaseModel):
    items: list[QueryMetricsTimeseriesItem] = Field(default_factory=list)
