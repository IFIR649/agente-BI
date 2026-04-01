from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


IntentType = Literal["aggregate_report", "time_series_report"]
FilterOp = Literal["eq", "neq", "gt", "gte", "lt", "lte", "between", "in"]
VisualizationType = Literal["table", "bar", "line", "pie", "area", "scatter", "pivot_table"]
ComparisonType = Literal["previous_period"]
Granularity = Literal["day", "week", "month", "year", "day_of_week"]
ConversationRole = Literal["user", "agent", "system"]
DecisionKind = Literal["query", "clarification", "assistant_message"]


class QueryFilter(BaseModel):
    field: str
    op: FilterOp
    value: Any


class SortSpec(BaseModel):
    field: str
    order: Literal["asc", "desc"] = "desc"


class QueryPlan(BaseModel):
    intent: IntentType
    dimensions: list[str] = Field(default_factory=list)
    metrics: list[str] = Field(default_factory=list)
    filters: list[QueryFilter] = Field(default_factory=list)
    sort: SortSpec | None = None
    comparison: ComparisonType | None = None
    visualization: VisualizationType | None = None
    top_n: int | None = None
    time_granularity: Granularity | None = None
    confidence: float = 0.0
    clarification_reason: str | None = None
    clarification_question: str | None = None
    unsupported_metrics: list[str] = Field(default_factory=list)

    @field_validator("confidence")
    @classmethod
    def validate_confidence(cls, value: float) -> float:
        return max(0.0, min(1.0, value))

    @field_validator("top_n")
    @classmethod
    def validate_top_n(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value < 1:
            raise ValueError("top_n debe ser mayor o igual a 1")
        return value


class ConversationTurn(BaseModel):
    role: ConversationRole
    text: str


class QueryRequest(BaseModel):
    dataset_id: str
    question: str
    history: list[ConversationTurn] = Field(default_factory=list)


class StructuredAgentDecision(BaseModel):
    kind: DecisionKind
    plan: QueryPlan | None = None
    question: str | None = None
    message: str | None = None
    reason: str | None = None
    hints: list[str] = Field(default_factory=list)


class AgentDecision(StructuredAgentDecision):
    meta: dict[str, Any] = Field(default_factory=dict)
