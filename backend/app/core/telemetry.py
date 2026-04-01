from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from math import ceil

from backend.app.models.telemetry import FXRateRecord, LLMCallTelemetry, LLMUsageTotals, QueryTelemetry, TelemetryStages


@dataclass(frozen=True)
class ModelPricing:
    input_per_million: float
    output_per_million: float
    cached_per_million: float


def resolve_model_pricing(model: str, *, prompt_token_count: int) -> ModelPricing:
    normalized = model.lower()
    if normalized.startswith("gemini-2.5-flash-lite"):
        return ModelPricing(input_per_million=0.10, output_per_million=0.40, cached_per_million=0.01)
    if normalized.startswith("gemini-2.5-flash"):
        return ModelPricing(input_per_million=0.30, output_per_million=2.50, cached_per_million=0.03)
    if normalized.startswith("gemini-2.5-pro"):
        if prompt_token_count > 200_000:
            return ModelPricing(input_per_million=2.50, output_per_million=15.00, cached_per_million=0.25)
        return ModelPricing(input_per_million=1.25, output_per_million=10.00, cached_per_million=0.125)
    return ModelPricing(input_per_million=0.0, output_per_million=0.0, cached_per_million=0.0)


def _round_cost(value: float) -> float:
    return round(value, 8)


def _cost_components(
    *,
    model: str,
    prompt_token_count: int,
    output_token_count: int,
    thoughts_token_count: int,
    tool_use_prompt_token_count: int,
    cached_content_token_count: int,
    fx_record: FXRateRecord | None,
) -> dict[str, float | int | str | date | None]:
    pricing = resolve_model_pricing(model, prompt_token_count=prompt_token_count)
    input_token_count = prompt_token_count + tool_use_prompt_token_count
    thinking_token_count = thoughts_token_count

    input_cost_usd = (input_token_count * pricing.input_per_million) / 1_000_000
    output_cost_usd = (output_token_count * pricing.output_per_million) / 1_000_000
    thinking_cost_usd = (thinking_token_count * pricing.output_per_million) / 1_000_000
    cached_cost_usd = (cached_content_token_count * pricing.cached_per_million) / 1_000_000
    total_cost_usd = input_cost_usd + output_cost_usd + thinking_cost_usd + cached_cost_usd

    usd_to_mxn_rate = fx_record.usd_to_mxn_rate if fx_record is not None else 0.0
    fx_date = fx_record.fx_date if fx_record is not None else None
    fx_source = fx_record.fx_source if fx_record is not None else None

    return {
        "input_token_count": input_token_count,
        "thinking_token_count": thinking_token_count,
        "input_cost_usd": _round_cost(input_cost_usd),
        "output_cost_usd": _round_cost(output_cost_usd),
        "thinking_cost_usd": _round_cost(thinking_cost_usd),
        "cached_cost_usd": _round_cost(cached_cost_usd),
        "total_cost_usd": _round_cost(total_cost_usd),
        "input_cost_mxn": _round_cost(input_cost_usd * usd_to_mxn_rate),
        "output_cost_mxn": _round_cost(output_cost_usd * usd_to_mxn_rate),
        "thinking_cost_mxn": _round_cost(thinking_cost_usd * usd_to_mxn_rate),
        "cached_cost_mxn": _round_cost(cached_cost_usd * usd_to_mxn_rate),
        "total_cost_mxn": _round_cost(total_cost_usd * usd_to_mxn_rate),
        "usd_to_mxn_rate": usd_to_mxn_rate,
        "fx_date": fx_date,
        "fx_source": fx_source,
        "estimated_cost_usd": _round_cost(total_cost_usd),
    }


def enrich_llm_call(call: LLMCallTelemetry, fx_record: FXRateRecord | None = None) -> LLMCallTelemetry:
    return call.model_copy(
        update=_cost_components(
            model=call.model,
            prompt_token_count=call.prompt_token_count,
            output_token_count=call.output_token_count,
            thoughts_token_count=call.thoughts_token_count,
            tool_use_prompt_token_count=call.tool_use_prompt_token_count,
            cached_content_token_count=call.cached_content_token_count,
            fx_record=fx_record,
        )
    )


def build_usage_totals_from_calls(
    calls: list[LLMCallTelemetry],
    *,
    fx_record: FXRateRecord | None = None,
) -> LLMUsageTotals:
    enriched_calls = [enrich_llm_call(call, fx_record) for call in calls]
    return LLMUsageTotals(
        call_count=len(enriched_calls),
        models=sorted({call.model for call in enriched_calls}),
        prompt_token_count=sum(call.prompt_token_count for call in enriched_calls),
        output_token_count=sum(call.output_token_count for call in enriched_calls),
        thoughts_token_count=sum(call.thoughts_token_count for call in enriched_calls),
        tool_use_prompt_token_count=sum(call.tool_use_prompt_token_count for call in enriched_calls),
        cached_content_token_count=sum(call.cached_content_token_count for call in enriched_calls),
        input_token_count=sum(call.input_token_count for call in enriched_calls),
        thinking_token_count=sum(call.thinking_token_count for call in enriched_calls),
        total_token_count=sum(call.total_token_count for call in enriched_calls),
        input_cost_usd=_round_cost(sum(call.input_cost_usd for call in enriched_calls)),
        output_cost_usd=_round_cost(sum(call.output_cost_usd for call in enriched_calls)),
        thinking_cost_usd=_round_cost(sum(call.thinking_cost_usd for call in enriched_calls)),
        cached_cost_usd=_round_cost(sum(call.cached_cost_usd for call in enriched_calls)),
        total_cost_usd=_round_cost(sum(call.total_cost_usd for call in enriched_calls)),
        input_cost_mxn=_round_cost(sum(call.input_cost_mxn for call in enriched_calls)),
        output_cost_mxn=_round_cost(sum(call.output_cost_mxn for call in enriched_calls)),
        thinking_cost_mxn=_round_cost(sum(call.thinking_cost_mxn for call in enriched_calls)),
        cached_cost_mxn=_round_cost(sum(call.cached_cost_mxn for call in enriched_calls)),
        total_cost_mxn=_round_cost(sum(call.total_cost_mxn for call in enriched_calls)),
        usd_to_mxn_rate=fx_record.usd_to_mxn_rate if fx_record is not None else 0.0,
        fx_date=fx_record.fx_date if fx_record is not None else None,
        fx_source=fx_record.fx_source if fx_record is not None else None,
        estimated_cost_usd=_round_cost(sum(call.total_cost_usd for call in enriched_calls)),
    )


def materialize_usage_totals(
    raw_totals: LLMUsageTotals,
    *,
    llm_calls: list[LLMCallTelemetry],
    fx_record: FXRateRecord | None = None,
) -> tuple[list[LLMCallTelemetry], LLMUsageTotals]:
    if llm_calls:
        enriched_calls = [enrich_llm_call(call, fx_record) for call in llm_calls]
        return enriched_calls, build_usage_totals_from_calls(enriched_calls, fx_record=fx_record)

    input_token_count = raw_totals.input_token_count or (raw_totals.prompt_token_count + raw_totals.tool_use_prompt_token_count)
    thinking_token_count = raw_totals.thinking_token_count or raw_totals.thoughts_token_count
    total_cost_usd = raw_totals.total_cost_usd or raw_totals.estimated_cost_usd
    usd_to_mxn_rate = fx_record.usd_to_mxn_rate if fx_record is not None else raw_totals.usd_to_mxn_rate
    total_cost_mxn = raw_totals.total_cost_mxn or _round_cost(total_cost_usd * usd_to_mxn_rate)

    totals = raw_totals.model_copy(
        update={
            "input_token_count": input_token_count,
            "thinking_token_count": thinking_token_count,
            "total_cost_usd": _round_cost(total_cost_usd),
            "total_cost_mxn": total_cost_mxn,
            "usd_to_mxn_rate": usd_to_mxn_rate,
            "fx_date": (fx_record.fx_date if fx_record is not None else raw_totals.fx_date),
            "fx_source": (fx_record.fx_source if fx_record is not None else raw_totals.fx_source),
            "estimated_cost_usd": _round_cost(total_cost_usd),
        }
    )
    return [], totals


def estimate_call_cost_usd(call: LLMCallTelemetry) -> float:
    return float(
        _cost_components(
            model=call.model,
            prompt_token_count=call.prompt_token_count,
            output_token_count=call.output_token_count,
            thoughts_token_count=call.thoughts_token_count,
            tool_use_prompt_token_count=call.tool_use_prompt_token_count,
            cached_content_token_count=call.cached_content_token_count,
            fx_record=None,
        )["total_cost_usd"]
    )


class QueryTelemetryCollector:
    def __init__(self, *, fx_resolver: object | None = None) -> None:
        self.query_id = str(uuid.uuid4())
        self.timestamp = datetime.now(timezone.utc)
        self.status = "ok"
        self.cache_hit = False
        self.stages = TelemetryStages()
        self.llm_calls: list[LLMCallTelemetry] = []
        self.fx_resolver = fx_resolver
        self._started_at = time.perf_counter()

    @contextmanager
    def stage_timer(self, stage_name: str):
        started_at = time.perf_counter()
        try:
            yield
        finally:
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            current = getattr(self.stages, stage_name, 0)
            setattr(self.stages, stage_name, current + elapsed_ms)

    def set_status(self, status: str) -> None:
        self.status = status

    def mark_cache_hit(self) -> None:
        self.cache_hit = True

    def add_llm_call(self, call: LLMCallTelemetry) -> None:
        self.llm_calls.append(call)

    def total_latency_ms(self) -> int:
        return int((time.perf_counter() - self._started_at) * 1000)

    def build(self) -> QueryTelemetry:
        fx_record = self._resolve_fx_record() if self.llm_calls else None
        enriched_calls = [enrich_llm_call(call, fx_record) for call in self.llm_calls]
        totals = build_usage_totals_from_calls(enriched_calls, fx_record=fx_record)
        return QueryTelemetry(
            query_id=self.query_id,
            timestamp=self.timestamp,
            status=self.status,
            cache_hit=self.cache_hit,
            total_latency_ms=self.total_latency_ms(),
            stages=self.stages,
            llm_totals=totals,
            llm_calls=enriched_calls,
        )

    def _resolve_fx_record(self) -> FXRateRecord | None:
        if self.fx_resolver is None:
            return None
        resolve = getattr(self.fx_resolver, "resolve", None)
        if resolve is None:
            return None
        return resolve(self.timestamp.date())


def compute_p95(values: list[int]) -> int:
    if not values:
        return 0
    sorted_values = sorted(values)
    index = max(0, ceil(len(sorted_values) * 0.95) - 1)
    return sorted_values[index]
