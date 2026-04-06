from __future__ import annotations

from backend.app.config import Settings
from backend.app.core.telemetry import QueryTelemetryCollector
from backend.app.core.gemini_client import GeminiClient, GeminiClientError, GeminiUnavailableError
from backend.app.models.dataset import DatasetCatalog
from backend.app.models.intent import QueryPlan
from backend.app.models.response import KPI
from backend.app.models.telemetry import LLMCallTelemetry

_SYSTEM_INSTRUCTION = (
    "Eres un analista de datos que responde en espanol.\n"
    "Tu trabajo es dar una respuesta DIRECTA a la pregunta del usuario basandote en los datos calculados.\n\n"
    "REGLAS:\n"
    "1. Responde la pregunta primero usando los nombres reales de metricas o dimensiones del resultado.\n"
    "2. Agrega contexto relevante SOLO si aporta valor (cambio porcentual, lider, tendencia notable).\n"
    "3. Maximo 2 oraciones. Sin relleno, sin introducciones, sin 'Segun los datos...'.\n"
    "4. Usa numeros formateados (125,430 en lugar de 125430.55).\n"
    "5. Si hay comparacion vs periodo anterior, menciona el cambio en porcentaje.\n"
    "6. No repitas la pregunta del usuario.\n"
    "7. No inventes datos que no esten en los KPIs o highlights proporcionados.\n"
    "8. Si el intent es time_series_report, menciona la tendencia (alcista/bajista/estable) y el valor pico."
)


class SummaryWriter:
    def __init__(self, settings: Settings, gemini_client: GeminiClient) -> None:
        self.settings = settings
        self.gemini_client = gemini_client

    def write(
        self,
        *,
        question: str,
        catalog: DatasetCatalog,
        kpis: list[KPI],
        highlights: list[str],
        plan: QueryPlan | None = None,
        telemetry_collector: QueryTelemetryCollector | None = None,
    ) -> str:
        if self.gemini_client.configured:
            try:
                result = self.gemini_client.generate_text_result(
                    system_instruction=_SYSTEM_INSTRUCTION,
                    prompt=self._build_prompt(question=question, catalog=catalog, kpis=kpis, highlights=highlights, plan=plan),
                    model=self.settings.gemini_lite_model,
                    temperature=self.settings.gemini_temperature_summary,
                )
                if telemetry_collector is not None:
                    telemetry_collector.add_llm_call(
                        LLMCallTelemetry(
                            stage="summary",
                            model=result.metrics.model,
                            latency_ms=result.metrics.latency_ms,
                            prompt_token_count=result.metrics.prompt_token_count,
                            output_token_count=result.metrics.output_token_count,
                            thoughts_token_count=result.metrics.thoughts_token_count,
                            tool_use_prompt_token_count=result.metrics.tool_use_prompt_token_count,
                            cached_content_token_count=result.metrics.cached_content_token_count,
                            total_token_count=result.metrics.total_token_count,
                            status=result.metrics.status,
                        )
                    )
                return result.payload
            except (GeminiUnavailableError, GeminiClientError):
                if self.settings.allow_local_gemini_fallback:
                    return self._write_local(question=question, kpis=kpis, highlights=highlights)
                raise

        if self.settings.allow_local_gemini_fallback:
            return self._write_local(question=question, kpis=kpis, highlights=highlights)

        raise GeminiUnavailableError("Gemini no esta configurado. Define AGENT_GEMINI_API_KEY.")

    def _build_prompt(
        self,
        *,
        question: str,
        catalog: DatasetCatalog,
        kpis: list[KPI],
        highlights: list[str],
        plan: QueryPlan | None,
    ) -> str:
        lines = [f"Pregunta del usuario: {question}"]
        if plan:
            lines.append(f"Tipo de consulta: {plan.intent}")
            lines.append("Nombres legibles de metricas y dimensiones:")
            for metric_name in plan.metrics:
                metric = catalog.metrics_index.get(metric_name)
                if metric:
                    lines.append(f"  - {metric_name} -> {metric.label}")
            for dimension_name in plan.dimensions:
                definition = catalog.dimension_definitions.get(dimension_name)
                if definition:
                    lines.append(f"  - {dimension_name} -> {definition.label}")
            if plan.filters:
                filter_descriptions = []
                for f in plan.filters:
                    if f.field in catalog.dimension_definitions:
                        field_label = catalog.dimension_definitions[f.field].label
                    elif f.field in catalog.columns:
                        field_label = catalog.columns[f.field].label or f.field
                    else:
                        field_label = f.field
                    if f.op == "between" and isinstance(f.value, list):
                        filter_descriptions.append(f"{field_label} entre {f.value[0]} y {f.value[1]}")
                    else:
                        filter_descriptions.append(f"{field_label} = {f.value}")
                lines.append(f"Filtros aplicados: {', '.join(filter_descriptions)}")
        lines.append("KPIs calculados:")
        for kpi in kpis:
            change_text = f", cambio: {kpi.change}" if kpi.change else ""
            lines.append(f"  - {kpi.label}: {kpi.value}{change_text}")
        if highlights:
            lines.append("Highlights:")
            for h in highlights:
                lines.append(f"  - {h}")
        return "\n".join(lines)

    def _write_local(self, *, question: str, kpis: list[KPI], highlights: list[str]) -> str:
        parts: list[str] = []
        if kpis:
            first = kpis[0]
            if first.change:
                parts.append(f"{first.label} fue {first.value} ({first.change}).")
            else:
                parts.append(f"{first.label}: {first.value}.")
        if highlights:
            parts.append(highlights[0] + ".")
        return " ".join(parts) if parts else "No hubo resultados para resumir."
