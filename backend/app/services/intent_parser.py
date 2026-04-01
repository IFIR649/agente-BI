from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import date, datetime

from backend.app.config import Settings
from backend.app.core.gemini_client import (
    GeminiClient,
    GeminiClientError,
    GeminiResponseFormatError,
    GeminiUnavailableError,
)
from backend.app.core.telemetry import QueryTelemetryCollector
from backend.app.core.utils import humanize_identifier, normalize_weekday_name
from backend.app.models.dataset import DatasetCatalog
from backend.app.models.intent import AgentDecision, ConversationTurn, QueryFilter, QueryPlan, SortSpec, StructuredAgentDecision
from backend.app.models.telemetry import LLMCallTelemetry
from backend.app.services.errors import ClarificationNeeded, PlanValidationError


logger = logging.getLogger(__name__)


class IntentParser:
    def __init__(self, settings: Settings, gemini_client: GeminiClient) -> None:
        self.settings = settings
        self.gemini_client = gemini_client

    def parse(
        self,
        *,
        question: str,
        catalog: DatasetCatalog,
        history: list[ConversationTurn] | None = None,
        telemetry_collector: QueryTelemetryCollector | None = None,
    ) -> AgentDecision:
        if not self.gemini_client.configured:
            raise GeminiUnavailableError("Gemini no esta configurado. Define AGENT_GEMINI_API_KEY.")

        prompt = self._build_prompt(question=question, history=history or [])
        last_service_error: GeminiClientError | None = None
        format_failures = 0

        for model_name in (self.settings.gemini_flash_model, self.settings.gemini_pro_model):
            try:
                result = self.gemini_client.generate_structured_result(
                    system_instruction=self._build_system_instruction(catalog),
                    prompt=prompt,
                    response_model=StructuredAgentDecision,
                    model=model_name,
                    temperature=self.settings.gemini_temperature_intent,
                )
                if telemetry_collector is not None:
                    telemetry_collector.add_llm_call(
                        LLMCallTelemetry(
                            stage="intent",
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
                decision = AgentDecision.model_validate(result.payload.model_dump())
                logger.info("model=%s status=ok kind=%s", model_name, decision.kind)
                return self._finalize_decision(question=question, catalog=catalog, decision=decision)
            except GeminiResponseFormatError as exc:
                logger.warning("model=%s status=format_error reason=%s", model_name, exc)
                format_failures += 1
                continue
            except GeminiUnavailableError:
                raise
            except GeminiClientError as exc:
                logger.warning("model=%s status=service_error reason=%s", model_name, exc)
                last_service_error = exc
                continue

        if format_failures:
            return self._generic_assistant_message(
                catalog=catalog,
                reason="No pude estructurar una respuesta valida para esta consulta.",
            )

        if last_service_error is not None:
            raise last_service_error

        return self._generic_assistant_message(
            catalog=catalog,
            reason="No pude interpretar la consulta con el catalogo disponible.",
        )

    def _build_system_instruction(self, catalog: DatasetCatalog) -> str:
        catalog_info = {
            "dataset_id": catalog.id,
            "row_count": catalog.row_count,
            "default_date_column": catalog.default_date_column,
            "default_metric": catalog.default_metric,
            "suggested_metrics": catalog.suggested_metrics,
            "suggested_dimensions": catalog.suggested_dimensions,
            "columns": [
                {
                    "name": name,
                    "type": column.type,
                    "label": column.label,
                    "semantic_role": column.semantic_role,
                    "non_null_ratio": round(column.non_null_ratio, 3),
                    "uniqueness_ratio": round(column.uniqueness_ratio, 3),
                    "boolean_like": column.boolean_like,
                    "min": column.min_value,
                    "max": column.max_value,
                }
                for name, column in catalog.columns.items()
            ],
            "dimensions_allowed": [
                {
                    "name": name,
                    "label": definition.label,
                    "kind": definition.kind,
                    "source_column": definition.source_column,
                    "granularity": definition.granularity,
                }
                for name, definition in catalog.dimension_definitions.items()
            ],
            "metrics_allowed": [
                {
                    "name": metric.name,
                    "label": metric.label,
                    "aggregator": metric.aggregator,
                    "source_column": metric.source_column,
                }
                for metric in catalog.metrics_allowed
            ],
            "aliases": catalog.aliases,
            "sample_rows": catalog.sample_rows[:3],
        }

        return (
            "Eres el interprete de un agente generico de analisis de datos sobre CSV.\n"
            "Debes devolver exclusivamente un AgentDecision valido.\n\n"
            "TIPOS DE RESPUESTA:\n"
            "1. kind='query': cuando la solicitud puede ejecutarse como QueryPlan.\n"
            "2. kind='clarification': cuando falta informacion concreta para construir un plan seguro.\n"
            "3. kind='assistant_message': cuando el usuario pregunta por capacidades, usos posibles, o necesita orientacion no ejecutable.\n\n"
            "REGLAS:\n"
            "- Usa solo metricas, dimensiones y columnas del catalogo.\n"
            "- No inventes campos ni sinonimos fuera del catalogo.\n"
            "- Si la pregunta es meta ('que puedo analizar', 'para que sirven estos datos'), responde con assistant_message.\n"
            "- Si la pregunta es ambigua, responde con clarification y sugiere hints basados en suggested_metrics/suggested_dimensions.\n"
            "- Si la consulta es ejecutable, devuelve un QueryPlan completo y consistente.\n"
            "- Usa default_metric, suggested_metrics, suggested_dimensions y default_date_column solo como contexto, no como texto literal obligatorio.\n"
            "- QueryFilter.field puede apuntar a una columna fisica o a una dimension derivada del catalogo.\n"
            "- Para dimensiones derivadas, usa filtros eq o in. Los filtros between se reservan para columnas de fecha fisicas.\n"
            "- Si filtras por day_of_week, usa valores canonicos: Lunes, Martes, Miercoles, Jueves, Viernes, Sabado, Domingo.\n"
            "- Si no estas seguro, prefiere clarification antes que inventar un plan.\n"
            "- El QueryPlan debe incluir confidence realista. Si confidence < 0.7, probablemente deberias responder clarification.\n\n"
            f"FECHA Y HORA ACTUAL: {datetime.now().strftime('%Y-%m-%d %H:%M')} (zona horaria del servidor)\n"
            "Usa esta fecha como referencia para interpretar expresiones temporales relativas "
            "como 'mes pasado', 'esta semana', 'ayer', 'ultimo trimestre', etc.\n\n"
            f"CATALOGO:\n{json.dumps(catalog_info, ensure_ascii=True, indent=2)}"
        )

    def _build_prompt(self, *, question: str, history: list[ConversationTurn]) -> str:
        if not history:
            return question
        recent = history[-6:]
        lines = [f"{turn.role}: {turn.text}" for turn in recent if turn.role in {"user", "agent"}]
        return "\n".join(["Historial reciente:", *lines, f"Pregunta actual: {question}"])

    def _finalize_decision(self, *, question: str, catalog: DatasetCatalog, decision: AgentDecision) -> AgentDecision:
        if decision.kind == "assistant_message":
            return AgentDecision(
                kind="assistant_message",
                message=decision.message or self._build_assistant_message(catalog),
                reason=decision.reason or "Te comparto una guia util basada en la estructura del dataset.",
                hints=decision.hints or self._suggest_queries(catalog),
                meta=decision.meta or {"kind": "assistant_message"},
            )

        if decision.kind == "clarification":
            return AgentDecision(
                kind="clarification",
                question=decision.question or "Necesito un poco mas de contexto para armar la consulta.",
                reason=decision.reason or "La solicitud no fue lo bastante especifica.",
                hints=decision.hints or self._suggest_queries(catalog),
                meta=decision.meta or {"kind": "clarification"},
            )

        if decision.plan is None:
            return self._generic_clarification(
                catalog=catalog,
                reason="No se recibio un QueryPlan valido para ejecutar.",
            )

        try:
            normalized_plan = self._finalize_plan(question=question, catalog=catalog, plan=decision.plan)
            return AgentDecision(kind="query", plan=normalized_plan, meta=decision.meta)
        except ClarificationNeeded as exc:
            return AgentDecision(
                kind="clarification",
                question=exc.question,
                reason=exc.reason,
                hints=exc.hints,
                meta=exc.meta,
            )

    def _finalize_plan(self, *, question: str, catalog: DatasetCatalog, plan: QueryPlan) -> QueryPlan:
        normalized_plan = deepcopy(plan)
        normalized_plan.metrics = self._unique(normalized_plan.metrics)
        normalized_plan.dimensions = self._unique(normalized_plan.dimensions)

        if normalized_plan.unsupported_metrics:
            requested = ", ".join(normalized_plan.unsupported_metrics)
            available = self._metric_hints(catalog)
            raise ClarificationNeeded(
                question=f"No pude mapear la metrica '{requested}' a este dataset.",
                reason="La metrica solicitada no esta disponible en el catalogo actual.",
                hints=available,
                meta={"kind": "unsupported_metric", "requested": normalized_plan.unsupported_metrics},
            )

        if normalized_plan.top_n and normalized_plan.top_n > self.settings.max_top_n:
            raise PlanValidationError(f"top_n no puede ser mayor a {self.settings.max_top_n}.")

        if not normalized_plan.metrics:
            raise ClarificationNeeded(
                question="¿Que medida o conteo quieres analizar?",
                reason="El plan no incluye una metrica ejecutable.",
                hints=self._metric_hints(catalog),
                meta={"kind": "metric"},
            )

        for metric_name in normalized_plan.metrics:
            if metric_name not in catalog.metrics_index:
                raise ClarificationNeeded(
                    question=f"La metrica {metric_name} no esta disponible en este dataset.",
                    reason="Necesito una metrica valida del catalogo.",
                    hints=self._metric_hints(catalog),
                    meta={"kind": "metric"},
                )

        for dimension_name in normalized_plan.dimensions:
            if dimension_name not in catalog.dimension_definitions:
                raise ClarificationNeeded(
                    question=f"La dimension {dimension_name} no esta disponible en este dataset.",
                    reason="Necesito una dimension valida del catalogo.",
                    hints=self._dimension_hints(catalog),
                    meta={"kind": "dimension"},
                )

        if normalized_plan.time_granularity:
            normalized_plan = self._resolve_time_dimension(catalog=catalog, plan=normalized_plan)

        for filter_spec in normalized_plan.filters:
            if filter_spec.field in catalog.columns:
                self._validate_filter_range(filter_spec, catalog)
                continue
            if filter_spec.field in catalog.dimension_definitions:
                self._validate_dimension_filter(filter_spec, catalog)
                self._normalize_dimension_filter(filter_spec, catalog)
                continue
            raise PlanValidationError(f"El filtro usa un campo o dimension no disponible: {filter_spec.field}.")

        if normalized_plan.intent == "time_series_report":
            has_time_dimension = any(
                dimension_name in catalog.dimension_definitions
                and catalog.dimension_definitions[dimension_name].kind == "time_granularity"
                for dimension_name in normalized_plan.dimensions
            )
            if not has_time_dimension:
                raise ClarificationNeeded(
                    question="¿En que granularidad temporal quieres ver la evolucion?",
                    reason="Una serie temporal necesita una dimension de tiempo resoluble.",
                    hints=self._time_hints(catalog),
                    meta={"kind": "time_granularity"},
                )

        if normalized_plan.comparison == "previous_period" and not any(
            filter_spec.field in catalog.date_columns and filter_spec.op == "between" for filter_spec in normalized_plan.filters
        ):
            normalized_plan.comparison = None

        if normalized_plan.sort and normalized_plan.sort.field not in set(normalized_plan.metrics + normalized_plan.dimensions):
            normalized_plan.sort = None
        if normalized_plan.sort is None and normalized_plan.metrics:
            first_dimension = normalized_plan.dimensions[0] if normalized_plan.dimensions else None
            first_definition = catalog.dimension_definitions.get(first_dimension) if first_dimension else None
            sort_by_time_dimension = first_definition is not None and first_definition.kind == "time_granularity"
            normalized_plan.sort = SortSpec(
                field=normalized_plan.dimensions[0] if sort_by_time_dimension else normalized_plan.metrics[0],
                order="asc" if sort_by_time_dimension else "desc",
            )

        if normalized_plan.visualization is None:
            normalized_plan.visualization = "line" if normalized_plan.intent == "time_series_report" else "bar"

        if normalized_plan.confidence < 0.7:
            raise ClarificationNeeded(
                question="Necesito un poco mas de precision para ejecutar la consulta.",
                reason="La confianza del plan fue insuficiente.",
                hints=self._suggest_queries(catalog),
                meta={"kind": "low_confidence", "confidence": normalized_plan.confidence},
            )

        return normalized_plan

    def _resolve_time_dimension(self, *, catalog: DatasetCatalog, plan: QueryPlan) -> QueryPlan:
        if any(
            dimension_name in catalog.dimension_definitions
            and catalog.dimension_definitions[dimension_name].kind == "time_granularity"
            for dimension_name in plan.dimensions
        ):
            return plan

        if not catalog.default_date_column:
            raise ClarificationNeeded(
                question="No tengo una columna temporal clara para esta serie. ¿Sobre cual fecha quieres trabajar?",
                reason="El dataset no tiene una fecha principal resoluble para esta consulta.",
                hints=self._time_hints(catalog),
                meta={"kind": "date_column"},
            )

        dimension_name = f"{catalog.default_date_column}_{plan.time_granularity}"
        if dimension_name not in catalog.dimension_definitions:
            raise PlanValidationError(f"La granularidad {plan.time_granularity} no esta disponible para {catalog.default_date_column}.")

        plan.dimensions = [dimension_name] + [dimension for dimension in plan.dimensions if dimension != catalog.default_date_column]
        return plan

    def _validate_filter_range(self, filter_spec: QueryFilter, catalog: DatasetCatalog) -> None:
        profile = catalog.columns[filter_spec.field]
        if profile.type not in {"date", "datetime"}:
            return
        if not profile.min_value or not profile.max_value:
            return

        min_date = date.fromisoformat(str(profile.min_value)[:10])
        max_date = date.fromisoformat(str(profile.max_value)[:10])
        if filter_spec.op == "between":
            start = date.fromisoformat(str(filter_spec.value[0])[:10])
            end = date.fromisoformat(str(filter_spec.value[1])[:10])
            if start < min_date or end > max_date:
                raise PlanValidationError(
                    f"El filtro de fechas para {filter_spec.field} debe estar entre {min_date.isoformat()} y {max_date.isoformat()}."
                )

    def _validate_dimension_filter(self, filter_spec: QueryFilter, catalog: DatasetCatalog) -> None:
        if filter_spec.op not in {"eq", "in"}:
            raise PlanValidationError(
                f"Los filtros sobre la dimension {filter_spec.field} solo soportan eq e in."
            )
        definition = catalog.dimension_definitions[filter_spec.field]
        if definition.granularity == "day_of_week" and filter_spec.op == "in" and not isinstance(filter_spec.value, list):
            raise PlanValidationError(
                f"El filtro {filter_spec.field} debe usar una lista de dias cuando el operador es in."
            )

    def _normalize_dimension_filter(self, filter_spec: QueryFilter, catalog: DatasetCatalog) -> None:
        definition = catalog.dimension_definitions[filter_spec.field]
        if definition.granularity != "day_of_week":
            return
        if filter_spec.op == "eq":
            canonical = normalize_weekday_name(str(filter_spec.value))
            if canonical is None:
                raise PlanValidationError(
                    "Los filtros de dia de la semana deben usar valores entre Lunes y Domingo."
                )
            filter_spec.value = canonical
            return
        if filter_spec.op == "in":
            values = filter_spec.value if isinstance(filter_spec.value, list) else [filter_spec.value]
            normalized_values: list[str] = []
            for value in values:
                canonical = normalize_weekday_name(str(value))
                if canonical is None:
                    raise PlanValidationError(
                        "Los filtros de dia de la semana deben usar valores entre Lunes y Domingo."
                    )
                normalized_values.append(canonical)
            filter_spec.value = normalized_values

    def _generic_assistant_message(self, *, catalog: DatasetCatalog, reason: str) -> AgentDecision:
        return AgentDecision(
            kind="assistant_message",
            message=self._build_assistant_message(catalog),
            reason=reason,
            hints=self._suggest_queries(catalog),
            meta={"kind": "assistant_message"},
        )

    def _generic_clarification(self, *, catalog: DatasetCatalog, reason: str) -> AgentDecision:
        return AgentDecision(
            kind="clarification",
            question="Necesito un poco mas de contexto para construir una consulta valida.",
            reason=reason,
            hints=self._suggest_queries(catalog),
            meta={"kind": "clarification"},
        )

    def _build_assistant_message(self, catalog: DatasetCatalog) -> str:
        metric_label = self._metric_phrase(catalog)
        dimension_phrases = self._dimension_phrases(catalog)
        time_phrase = " ver tendencias temporales," if catalog.default_date_column else ""
        compare_phrase = f" comparar por {', '.join(dimension_phrases)}," if dimension_phrases else ""
        return (
            f"Puedo ayudarte a resumir {metric_label},{time_phrase}{compare_phrase} "
            "hacer rankings y explorar distribuciones usando la estructura real del dataset."
        ).replace(",,", ",").replace(" ,", " ").strip()

    def _suggest_queries(self, catalog: DatasetCatalog) -> list[str]:
        metric_name = catalog.default_metric or (catalog.suggested_metrics[0] if catalog.suggested_metrics else None)
        metric_label = self._metric_label(metric_name, catalog)
        suggestions: list[str] = []

        if metric_label:
            suggestions.append(f"resumen general de {metric_label}")

        if metric_label and catalog.suggested_dimensions:
            first_dimension = self._dimension_label(catalog.suggested_dimensions[0], catalog)
            suggestions.append(f"{metric_label} por {first_dimension}")
            suggestions.append(f"top 5 {first_dimension} por {metric_label}")

        if metric_label and catalog.default_date_column:
            suggestions.append(f"tendencia mensual de {metric_label}")

        return self._unique(suggestions)[:4]

    def _metric_hints(self, catalog: DatasetCatalog) -> list[str]:
        metric_names = catalog.suggested_metrics or ([catalog.default_metric] if catalog.default_metric else [])
        labels = [self._metric_label(name, catalog) for name in metric_names if self._metric_label(name, catalog)]
        if not labels:
            labels = ["conteo de registros"]
        return labels[:4]

    def _dimension_hints(self, catalog: DatasetCatalog) -> list[str]:
        labels = [self._dimension_label(name, catalog) for name in catalog.suggested_dimensions]
        return labels[:4]

    def _time_hints(self, catalog: DatasetCatalog) -> list[str]:
        if not catalog.default_date_column:
            return []
        date_label = self._column_label(catalog.default_date_column, catalog)
        return [
            f"{date_label} por dia",
            f"{date_label} por semana",
            f"{date_label} por mes",
            f"{date_label} por dia de la semana",
        ]

    def _metric_phrase(self, catalog: DatasetCatalog) -> str:
        metric_name = catalog.default_metric or (catalog.suggested_metrics[0] if catalog.suggested_metrics else None)
        return self._metric_label(metric_name, catalog) or "el dataset"

    def _metric_label(self, metric_name: str | None, catalog: DatasetCatalog) -> str | None:
        if not metric_name:
            return None
        if metric_name == "row_count":
            return "conteo de registros"
        metric = catalog.metrics_index.get(metric_name)
        if not metric:
            return humanize_identifier(metric_name).lower()
        if metric.source_column:
            return self._column_label(metric.source_column, catalog)
        return metric.label.lower()

    def _dimension_label(self, dimension_name: str, catalog: DatasetCatalog) -> str:
        definition = catalog.dimension_definitions.get(dimension_name)
        return definition.label.lower() if definition else humanize_identifier(dimension_name).lower()

    def _column_label(self, column_name: str, catalog: DatasetCatalog) -> str:
        profile = catalog.columns.get(column_name)
        if profile and profile.label:
            return profile.label.lower()
        return humanize_identifier(column_name).lower()

    def _dimension_phrases(self, catalog: DatasetCatalog) -> list[str]:
        return [self._dimension_label(name, catalog) for name in catalog.suggested_dimensions[:3]]

    def _unique(self, values: list[str]) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result
