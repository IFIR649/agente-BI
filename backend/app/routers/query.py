from __future__ import annotations

from fastapi import APIRouter, Header, Request, status
from fastapi.responses import JSONResponse

from backend.app.core.telemetry import QueryTelemetryCollector
from backend.app.core.utils import build_cache_key
from backend.app.core.gemini_client import GeminiClientError, GeminiUnavailableError
from backend.app.models.intent import QueryRequest
from backend.app.models.response import (
    AssistantMessageResponse,
    ClarificationResponse,
    QueryErrorResponse,
    QuerySuccessResponse,
    ResponseMeta,
)
from backend.app.services.errors import ClarificationNeeded, PlanValidationError


router = APIRouter(tags=["query"])


@router.post(
    "/query",
    response_model=QuerySuccessResponse | ClarificationResponse | AssistantMessageResponse,
    responses={
        404: {"model": QueryErrorResponse},
        422: {"model": QueryErrorResponse},
        429: {"model": QueryErrorResponse},
        502: {"model": QueryErrorResponse},
        503: {"model": QueryErrorResponse},
        500: {"model": QueryErrorResponse},
    },
)
async def query_dataset(
    payload: QueryRequest,
    request: Request,
    x_user_id: str | None = Header(default=None),
) -> QuerySuccessResponse | ClarificationResponse | AssistantMessageResponse | JSONResponse:
    collector = QueryTelemetryCollector(fx_resolver=request.app.state.fx_resolver)
    user_id = x_user_id or f"anonymous:{request.client.host if request.client else 'unknown'}"

    allowed, retry_after = request.app.state.rate_limiter.check(user_id)
    if not allowed:
        return _error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_429_TOO_MANY_REQUESTS,
            error_status="rate_limited",
            detail="Se excedio el limite de consultas.",
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
            extra_headers={"Retry-After": str(retry_after)},
        )

    catalog = request.app.state.dataset_profiler.get_catalog(payload.dataset_id)
    if catalog is None:
        return _error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_404_NOT_FOUND,
            error_status="not_found",
            detail="Dataset no encontrado.",
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
        )

    history_context = " | ".join(f"{turn.role}:{turn.text}" for turn in payload.history[-8:])
    with collector.stage_timer("cache_lookup_ms"):
        cache_key = build_cache_key(payload.dataset_id, payload.question, catalog.catalog_version, history_context)
        cached = request.app.state.cache.get(cache_key)
    if cached is not None:
        cached_response = QuerySuccessResponse.model_validate(cached)
        cached_response.meta.cached = True
        collector.mark_cache_hit()
        collector.set_status("ok")
        telemetry = collector.build()
        cached_response.meta.execution_ms = telemetry.total_latency_ms
        cached_response.telemetry = telemetry
        request.app.state.audit_logger.log(
            query_id=telemetry.query_id,
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
            status="ok",
            validation_passed=True,
            telemetry=telemetry,
            intent_parsed=cached_response.meta.plan.model_dump(mode="json"),
            columns_used=cached_response.meta.columns_used,
            execution_ms=telemetry.total_latency_ms,
            response_summary=cached_response.summary,
        )
        return cached_response

    try:
        with collector.stage_timer("intent_ms"):
            decision = request.app.state.intent_parser.parse(
                question=payload.question,
                catalog=catalog,
                history=payload.history,
                telemetry_collector=collector,
            )

        if decision.kind == "assistant_message":
            collector.set_status("assistant_message")
            telemetry = collector.build()
            response = AssistantMessageResponse(
                message=decision.message or "",
                reason=decision.reason or "Mensaje asistivo generado desde el catalogo.",
                hints=decision.hints,
                meta=decision.meta,
                telemetry=telemetry,
            )
            request.app.state.audit_logger.log(
                query_id=telemetry.query_id,
                user_id=user_id,
                dataset_id=payload.dataset_id,
                question=payload.question,
                status="assistant_message",
                validation_passed=False,
                telemetry=telemetry,
                execution_ms=telemetry.total_latency_ms,
                response_summary=decision.message,
            )
            return response

        if decision.kind == "clarification":
            collector.set_status("needs_clarification")
            telemetry = collector.build()
            response = ClarificationResponse(
                question=decision.question or "Necesito un poco mas de contexto.",
                reason=decision.reason or "La consulta requiere una aclaracion.",
                hints=decision.hints,
                meta=decision.meta,
                telemetry=telemetry,
            )
            request.app.state.audit_logger.log(
                query_id=telemetry.query_id,
                user_id=user_id,
                dataset_id=payload.dataset_id,
                question=payload.question,
                status="needs_clarification",
                validation_passed=False,
                telemetry=telemetry,
                execution_ms=telemetry.total_latency_ms,
                error_message=decision.reason,
            )
            return response

        plan = decision.plan
        if plan is None:
            return _error_response(
                request=request,
                collector=collector,
                http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_status="error",
                detail="El parser no devolvio un plan ejecutable.",
                user_id=user_id,
                dataset_id=payload.dataset_id,
                question=payload.question,
            )

        with collector.stage_timer("query_execution_ms"):
            execution = request.app.state.query_executor.execute(catalog=catalog, plan=plan)
        with collector.stage_timer("response_build_ms"):
            built = request.app.state.response_builder.build(catalog=catalog, plan=plan, execution=execution)
        with collector.stage_timer("summary_ms"):
            summary = request.app.state.summary_writer.write(
                question=payload.question,
                catalog=catalog,
                kpis=built.kpis,
                highlights=built.highlights,
                plan=plan,
                telemetry_collector=collector,
            )
        collector.set_status("ok")
        telemetry = collector.build()

        response = QuerySuccessResponse(
            summary=summary,
            kpis=built.kpis,
            table=built.table,
            chart=built.chart,
            meta=ResponseMeta(
                dataset_id=payload.dataset_id,
                execution_ms=telemetry.total_latency_ms,
                cached=False,
                comparison_applied=execution.comparison_applied,
                columns_used=built.columns_used,
                row_count=len(execution.rows),
                plan=plan,
            ),
            telemetry=telemetry,
        )

        request.app.state.cache.set(cache_key, response.model_dump(mode="json", exclude={"telemetry"}))
        request.app.state.audit_logger.log(
            query_id=telemetry.query_id,
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
            status="ok",
            validation_passed=True,
            telemetry=telemetry,
            intent_parsed=plan.model_dump(mode="json"),
            columns_used=built.columns_used,
            execution_ms=telemetry.total_latency_ms,
            response_summary=summary,
        )
        return response
    except ClarificationNeeded as exc:
        collector.set_status("needs_clarification")
        telemetry = collector.build()
        response = ClarificationResponse(
            question=exc.question,
            reason=exc.reason,
            hints=exc.hints,
            meta=exc.meta,
            telemetry=telemetry,
        )
        request.app.state.audit_logger.log(
            query_id=telemetry.query_id,
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
            status="needs_clarification",
            validation_passed=False,
            telemetry=telemetry,
            execution_ms=telemetry.total_latency_ms,
            error_message=exc.reason,
        )
        return response
    except PlanValidationError as exc:
        return _error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_422_UNPROCESSABLE_CONTENT,
            error_status="validation_error",
            detail=exc.detail,
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
        )
    except GeminiUnavailableError as exc:
        return _error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_status="gemini_unavailable",
            detail=str(exc),
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
        )
    except GeminiClientError as exc:
        return _error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_502_BAD_GATEWAY,
            error_status="gemini_error",
            detail=str(exc),
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
        )
    except Exception as exc:
        return _error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_status="error",
            detail="Error interno ejecutando la consulta.",
            user_id=user_id,
            dataset_id=payload.dataset_id,
            question=payload.question,
            error_message=str(exc),
        )


def _error_response(
    *,
    request: Request,
    collector: QueryTelemetryCollector,
    http_status: int,
    error_status: str,
    detail: str,
    user_id: str,
    dataset_id: str,
    question: str,
    error_message: str | None = None,
    extra_headers: dict[str, str] | None = None,
) -> JSONResponse:
    collector.set_status(error_status)
    telemetry = collector.build()
    response = QueryErrorResponse(
        status=error_status,
        detail=detail,
        telemetry=telemetry,
    )
    request.app.state.audit_logger.log(
        query_id=telemetry.query_id,
        user_id=user_id,
        dataset_id=dataset_id,
        question=question,
        status=error_status,
        validation_passed=False,
        telemetry=telemetry,
        execution_ms=telemetry.total_latency_ms,
        error_message=error_message or detail,
    )
    return JSONResponse(
        status_code=http_status,
        content=response.model_dump(mode="json"),
        headers=extra_headers,
    )
