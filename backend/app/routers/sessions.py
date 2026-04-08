from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from backend.app.core.auth import AuthContext, get_auth_context
from backend.app.core.session import Session
from backend.app.core.telemetry import QueryTelemetryCollector
from backend.app.core.utils import build_cache_key
from backend.app.core.gemini_client import GeminiClientError, GeminiUnavailableError
from backend.app.models.dataset import UploadMetadata
from backend.app.models.intent import ConversationTurn
from backend.app.models.response import (
    AssistantMessageResponse,
    ClarificationResponse,
    QueryErrorResponse,
    QuerySuccessResponse,
    ResponseMeta,
)
from backend.app.services.errors import ClarificationNeeded, PlanValidationError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/sessions", tags=["sessions"])


# ------------------------------------------------------------------ #
# Modelos de request/response                                          #
# ------------------------------------------------------------------ #

class CreateSessionRequest(BaseModel):
    user_id: str = ""


class CreateSessionResponse(BaseModel):
    token: str
    expires_in: int


class HeartbeatResponse(BaseModel):
    ok: bool
    expires_in: int


class DestroyResponse(BaseModel):
    ok: bool


class SessionQueryRequest(BaseModel):
    question: str
    history: list[ConversationTurn] = []


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _get_session_or_401(request: Request, token: str) -> Session:
    auth = get_auth_context(request)
    session = request.app.state.session_store.get_session(token)
    if session is None:
        raise HTTPException(status_code=401, detail="Token de sesion invalido o expirado.")
    if session.api_key_id != auth.api_key_id or session.user_id != auth.actor_user_id:
        raise HTTPException(status_code=403, detail="El token de sesion no pertenece a este usuario.")
    return session


# ------------------------------------------------------------------ #
# Endpoints                                                            #
# ------------------------------------------------------------------ #

@router.post("", response_model=CreateSessionResponse, status_code=status.HTTP_201_CREATED)
async def create_session(
    request: Request,
    body: CreateSessionRequest | None = None,
) -> CreateSessionResponse:
    """Crea una sesion y devuelve el token de acceso."""
    store = request.app.state.session_store
    settings = request.app.state.settings
    auth = get_auth_context(request)

    if store.active_count() >= settings.max_concurrent_sessions:
        raise HTTPException(
            status_code=503,
            detail=f"Se alcanzo el limite de {settings.max_concurrent_sessions} sesiones concurrentes.",
        )

    token = store.create_session(auth)
    return CreateSessionResponse(token=token, expires_in=settings.session_timeout_seconds)


@router.post("/{token}/upload", response_model=dict, status_code=status.HTTP_201_CREATED)
async def upload_csv(
    token: str,
    request: Request,
    file: UploadFile = File(...),
    metadata: str | None = Form(default=None),
) -> dict:
    """Sube un CSV y lo pre-carga en DuckDB para la sesion indicada."""
    session = _get_session_or_401(request, token)

    if session.has_dataset():
        raise HTTPException(status_code=409, detail="La sesion ya tiene un dataset cargado.")

    settings = request.app.state.settings
    filename = file.filename or "dataset.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos CSV.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="El archivo esta vacio.")

    max_size_bytes = settings.max_upload_size_mb * 1024 * 1024
    if len(content) > max_size_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"El archivo supera el limite de {settings.max_upload_size_mb} MB.",
        )

    parsed_metadata = UploadMetadata()
    if metadata:
        try:
            parsed_metadata = UploadMetadata.model_validate(json.loads(metadata))
        except Exception as exc:
            raise HTTPException(status_code=422, detail="metadata no es un JSON valido.") from exc

    # Perfila el CSV (guarda en disco, construye catalogo)
    try:
        catalog = await asyncio.to_thread(
            request.app.state.dataset_profiler.profile_and_store,
            filename=filename,
            content=content,
            metadata=parsed_metadata,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo procesar el CSV: {exc}") from exc

    # Rutas de archivos generados por el profiler
    csv_path = settings.uploads_dir / f"{catalog.id}.csv"
    catalog_path = settings.catalogs_dir / f"{catalog.id}.json"

    # Pre-carga el CSV en una conexion DuckDB persistente
    db_manager = request.app.state.db_manager
    duckdb_conn = db_manager.create_persistent_connection()
    try:
        await asyncio.to_thread(
            db_manager.load_csv_into_table,
            duckdb_conn,
            csv_path,
            "dataset",
        )
    except Exception as exc:
        duckdb_conn.close()
        csv_path.unlink(missing_ok=True)
        catalog_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"Error al cargar CSV en DuckDB: {exc}") from exc

    request.app.state.session_store.attach_dataset(
        token,
        dataset_id=catalog.id,
        catalog=catalog,
        csv_path=csv_path,
        catalog_path=catalog_path,
        duckdb_conn=duckdb_conn,
    )

    logger.info("csv loaded token=%s dataset=%s rows_approx=%s", token, catalog.id, len(content))
    return catalog.to_summary().model_dump(mode="json")


@router.post("/{token}/query",
    response_model=QuerySuccessResponse | ClarificationResponse | AssistantMessageResponse,
    responses={
        401: {"model": QueryErrorResponse},
        412: {"model": QueryErrorResponse},
        422: {"model": QueryErrorResponse},
        429: {"model": QueryErrorResponse},
        502: {"model": QueryErrorResponse},
        503: {"model": QueryErrorResponse},
        500: {"model": QueryErrorResponse},
    },
)
async def session_query(
    token: str,
    payload: SessionQueryRequest,
    request: Request,
) -> QuerySuccessResponse | ClarificationResponse | AssistantMessageResponse | JSONResponse:
    """Consulta en lenguaje natural sobre el CSV de la sesion."""
    auth = get_auth_context(request)
    session = _get_session_or_401(request, token)

    if not session.has_dataset():
        return JSONResponse(
            status_code=412,
            content={"status": "error", "detail": "La sesion no tiene un dataset cargado aun."},
        )

    collector = QueryTelemetryCollector(fx_resolver=request.app.state.fx_resolver)
    user_id = session.user_id

    allowed, retry_after = request.app.state.rate_limiter.check(user_id)
    if not allowed:
        return _session_error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_429_TOO_MANY_REQUESTS,
            error_status="rate_limited",
            detail="Se excedio el limite de consultas.",
            user_id=user_id,
            dataset_id=session.dataset_id or "",
            question=payload.question,
            extra_headers={"Retry-After": str(retry_after)},
        )

    catalog = session.catalog
    assert catalog is not None  # garantizado por has_dataset()

    history_context = " | ".join(f"{turn.role}:{turn.text}" for turn in payload.history[-8:])
    with collector.stage_timer("cache_lookup_ms"):
        cache_key = build_cache_key(catalog.id, payload.question, catalog.catalog_version, history_context)
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
            dataset_id=catalog.id,
            question=payload.question,
            status="ok",
            validation_passed=True,
            telemetry=telemetry,
            **_audit_context_kwargs(auth, session_token=token),
            intent_parsed=cached_response.meta.plan.model_dump(mode="json"),
            columns_used=cached_response.meta.columns_used,
            execution_ms=telemetry.total_latency_ms,
            response_summary=cached_response.summary,
        )
        return cached_response

    try:
        with collector.stage_timer("intent_ms"):
            decision = await asyncio.to_thread(
                request.app.state.intent_parser.parse,
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
                dataset_id=catalog.id,
                question=payload.question,
                status="assistant_message",
                validation_passed=False,
                telemetry=telemetry,
                **_audit_context_kwargs(auth, session_token=token),
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
                dataset_id=catalog.id,
                question=payload.question,
                status="needs_clarification",
                validation_passed=False,
                telemetry=telemetry,
                **_audit_context_kwargs(auth, session_token=token),
                execution_ms=telemetry.total_latency_ms,
                error_message=decision.reason,
            )
            return response

        plan = decision.plan
        if plan is None:
            return _session_error_response(
                request=request,
                collector=collector,
                http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_status="error",
                detail="El parser no devolvio un plan ejecutable.",
                user_id=user_id,
                dataset_id=catalog.id,
                question=payload.question,
            )

        # Ejecuta usando la conexion pre-cargada de la sesion (lock por thread safety)
        with collector.stage_timer("query_execution_ms"):
            with session.lock:
                execution = await asyncio.to_thread(
                    request.app.state.query_executor.execute,
                    catalog=catalog,
                    plan=plan,
                    connection=session.duckdb_conn,
                    table_name="dataset",
                )

        with collector.stage_timer("response_build_ms"):
            built = request.app.state.response_builder.build(catalog=catalog, plan=plan, execution=execution)

        with collector.stage_timer("summary_ms"):
            summary = await asyncio.to_thread(
                request.app.state.summary_writer.write,
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
                dataset_id=catalog.id,
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
            dataset_id=catalog.id,
            question=payload.question,
            status="ok",
            validation_passed=True,
            telemetry=telemetry,
            **_audit_context_kwargs(auth, session_token=token),
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
            dataset_id=catalog.id,
            question=payload.question,
            status="needs_clarification",
            validation_passed=False,
            telemetry=telemetry,
            **_audit_context_kwargs(auth, session_token=token),
            execution_ms=telemetry.total_latency_ms,
            error_message=exc.reason,
        )
        return response
    except PlanValidationError as exc:
        return _session_error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_422_UNPROCESSABLE_CONTENT,
            error_status="validation_error",
            detail=exc.detail,
            user_id=user_id,
            dataset_id=catalog.id,
            question=payload.question,
        )
    except GeminiUnavailableError as exc:
        return _session_error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_status="gemini_unavailable",
            detail=str(exc),
            user_id=user_id,
            dataset_id=catalog.id,
            question=payload.question,
        )
    except GeminiClientError as exc:
        return _session_error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_502_BAD_GATEWAY,
            error_status="gemini_error",
            detail=str(exc),
            user_id=user_id,
            dataset_id=catalog.id,
            question=payload.question,
        )
    except Exception as exc:
        logger.error("session query error token=%s: %s", token, exc)
        return _session_error_response(
            request=request,
            collector=collector,
            http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_status="error",
            detail="Error interno ejecutando la consulta.",
            user_id=user_id,
            dataset_id=catalog.id,
            question=payload.question,
            error_message=str(exc),
        )


@router.post("/{token}/heartbeat", response_model=HeartbeatResponse)
async def heartbeat(token: str, request: Request) -> HeartbeatResponse:
    """Keep-alive: el cliente llama cada 30s para evitar que la sesion expire."""
    _get_session_or_401(request, token)
    ok = request.app.state.session_store.heartbeat(token)
    if not ok:
        raise HTTPException(status_code=401, detail="Token de sesion invalido o expirado.")
    return HeartbeatResponse(ok=True, expires_in=request.app.state.settings.session_timeout_seconds)


@router.post("/{token}/logout", response_model=DestroyResponse)
@router.delete("/{token}", response_model=DestroyResponse)
async def destroy_session(token: str, request: Request) -> DestroyResponse:
    """Cierra la sesion y destruye el CSV del servidor.

    Idempotente: retorna ok=True aunque el token no exista.
    Disponible via DELETE y POST /logout para compatibilidad con sendBeacon.
    """
    session = request.app.state.session_store.get_session(token)
    if session is not None:
        _get_session_or_401(request, token)
    request.app.state.session_store.destroy_session(token)
    return DestroyResponse(ok=True)


# ------------------------------------------------------------------ #
# Helper privado                                                        #
# ------------------------------------------------------------------ #

def _session_error_response(
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
    response = QueryErrorResponse(status=error_status, detail=detail, telemetry=telemetry)
    auth = get_auth_context(request)
    request.app.state.audit_logger.log(
        query_id=telemetry.query_id,
        user_id=user_id,
        dataset_id=dataset_id,
        question=question,
        status=error_status,
        validation_passed=False,
        telemetry=telemetry,
        **_audit_context_kwargs(auth, session_token=request.path_params.get("token")),
        execution_ms=telemetry.total_latency_ms,
        error_message=error_message or detail,
    )
    return JSONResponse(
        status_code=http_status,
        content=response.model_dump(mode="json"),
        headers=extra_headers,
    )


def _audit_context_kwargs(auth: AuthContext, *, session_token: str | None) -> dict[str, object]:
    return {
        "principal_id": auth.principal_id,
        "api_key_id": auth.api_key_id,
        "actor_user_name": auth.actor_user_name,
        "client_id": auth.client_id,
        "app_session_id": auth.app_session_id,
        "session_token": session_token,
    }
