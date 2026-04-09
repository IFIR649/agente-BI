from __future__ import annotations

import asyncio
import gzip as _gzip
import json
import logging
import time
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from backend.app.core.auth import AuthContext, require_api_access
from backend.app.core.gemini_client import GeminiClientError, GeminiUnavailableError
from backend.app.core.session import Session
from backend.app.core.telemetry import QueryTelemetryCollector
from backend.app.core.utils import build_cache_key
from backend.app.models.dataset import DatasetSummary, UploadMetadata
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

router = APIRouter(prefix="/chat", tags=["chat"])


class ChatBootstrapResponse(BaseModel):
    session_token: str
    expires_in: int
    dataset: DatasetSummary
    ui_url: str


class ChatSessionResponse(BaseModel):
    session_token: str
    expires_in: int
    dataset: DatasetSummary


class ChatMessageRequest(BaseModel):
    question: str
    history: list[ConversationTurn] = Field(default_factory=list)


class ChatHeartbeatResponse(BaseModel):
    ok: bool
    expires_in: int


class ChatLogoutResponse(BaseModel):
    ok: bool


def _session_audit_context(session: Session, *, session_token: str) -> dict[str, object]:
    return {
        "principal_id": session.principal_id,
        "api_key_id": session.api_key_id,
        "actor_user_name": session.actor_user_name,
        "client_id": session.client_id,
        "app_session_id": session.app_session_id,
        "session_token": session_token,
    }


def _seconds_until_expiration(request: Request, session: Session) -> int:
    timeout = request.app.state.settings.session_timeout_seconds
    elapsed = int(time.monotonic() - session.last_heartbeat)
    return max(0, timeout - elapsed)


def _build_ui_url(request: Request, *, session_token: str, user_id: str) -> str:
    root_url = str(request.url_for("root"))
    query = urlencode({"session_token": session_token, "user_id": user_id})
    separator = "&" if "?" in root_url else "?"
    return f"{root_url}{separator}{query}"


def _resolve_token_session(
    request: Request,
    *,
    session_token: str | None,
    user_id: str | None,
) -> Session:
    if not session_token or not session_token.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Falta X-Session-Token.")
    if not user_id or not user_id.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Falta X-User-Id.")

    token = session_token.strip()
    actor_user_id = user_id.strip()
    session = request.app.state.session_store.get_session(token)
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token de sesion invalido o expirado.")

    if _seconds_until_expiration(request, session) <= 0:
        request.app.state.session_store.destroy_session(token)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token de sesion invalido o expirado.")

    if session.user_id != actor_user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="El token de sesion no pertenece a este usuario.")

    return session


async def _bootstrap_session_upload(
    request: Request,
    *,
    auth: AuthContext,
    file: UploadFile,
    metadata: str | None,
) -> tuple[str, DatasetSummary]:
    settings = request.app.state.settings
    store = request.app.state.session_store
    profile_started = time.perf_counter()

    if store.active_count() >= settings.max_concurrent_sessions:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Se alcanzo el limite de {settings.max_concurrent_sessions} sesiones concurrentes.",
        )

    filename = file.filename or "dataset.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Solo se aceptan archivos CSV.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="El archivo esta vacio.")

    if content[:2] == b'\x1f\x8b':
        try:
            content = _gzip.decompress(content)
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No se pudo descomprimir el archivo: {exc}") from exc

    max_size_bytes = settings.max_upload_size_mb * 1024 * 1024
    if len(content) > max_size_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"El archivo supera el limite de {settings.max_upload_size_mb} MB.",
        )

    parsed_metadata = UploadMetadata()
    if metadata:
        try:
            parsed_metadata = UploadMetadata.model_validate(json.loads(metadata))
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail="metadata no es un JSON valido.") from exc

    try:
        catalog = await asyncio.to_thread(
            request.app.state.dataset_profiler.profile_and_store,
            filename=filename,
            content=content,
            metadata=parsed_metadata,
            generate_labels=False,
        )
    except Exception:
        raise
    profile_elapsed_ms = int((time.perf_counter() - profile_started) * 1000)

    csv_path = settings.uploads_dir / f"{catalog.id}.csv"
    catalog_path = settings.catalogs_dir / f"{catalog.id}.json"
    db_manager = request.app.state.db_manager
    duckdb_conn = db_manager.create_persistent_connection()
    load_started = time.perf_counter()

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
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error al cargar CSV en DuckDB: {exc}") from exc
    load_elapsed_ms = int((time.perf_counter() - load_started) * 1000)

    token = store.create_session(auth)

    attached = request.app.state.session_store.attach_dataset(
        token,
        dataset_id=catalog.id,
        catalog=catalog,
        csv_path=csv_path,
        catalog_path=catalog_path,
        duckdb_conn=duckdb_conn,
    )
    if not attached:
        duckdb_conn.close()
        csv_path.unlink(missing_ok=True)
        catalog_path.unlink(missing_ok=True)
        request.app.state.session_store.destroy_session(token)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="No se pudo asociar el dataset a la sesion.")

    logger.info(
        "chat bootstrap token=%s dataset=%s user=%s profile_ms=%s load_ms=%s labels=disabled",
        token,
        catalog.id,
        auth.actor_user_id,
        profile_elapsed_ms,
        load_elapsed_ms,
    )
    return token, catalog.to_summary()


def _chat_error_response(
    *,
    request: Request,
    collector: QueryTelemetryCollector,
    session: Session,
    session_token: str,
    http_status: int,
    error_status: str,
    detail: str,
    question: str,
    error_message: str | None = None,
    extra_headers: dict[str, str] | None = None,
) -> JSONResponse:
    collector.set_status(error_status)
    telemetry = collector.build()
    response = QueryErrorResponse(status=error_status, detail=detail, telemetry=telemetry)
    request.app.state.audit_logger.log(
        query_id=telemetry.query_id,
        user_id=session.user_id,
        dataset_id=session.dataset_id or "",
        question=question,
        status=error_status,
        validation_passed=False,
        telemetry=telemetry,
        **_session_audit_context(session, session_token=session_token),
        execution_ms=telemetry.total_latency_ms,
        error_message=error_message or detail,
    )
    return JSONResponse(
        status_code=http_status,
        content=response.model_dump(mode="json"),
        headers=extra_headers,
    )


@router.post("/bootstrap", response_model=ChatBootstrapResponse, status_code=status.HTTP_201_CREATED)
async def chat_bootstrap(
    request: Request,
    auth: AuthContext = Depends(require_api_access),
    file: UploadFile = File(...),
    metadata: str | None = Form(default=None),
) -> ChatBootstrapResponse:
    try:
        token, dataset = await _bootstrap_session_upload(
            request,
            auth=auth,
            file=file,
            metadata=metadata,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"No se pudo procesar el CSV: {exc}") from exc

    return ChatBootstrapResponse(
        session_token=token,
        expires_in=request.app.state.settings.session_timeout_seconds,
        dataset=dataset,
        ui_url=_build_ui_url(request, session_token=token, user_id=auth.actor_user_id),
    )


@router.get("/session", response_model=ChatSessionResponse)
async def get_chat_session(
    request: Request,
    x_session_token: str | None = Header(default=None, alias="X-Session-Token"),
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> ChatSessionResponse:
    session = _resolve_token_session(request, session_token=x_session_token, user_id=x_user_id)
    if not session.has_dataset() or session.catalog is None:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED, detail="La sesion no tiene un dataset cargado aun.")

    return ChatSessionResponse(
        session_token=session.token,
        expires_in=_seconds_until_expiration(request, session),
        dataset=session.catalog.to_summary(),
    )


@router.post(
    "/message",
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
async def chat_message(
    request: Request,
    payload: ChatMessageRequest,
    x_session_token: str | None = Header(default=None, alias="X-Session-Token"),
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> QuerySuccessResponse | ClarificationResponse | AssistantMessageResponse | JSONResponse:
    session = _resolve_token_session(request, session_token=x_session_token, user_id=x_user_id)
    session_token = session.token
    collector = QueryTelemetryCollector(fx_resolver=request.app.state.fx_resolver)

    if not session.has_dataset() or session.catalog is None:
        return _chat_error_response(
            request=request,
            collector=collector,
            session=session,
            session_token=session_token,
            http_status=status.HTTP_412_PRECONDITION_FAILED,
            error_status="error",
            detail="La sesion no tiene un dataset cargado aun.",
            question=payload.question,
        )

    user_id = session.user_id
    allowed, retry_after = request.app.state.rate_limiter.check(user_id)
    if not allowed:
        return _chat_error_response(
            request=request,
            collector=collector,
            session=session,
            session_token=session_token,
            http_status=status.HTTP_429_TOO_MANY_REQUESTS,
            error_status="rate_limited",
            detail="Se excedio el limite de consultas.",
            question=payload.question,
            extra_headers={"Retry-After": str(retry_after)},
        )

    catalog = session.catalog
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
            **_session_audit_context(session, session_token=session_token),
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
                **_session_audit_context(session, session_token=session_token),
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
                **_session_audit_context(session, session_token=session_token),
                execution_ms=telemetry.total_latency_ms,
                error_message=decision.reason,
            )
            return response

        plan = decision.plan
        if plan is None:
            return _chat_error_response(
                request=request,
                collector=collector,
                session=session,
                session_token=session_token,
                http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
                error_status="error",
                detail="El parser no devolvio un plan ejecutable.",
                question=payload.question,
            )

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
            **_session_audit_context(session, session_token=session_token),
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
            **_session_audit_context(session, session_token=session_token),
            execution_ms=telemetry.total_latency_ms,
            error_message=exc.reason,
        )
        return response
    except PlanValidationError as exc:
        return _chat_error_response(
            request=request,
            collector=collector,
            session=session,
            session_token=session_token,
            http_status=status.HTTP_422_UNPROCESSABLE_CONTENT,
            error_status="validation_error",
            detail=exc.detail,
            question=payload.question,
        )
    except GeminiUnavailableError as exc:
        return _chat_error_response(
            request=request,
            collector=collector,
            session=session,
            session_token=session_token,
            http_status=status.HTTP_503_SERVICE_UNAVAILABLE,
            error_status="gemini_unavailable",
            detail=str(exc),
            question=payload.question,
        )
    except GeminiClientError as exc:
        return _chat_error_response(
            request=request,
            collector=collector,
            session=session,
            session_token=session_token,
            http_status=status.HTTP_502_BAD_GATEWAY,
            error_status="gemini_error",
            detail=str(exc),
            question=payload.question,
        )
    except Exception as exc:
        logger.error("chat message error token=%s: %s", session_token, exc)
        return _chat_error_response(
            request=request,
            collector=collector,
            session=session,
            session_token=session_token,
            http_status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            error_status="error",
            detail="Error interno ejecutando la consulta.",
            question=payload.question,
            error_message=str(exc),
        )


@router.post("/heartbeat", response_model=ChatHeartbeatResponse)
async def chat_heartbeat(
    request: Request,
    x_session_token: str | None = Header(default=None, alias="X-Session-Token"),
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> ChatHeartbeatResponse:
    session = _resolve_token_session(request, session_token=x_session_token, user_id=x_user_id)
    ok = request.app.state.session_store.heartbeat(session.token)
    if not ok:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token de sesion invalido o expirado.")
    return ChatHeartbeatResponse(ok=True, expires_in=request.app.state.settings.session_timeout_seconds)


@router.post("/logout", response_model=ChatLogoutResponse)
async def chat_logout(
    request: Request,
    x_session_token: str | None = Header(default=None, alias="X-Session-Token"),
    x_user_id: str | None = Header(default=None, alias="X-User-Id"),
) -> ChatLogoutResponse:
    if not x_session_token or not x_session_token.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Falta X-Session-Token.")
    if not x_user_id or not x_user_id.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Falta X-User-Id.")

    token = x_session_token.strip()
    session = request.app.state.session_store.get_session(token)
    if session is None:
        return ChatLogoutResponse(ok=True)
    if session.user_id != x_user_id.strip():
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="El token de sesion no pertenece a este usuario.")

    request.app.state.session_store.destroy_session(token)
    return ChatLogoutResponse(ok=True)
