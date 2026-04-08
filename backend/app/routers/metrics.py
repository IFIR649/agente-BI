from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Query, Request

from backend.app.models.telemetry import (
    QueryMetricsListResponse,
    QueryMetricsSummaryResponse,
    QueryMetricsTimeseriesResponse,
)


router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/queries", response_model=QueryMetricsListResponse)
async def list_query_metrics(
    request: Request,
    dataset_id: str | None = None,
    user_id: str | None = None,
    session_token: str | None = None,
    client_id: str | None = None,
    app_session_id: str | None = None,
    status: str | None = None,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    cache_hit: bool | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> QueryMetricsListResponse:
    items, total = request.app.state.audit_logger.list_queries(
        dataset_id=dataset_id,
        user_id=user_id,
        session_token=session_token,
        client_id=client_id,
        app_session_id=app_session_id,
        status=status,
        date_from=from_,
        date_to=to,
        cache_hit=cache_hit,
        limit=limit,
        offset=offset,
    )
    return QueryMetricsListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/summary", response_model=QueryMetricsSummaryResponse)
async def summarize_query_metrics(
    request: Request,
    dataset_id: str | None = None,
    user_id: str | None = None,
    session_token: str | None = None,
    client_id: str | None = None,
    app_session_id: str | None = None,
    status: str | None = None,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    cache_hit: bool | None = None,
) -> QueryMetricsSummaryResponse:
    return request.app.state.audit_logger.summarize_queries(
        dataset_id=dataset_id,
        user_id=user_id,
        session_token=session_token,
        client_id=client_id,
        app_session_id=app_session_id,
        status=status,
        date_from=from_,
        date_to=to,
        cache_hit=cache_hit,
    )


@router.get("/timeseries", response_model=QueryMetricsTimeseriesResponse)
async def timeseries_query_metrics(
    request: Request,
    dataset_id: str | None = None,
    user_id: str | None = None,
    session_token: str | None = None,
    client_id: str | None = None,
    app_session_id: str | None = None,
    status: str | None = None,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    cache_hit: bool | None = None,
) -> QueryMetricsTimeseriesResponse:
    return request.app.state.audit_logger.timeseries_queries(
        dataset_id=dataset_id,
        user_id=user_id,
        session_token=session_token,
        client_id=client_id,
        app_session_id=app_session_id,
        status=status,
        date_from=from_,
        date_to=to,
        cache_hit=cache_hit,
    )
