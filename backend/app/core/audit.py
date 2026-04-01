from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.app.core.telemetry import compute_p95, materialize_usage_totals
from backend.app.models.telemetry import (
    LLMCallTelemetry,
    LLMUsageTotals,
    ModelBreakdownItem,
    QueryAuditRecord,
    QueryMetricsSummaryResponse,
    QueryMetricsTimeseriesItem,
    QueryMetricsTimeseriesResponse,
    QueryTelemetry,
    StageBreakdownItem,
    StatusBreakdownItem,
    TelemetryStages,
)


_NON_ERROR_STATUSES = {"ok", "assistant_message", "needs_clarification"}


class AuditLogger:
    def __init__(self, db_path: Path, *, fx_resolver: object | None = None) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.fx_resolver = fx_resolver
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS query_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    status TEXT NOT NULL,
                    intent_parsed TEXT,
                    validation_passed INTEGER NOT NULL,
                    columns_used TEXT,
                    execution_ms INTEGER,
                    response_summary TEXT,
                    error_message TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS query_llm_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    model TEXT NOT NULL,
                    latency_ms INTEGER,
                    prompt_token_count INTEGER,
                    output_token_count INTEGER,
                    thoughts_token_count INTEGER,
                    tool_use_prompt_token_count INTEGER,
                    cached_content_token_count INTEGER,
                    total_token_count INTEGER,
                    estimated_cost_usd REAL,
                    status TEXT NOT NULL
                )
                """
            )
            self._ensure_columns(connection, "query_audit", _QUERY_AUDIT_COLUMNS)
            self._ensure_columns(connection, "query_llm_calls", _QUERY_LLM_CALL_COLUMNS)
            self._create_indexes(connection)
            connection.commit()

    def _ensure_columns(self, connection: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
        existing = {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
        for name, sql_type in columns.items():
            if name not in existing:
                connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {sql_type}")

    def _create_indexes(self, connection: sqlite3.Connection) -> None:
        connection.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_query_audit_query_id ON query_audit(query_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_query_audit_timestamp ON query_audit(timestamp)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_query_audit_dataset_id ON query_audit(dataset_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_query_audit_user_id ON query_audit(user_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_query_audit_status ON query_audit(status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_query_llm_calls_query_id ON query_llm_calls(query_id)")

    def log(
        self,
        *,
        query_id: str,
        user_id: str,
        dataset_id: str,
        question: str,
        status: str,
        validation_passed: bool,
        telemetry: QueryTelemetry,
        intent_parsed: dict[str, Any] | None = None,
        columns_used: list[str] | None = None,
        execution_ms: int | None = None,
        response_summary: str | None = None,
        error_message: str | None = None,
    ) -> None:
        totals = telemetry.llm_totals
        payload = (
            query_id,
            datetime.now(timezone.utc).isoformat(),
            user_id,
            dataset_id,
            question,
            status,
            json.dumps(intent_parsed, ensure_ascii=True) if intent_parsed is not None else None,
            1 if validation_passed else 0,
            json.dumps(columns_used or [], ensure_ascii=True),
            execution_ms,
            response_summary,
            error_message,
            1 if telemetry.cache_hit else 0,
            telemetry.total_latency_ms,
            json.dumps(telemetry.stages.model_dump(mode="json"), ensure_ascii=True),
            json.dumps(totals.model_dump(mode="json"), ensure_ascii=True),
            totals.input_token_count,
            totals.output_token_count,
            totals.thinking_token_count,
            totals.total_token_count,
            totals.input_cost_usd,
            totals.output_cost_usd,
            totals.thinking_cost_usd,
            totals.cached_cost_usd,
            totals.total_cost_usd,
            totals.input_cost_mxn,
            totals.output_cost_mxn,
            totals.thinking_cost_mxn,
            totals.cached_cost_mxn,
            totals.total_cost_mxn,
            totals.usd_to_mxn_rate,
            totals.fx_date.isoformat() if totals.fx_date else None,
            totals.fx_source,
        )
        llm_rows = [
            (
                query_id,
                datetime.now(timezone.utc).isoformat(),
                call.stage,
                call.model,
                call.latency_ms,
                call.prompt_token_count,
                call.output_token_count,
                call.thoughts_token_count,
                call.tool_use_prompt_token_count,
                call.cached_content_token_count,
                call.input_token_count,
                call.thinking_token_count,
                call.total_token_count,
                call.input_cost_usd,
                call.output_cost_usd,
                call.thinking_cost_usd,
                call.cached_cost_usd,
                call.total_cost_usd,
                call.input_cost_mxn,
                call.output_cost_mxn,
                call.thinking_cost_mxn,
                call.cached_cost_mxn,
                call.total_cost_mxn,
                call.usd_to_mxn_rate,
                call.fx_date.isoformat() if call.fx_date else None,
                call.fx_source,
                call.estimated_cost_usd,
                call.status,
            )
            for call in telemetry.llm_calls
        ]
        with self._lock:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO query_audit (
                        query_id,
                        timestamp,
                        user_id,
                        dataset_id,
                        question,
                        status,
                        intent_parsed,
                        validation_passed,
                        columns_used,
                        execution_ms,
                        response_summary,
                        error_message,
                        cache_hit,
                        total_latency_ms,
                        stages,
                        llm_totals,
                        input_token_count,
                        output_token_count,
                        thinking_token_count,
                        total_token_count,
                        input_cost_usd,
                        output_cost_usd,
                        thinking_cost_usd,
                        cached_cost_usd,
                        total_cost_usd,
                        input_cost_mxn,
                        output_cost_mxn,
                        thinking_cost_mxn,
                        cached_cost_mxn,
                        total_cost_mxn,
                        usd_to_mxn_rate,
                        fx_date,
                        fx_source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                if llm_rows:
                    connection.executemany(
                        """
                        INSERT INTO query_llm_calls (
                            query_id,
                            created_at,
                            stage,
                            model,
                            latency_ms,
                            prompt_token_count,
                            output_token_count,
                            thoughts_token_count,
                            tool_use_prompt_token_count,
                            cached_content_token_count,
                            input_token_count,
                            thinking_token_count,
                            total_token_count,
                            input_cost_usd,
                            output_cost_usd,
                            thinking_cost_usd,
                            cached_cost_usd,
                            total_cost_usd,
                            input_cost_mxn,
                            output_cost_mxn,
                            thinking_cost_mxn,
                            cached_cost_mxn,
                            total_cost_mxn,
                            usd_to_mxn_rate,
                            fx_date,
                            fx_source,
                            estimated_cost_usd,
                            status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        llm_rows,
                    )
                connection.commit()

    def list_queries(
        self,
        *,
        dataset_id: str | None = None,
        user_id: str | None = None,
        status: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        cache_hit: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[QueryAuditRecord], int]:
        where_sql, params = self._build_filters(
            dataset_id=dataset_id,
            user_id=user_id,
            status=status,
            date_from=date_from,
            date_to=date_to,
            cache_hit=cache_hit,
        )
        with self._connect() as connection:
            total = int(connection.execute(f"SELECT COUNT(*) FROM query_audit {where_sql}", params).fetchone()[0])
            rows = connection.execute(
                f"""
                SELECT
                    query_id,
                    timestamp,
                    user_id,
                    dataset_id,
                    question,
                    status,
                    cache_hit,
                    validation_passed,
                    columns_used,
                    execution_ms,
                    total_latency_ms,
                    stages,
                    llm_totals,
                    response_summary,
                    error_message
                FROM query_audit
                {where_sql}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
                """,
                [*params, limit, offset],
            ).fetchall()
            llm_calls_by_query = self._fetch_llm_calls(connection, [row["query_id"] for row in rows if row["query_id"]])
        return [self._row_to_audit_record(row, llm_calls_by_query.get(row["query_id"], [])) for row in rows], total

    def summarize_queries(
        self,
        *,
        dataset_id: str | None = None,
        user_id: str | None = None,
        status: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        cache_hit: bool | None = None,
    ) -> QueryMetricsSummaryResponse:
        records, _ = self.list_queries(
            dataset_id=dataset_id,
            user_id=user_id,
            status=status,
            date_from=date_from,
            date_to=date_to,
            cache_hit=cache_hit,
            limit=1_000_000,
            offset=0,
        )
        latencies = [record.total_latency_ms or 0 for record in records if record.total_latency_ms is not None]
        by_status: dict[str, StatusBreakdownItem] = {}
        by_model: dict[str, ModelBreakdownItem] = {}
        by_stage: dict[str, dict[str, float | int | str]] = {}

        for record in records:
            status_bucket = by_status.setdefault(record.status, StatusBreakdownItem(status=record.status))
            status_bucket.query_count += 1
            status_bucket.total_token_count += record.llm_totals.total_token_count
            status_bucket.total_cost_mxn = round(status_bucket.total_cost_mxn + record.llm_totals.total_cost_mxn, 8)
            status_bucket.total_estimated_cost_usd = round(
                status_bucket.total_estimated_cost_usd + record.llm_totals.estimated_cost_usd,
                8,
            )

            for call in record.llm_calls:
                model_bucket = by_model.setdefault(call.model, ModelBreakdownItem(model=call.model))
                model_bucket.call_count += 1
                model_bucket.total_token_count += call.total_token_count
                model_bucket.total_cost_mxn = round(model_bucket.total_cost_mxn + call.total_cost_mxn, 8)
                model_bucket.total_estimated_cost_usd = round(
                    model_bucket.total_estimated_cost_usd + call.estimated_cost_usd,
                    8,
                )

                stage_bucket = by_stage.setdefault(
                    call.stage,
                    {
                        "stage": call.stage,
                        "call_count": 0,
                        "total_latency_ms": 0,
                        "total_token_count": 0,
                        "total_cost_mxn": 0.0,
                        "total_estimated_cost_usd": 0.0,
                    },
                )
                stage_bucket["call_count"] += 1
                stage_bucket["total_latency_ms"] += call.latency_ms
                stage_bucket["total_token_count"] += call.total_token_count
                stage_bucket["total_cost_mxn"] += call.total_cost_mxn
                stage_bucket["total_estimated_cost_usd"] += call.estimated_cost_usd

        return QueryMetricsSummaryResponse(
            query_count=len(records),
            cache_hit_count=sum(1 for record in records if record.cache_hit),
            error_count=sum(1 for record in records if record.status not in _NON_ERROR_STATUSES),
            avg_total_latency_ms=round(sum(latencies) / len(latencies), 2) if latencies else 0.0,
            p95_total_latency_ms=compute_p95(latencies),
            total_prompt_token_count=sum(record.llm_totals.prompt_token_count for record in records),
            total_output_token_count=sum(record.llm_totals.output_token_count for record in records),
            total_thoughts_token_count=sum(record.llm_totals.thoughts_token_count for record in records),
            total_tool_use_prompt_token_count=sum(record.llm_totals.tool_use_prompt_token_count for record in records),
            total_cached_content_token_count=sum(record.llm_totals.cached_content_token_count for record in records),
            total_input_token_count=sum(record.llm_totals.input_token_count for record in records),
            total_thinking_token_count=sum(record.llm_totals.thinking_token_count for record in records),
            total_token_count=sum(record.llm_totals.total_token_count for record in records),
            total_input_cost_usd=round(sum(record.llm_totals.input_cost_usd for record in records), 8),
            total_output_cost_usd=round(sum(record.llm_totals.output_cost_usd for record in records), 8),
            total_thinking_cost_usd=round(sum(record.llm_totals.thinking_cost_usd for record in records), 8),
            total_cached_cost_usd=round(sum(record.llm_totals.cached_cost_usd for record in records), 8),
            total_cost_usd=round(sum(record.llm_totals.total_cost_usd for record in records), 8),
            total_input_cost_mxn=round(sum(record.llm_totals.input_cost_mxn for record in records), 8),
            total_output_cost_mxn=round(sum(record.llm_totals.output_cost_mxn for record in records), 8),
            total_thinking_cost_mxn=round(sum(record.llm_totals.thinking_cost_mxn for record in records), 8),
            total_cached_cost_mxn=round(sum(record.llm_totals.cached_cost_mxn for record in records), 8),
            total_cost_mxn=round(sum(record.llm_totals.total_cost_mxn for record in records), 8),
            total_estimated_cost_usd=round(sum(record.llm_totals.estimated_cost_usd for record in records), 8),
            by_status=sorted(by_status.values(), key=lambda item: item.status),
            by_model=sorted(by_model.values(), key=lambda item: item.model),
            by_stage=[
                StageBreakdownItem(
                    stage=str(bucket["stage"]),
                    call_count=int(bucket["call_count"]),
                    avg_latency_ms=round(float(bucket["total_latency_ms"]) / int(bucket["call_count"]), 2)
                    if int(bucket["call_count"])
                    else 0.0,
                    total_token_count=int(bucket["total_token_count"]),
                    total_cost_mxn=round(float(bucket["total_cost_mxn"]), 8),
                    total_estimated_cost_usd=round(float(bucket["total_estimated_cost_usd"]), 8),
                )
                for bucket in sorted(by_stage.values(), key=lambda item: str(item["stage"]))
            ],
        )

    def timeseries_queries(
        self,
        *,
        dataset_id: str | None = None,
        user_id: str | None = None,
        status: str | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        cache_hit: bool | None = None,
    ) -> QueryMetricsTimeseriesResponse:
        records, _ = self.list_queries(
            dataset_id=dataset_id,
            user_id=user_id,
            status=status,
            date_from=date_from,
            date_to=date_to,
            cache_hit=cache_hit,
            limit=1_000_000,
            offset=0,
        )
        buckets: dict[str, dict[str, int | float | datetime]] = {}
        for record in records:
            bucket_date = record.timestamp.astimezone(timezone.utc).date()
            bucket = buckets.setdefault(
                bucket_date.isoformat(),
                {
                    "date": bucket_date,
                    "query_count": 0,
                    "input_token_count": 0,
                    "output_token_count": 0,
                    "thinking_token_count": 0,
                    "total_token_count": 0,
                    "input_cost_mxn": 0.0,
                    "output_cost_mxn": 0.0,
                    "thinking_cost_mxn": 0.0,
                    "total_cost_mxn": 0.0,
                },
            )
            bucket["query_count"] += 1
            bucket["input_token_count"] += record.llm_totals.input_token_count
            bucket["output_token_count"] += record.llm_totals.output_token_count
            bucket["thinking_token_count"] += record.llm_totals.thinking_token_count
            bucket["total_token_count"] += record.llm_totals.total_token_count
            bucket["input_cost_mxn"] += record.llm_totals.input_cost_mxn
            bucket["output_cost_mxn"] += record.llm_totals.output_cost_mxn
            bucket["thinking_cost_mxn"] += record.llm_totals.thinking_cost_mxn
            bucket["total_cost_mxn"] += record.llm_totals.total_cost_mxn

        items = [
            QueryMetricsTimeseriesItem(
                date=bucket["date"],
                query_count=int(bucket["query_count"]),
                input_token_count=int(bucket["input_token_count"]),
                output_token_count=int(bucket["output_token_count"]),
                thinking_token_count=int(bucket["thinking_token_count"]),
                total_token_count=int(bucket["total_token_count"]),
                input_cost_mxn=round(float(bucket["input_cost_mxn"]), 8),
                output_cost_mxn=round(float(bucket["output_cost_mxn"]), 8),
                thinking_cost_mxn=round(float(bucket["thinking_cost_mxn"]), 8),
                total_cost_mxn=round(float(bucket["total_cost_mxn"]), 8),
            )
            for bucket in sorted(buckets.values(), key=lambda item: item["date"])
        ]
        return QueryMetricsTimeseriesResponse(items=items)

    def _build_filters(
        self,
        *,
        dataset_id: str | None,
        user_id: str | None,
        status: str | None,
        date_from: datetime | None,
        date_to: datetime | None,
        cache_hit: bool | None,
    ) -> tuple[str, list[Any]]:
        conditions: list[str] = []
        params: list[Any] = []
        if dataset_id:
            conditions.append("dataset_id = ?")
            params.append(dataset_id)
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        if status:
            conditions.append("status = ?")
            params.append(status)
        if date_from:
            conditions.append("timestamp >= ?")
            params.append(date_from.isoformat())
        if date_to:
            conditions.append("timestamp <= ?")
            params.append(date_to.isoformat())
        if cache_hit is not None:
            conditions.append("cache_hit = ?")
            params.append(1 if cache_hit else 0)
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return where_sql, params

    def _fetch_llm_calls(
        self,
        connection: sqlite3.Connection,
        query_ids: list[str],
    ) -> dict[str, list[LLMCallTelemetry]]:
        if not query_ids:
            return {}
        placeholders = ", ".join(["?"] * len(query_ids))
        rows = connection.execute(
            f"""
            SELECT
                query_id,
                stage,
                model,
                latency_ms,
                prompt_token_count,
                output_token_count,
                thoughts_token_count,
                tool_use_prompt_token_count,
                cached_content_token_count,
                input_token_count,
                thinking_token_count,
                total_token_count,
                input_cost_usd,
                output_cost_usd,
                thinking_cost_usd,
                cached_cost_usd,
                total_cost_usd,
                input_cost_mxn,
                output_cost_mxn,
                thinking_cost_mxn,
                cached_cost_mxn,
                total_cost_mxn,
                usd_to_mxn_rate,
                fx_date,
                fx_source,
                estimated_cost_usd,
                status
            FROM query_llm_calls
            WHERE query_id IN ({placeholders})
            ORDER BY id ASC
            """,
            query_ids,
        ).fetchall()
        result: dict[str, list[LLMCallTelemetry]] = {}
        for row in rows:
            result.setdefault(row["query_id"], []).append(
                LLMCallTelemetry(
                    stage=row["stage"],
                    model=row["model"],
                    latency_ms=int(row["latency_ms"] or 0),
                    prompt_token_count=int(row["prompt_token_count"] or 0),
                    output_token_count=int(row["output_token_count"] or 0),
                    thoughts_token_count=int(row["thoughts_token_count"] or 0),
                    tool_use_prompt_token_count=int(row["tool_use_prompt_token_count"] or 0),
                    cached_content_token_count=int(row["cached_content_token_count"] or 0),
                    input_token_count=int(row["input_token_count"] or 0),
                    thinking_token_count=int(row["thinking_token_count"] or 0),
                    total_token_count=int(row["total_token_count"] or 0),
                    input_cost_usd=float(row["input_cost_usd"] or 0.0),
                    output_cost_usd=float(row["output_cost_usd"] or 0.0),
                    thinking_cost_usd=float(row["thinking_cost_usd"] or 0.0),
                    cached_cost_usd=float(row["cached_cost_usd"] or 0.0),
                    total_cost_usd=float(row["total_cost_usd"] or 0.0),
                    input_cost_mxn=float(row["input_cost_mxn"] or 0.0),
                    output_cost_mxn=float(row["output_cost_mxn"] or 0.0),
                    thinking_cost_mxn=float(row["thinking_cost_mxn"] or 0.0),
                    cached_cost_mxn=float(row["cached_cost_mxn"] or 0.0),
                    total_cost_mxn=float(row["total_cost_mxn"] or 0.0),
                    usd_to_mxn_rate=float(row["usd_to_mxn_rate"] or 0.0),
                    fx_date=datetime.fromisoformat(row["fx_date"]).date() if row["fx_date"] else None,
                    fx_source=row["fx_source"],
                    estimated_cost_usd=float(row["estimated_cost_usd"] or 0.0),
                    status=row["status"],
                )
            )
        return result

    def _row_to_audit_record(self, row: sqlite3.Row, llm_calls: list[LLMCallTelemetry]) -> QueryAuditRecord:
        columns_used = json.loads(row["columns_used"] or "[]")
        stages = TelemetryStages.model_validate(json.loads(row["stages"] or "{}"))
        raw_totals = LLMUsageTotals.model_validate(json.loads(row["llm_totals"] or "{}"))
        timestamp = datetime.fromisoformat(row["timestamp"])
        fx_record = None
        if self.fx_resolver is not None:
            resolve = getattr(self.fx_resolver, "resolve", None)
            if resolve is not None and (raw_totals.call_count or llm_calls):
                fx_record = resolve(timestamp.astimezone(timezone.utc).date())
        materialized_calls, materialized_totals = materialize_usage_totals(raw_totals, llm_calls=llm_calls, fx_record=fx_record)
        query_id = row["query_id"] or f"legacy-{row['timestamp']}-{row['user_id']}"
        return QueryAuditRecord(
            query_id=query_id,
            timestamp=timestamp,
            user_id=row["user_id"],
            dataset_id=row["dataset_id"],
            question=row["question"],
            status=row["status"],
            cache_hit=bool(row["cache_hit"]),
            validation_passed=bool(row["validation_passed"]),
            columns_used=list(columns_used),
            execution_ms=row["execution_ms"],
            total_latency_ms=row["total_latency_ms"],
            stages=stages,
            llm_totals=materialized_totals,
            llm_calls=materialized_calls,
            response_summary=row["response_summary"],
            error_message=row["error_message"],
        )


_QUERY_AUDIT_COLUMNS = {
    "query_id": "TEXT",
    "cache_hit": "INTEGER NOT NULL DEFAULT 0",
    "total_latency_ms": "INTEGER",
    "stages": "TEXT",
    "llm_totals": "TEXT",
    "input_token_count": "INTEGER",
    "output_token_count": "INTEGER",
    "thinking_token_count": "INTEGER",
    "total_token_count": "INTEGER",
    "input_cost_usd": "REAL",
    "output_cost_usd": "REAL",
    "thinking_cost_usd": "REAL",
    "cached_cost_usd": "REAL",
    "total_cost_usd": "REAL",
    "input_cost_mxn": "REAL",
    "output_cost_mxn": "REAL",
    "thinking_cost_mxn": "REAL",
    "cached_cost_mxn": "REAL",
    "total_cost_mxn": "REAL",
    "usd_to_mxn_rate": "REAL",
    "fx_date": "TEXT",
    "fx_source": "TEXT",
}

_QUERY_LLM_CALL_COLUMNS = {
    "input_token_count": "INTEGER",
    "thinking_token_count": "INTEGER",
    "input_cost_usd": "REAL",
    "output_cost_usd": "REAL",
    "thinking_cost_usd": "REAL",
    "cached_cost_usd": "REAL",
    "total_cost_usd": "REAL",
    "input_cost_mxn": "REAL",
    "output_cost_mxn": "REAL",
    "thinking_cost_mxn": "REAL",
    "cached_cost_mxn": "REAL",
    "total_cost_mxn": "REAL",
    "usd_to_mxn_rate": "REAL",
    "fx_date": "TEXT",
    "fx_source": "TEXT",
}
