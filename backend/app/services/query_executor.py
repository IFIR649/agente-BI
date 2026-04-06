from __future__ import annotations

import logging
from copy import deepcopy
from datetime import date, timedelta
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

from backend.app.config import Settings
from backend.app.core.database import DuckDBManager, quote_identifier
from backend.app.core.utils import jsonable_value
from backend.app.models.dataset import DatasetCatalog
from backend.app.models.intent import QueryFilter, QueryPlan


class QueryExecutionResult(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]] = Field(default_factory=list)
    current_totals: dict[str, Any] = Field(default_factory=dict)
    previous_totals: dict[str, Any] | None = None
    comparison_applied: bool = False


class QueryExecutor:
    def __init__(self, settings: Settings, db_manager: DuckDBManager) -> None:
        self.settings = settings
        self.db_manager = db_manager

    def execute(self, *, catalog: DatasetCatalog, plan: QueryPlan) -> QueryExecutionResult:
        logger.info(
            "execute dataset=%s intent=%s metrics=%s dimensions=%s",
            catalog.id, plan.intent, plan.metrics, plan.dimensions,
        )
        csv_path = catalog.resolve_csv_path(data_dir=self.settings.data_dir)
        if csv_path is None:
            raise FileNotFoundError(f"No se encontro el archivo del dataset {catalog.id}.")

        with self.db_manager.session() as connection:
            self.db_manager.register_csv_view(connection, csv_path)

            rows = self._execute_main_query(connection, catalog=catalog, plan=plan)
            current_totals = self._execute_totals_query(connection, catalog=catalog, filters=plan.filters, metrics=plan.metrics)

            previous_totals = None
            comparison_applied = False
            if plan.comparison == "previous_period":
                previous_filters = self._build_previous_period_filters(plan.filters, catalog)
                if previous_filters is not None:
                    previous_totals = self._execute_totals_query(
                        connection,
                        catalog=catalog,
                        filters=previous_filters,
                        metrics=plan.metrics,
                    )
                    comparison_applied = True

            columns = list(rows[0].keys()) if rows else list(plan.dimensions + plan.metrics)
            return QueryExecutionResult(
                columns=columns,
                rows=rows,
                current_totals=current_totals,
                previous_totals=previous_totals,
                comparison_applied=comparison_applied,
            )

    def _execute_main_query(self, connection, *, catalog: DatasetCatalog, plan: QueryPlan) -> list[dict[str, Any]]:
        select_clauses: list[str] = []
        group_by_clauses: list[str] = []

        for dimension_name in plan.dimensions:
            dimension = catalog.dimension_definitions[dimension_name]
            select_clauses.append(f"{dimension.expression} AS {quote_identifier(dimension_name)}")
            group_by_clauses.append(dimension.expression)
            if dimension.order_expression and dimension.order_expression not in group_by_clauses:
                group_by_clauses.append(dimension.order_expression)

        for metric_name in plan.metrics:
            metric = catalog.metrics_index[metric_name]
            select_clauses.append(f"{metric.formula} AS {quote_identifier(metric_name)}")

        where_sql, params = self._build_where_clause(plan.filters, catalog)

        group_by_sql = f"GROUP BY {', '.join(group_by_clauses)}" if group_by_clauses else ""
        if plan.sort:
            order_by_sql = self._build_order_by_clause(plan.sort.field, plan.sort.order, catalog)
        elif plan.dimensions and plan.intent == "time_series_report":
            order_by_sql = self._build_order_by_clause(plan.dimensions[0], "asc", catalog)
        else:
            order_by_sql = ""
        limit_sql = f"LIMIT {plan.top_n}" if plan.top_n else ""

        sql = f"""
            SELECT {', '.join(select_clauses)}
            FROM dataset_view
            {where_sql}
            {group_by_sql}
            {order_by_sql}
            {limit_sql}
        """
        cursor = connection.execute(sql, params)
        columns = [column[0] for column in cursor.description]
        return [
            {columns[index]: jsonable_value(value) for index, value in enumerate(row)}
            for row in cursor.fetchall()
        ]

    def _execute_totals_query(self, connection, *, catalog: DatasetCatalog, filters: list[QueryFilter], metrics: list[str]) -> dict[str, Any]:
        select_clauses = [
            f"{catalog.metrics_index[metric_name].formula} AS {quote_identifier(metric_name)}"
            for metric_name in metrics
        ]
        where_sql, params = self._build_where_clause(filters, catalog)
        sql = f"""
            SELECT {', '.join(select_clauses)}
            FROM dataset_view
            {where_sql}
        """
        cursor = connection.execute(sql, params)
        row = cursor.fetchone() or []
        columns = [column[0] for column in cursor.description]
        return {columns[index]: jsonable_value(value) for index, value in enumerate(row)}

    def _build_where_clause(self, filters: list[QueryFilter], catalog: DatasetCatalog | None = None) -> tuple[str, list[Any]]:
        if not filters:
            return "", []

        clauses: list[str] = []
        params: list[Any] = []
        op_map = {
            "eq": "=",
            "neq": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
        }

        for filter_spec in filters:
            field = self._resolve_filter_expression(filter_spec.field, catalog)

            if filter_spec.op in op_map:
                clauses.append(f"{field} {op_map[filter_spec.op]} ?")
                params.append(filter_spec.value)
            elif filter_spec.op == "between":
                clauses.append(f"{field} BETWEEN ? AND ?")
                params.extend(filter_spec.value)
            elif filter_spec.op == "in":
                placeholders = ", ".join(["?"] * len(filter_spec.value))
                clauses.append(f"{field} IN ({placeholders})")
                params.extend(filter_spec.value)

        return f"WHERE {' AND '.join(clauses)}", params

    def _build_order_by_clause(self, field_name: str, order: str, catalog: DatasetCatalog) -> str:
        definition = catalog.dimension_definitions.get(field_name)
        if definition is not None:
            order_expr = definition.order_expression or quote_identifier(field_name)
            return f"ORDER BY {order_expr} {order.upper()}"
        return f"ORDER BY {quote_identifier(field_name)} {order.upper()}"

    def _resolve_filter_expression(self, field_name: str, catalog: DatasetCatalog | None) -> str:
        if catalog is None:
            return quote_identifier(field_name)
        definition = catalog.dimension_definitions.get(field_name)
        if definition is not None:
            return definition.expression
        col_profile = catalog.columns.get(field_name)
        if col_profile and col_profile.detected_date_format:
            return self._build_date_field_expr(field_name, col_profile.detected_date_format)
        return quote_identifier(field_name)

    def _build_date_field_expr(self, field_name: str, fmt: str) -> str:
        sql_name = quote_identifier(field_name)
        if "%H" in fmt and "%M" in fmt:
            normalized = f"regexp_replace({sql_name}, ' (\\d):(\\d{{2}})', ' 0\\1:\\2')"
            return f"STRPTIME({normalized}, '{fmt}')"
        return f"STRPTIME({sql_name}, '{fmt}')"

    def _build_previous_period_filters(self, filters: list[QueryFilter], catalog: DatasetCatalog) -> list[QueryFilter] | None:
        for index, filter_spec in enumerate(filters):
            if filter_spec.field not in catalog.date_columns or filter_spec.op != "between":
                continue
            start = date.fromisoformat(str(filter_spec.value[0])[:10])
            end = date.fromisoformat(str(filter_spec.value[1])[:10])
            days = (end - start).days + 1
            previous_end = start - timedelta(days=1)
            previous_start = previous_end - timedelta(days=days - 1)
            previous_filters = deepcopy(filters)
            previous_filters[index] = QueryFilter(
                field=filter_spec.field,
                op="between",
                value=[previous_start.isoformat(), previous_end.isoformat()],
            )
            return previous_filters
        return None
