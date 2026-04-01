from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.app.core.utils import humanize_identifier, jsonable_value, percent_change
from backend.app.models.dataset import DatasetCatalog
from backend.app.models.intent import QueryPlan
from backend.app.models.response import ChartData, ChartSeries, KPI, PivotTableData, TableData
from backend.app.services.query_executor import QueryExecutionResult


class BuiltResponse(BaseModel):
    kpis: list[KPI]
    table: TableData
    chart: ChartData
    highlights: list[str] = Field(default_factory=list)
    columns_used: list[str] = Field(default_factory=list)


class ResponseBuilder:
    def build(self, *, catalog: DatasetCatalog, plan: QueryPlan, execution: QueryExecutionResult) -> BuiltResponse:
        kpis = self._build_kpis(catalog=catalog, metrics=plan.metrics, execution=execution)
        table = self._build_table(catalog=catalog, plan=plan, rows=execution.rows)
        chart = self._build_chart(catalog=catalog, plan=plan, rows=execution.rows, kpis=kpis)
        highlights = self._build_highlights(catalog=catalog, plan=plan, execution=execution)
        columns_used = [*plan.dimensions, *plan.metrics, *(filter_spec.field for filter_spec in plan.filters)]
        return BuiltResponse(
            kpis=kpis,
            table=table,
            chart=chart,
            highlights=highlights,
            columns_used=list(dict.fromkeys(columns_used)),
        )

    def _build_kpis(self, *, catalog: DatasetCatalog, metrics: list[str], execution: QueryExecutionResult) -> list[KPI]:
        kpis: list[KPI] = []
        for metric_name in metrics:
            metric = catalog.metrics_index[metric_name]
            current_value = execution.current_totals.get(metric_name)
            previous_value = execution.previous_totals.get(metric_name) if execution.previous_totals else None
            change, direction = percent_change(current_value, previous_value)
            kpis.append(
                KPI(
                    label=metric.label,
                    value=jsonable_value(current_value),
                    change=change,
                    direction=direction,
                )
            )
        return kpis

    def _build_table(self, *, catalog: DatasetCatalog, plan: QueryPlan, rows: list[dict[str, Any]]) -> TableData:
        ordered_columns = [*plan.dimensions, *plan.metrics]
        labels = []
        for column_name in ordered_columns:
            if column_name in catalog.dimension_definitions:
                labels.append(catalog.dimension_definitions[column_name].label)
            elif column_name in catalog.metrics_index:
                labels.append(catalog.metrics_index[column_name].label)
            elif column_name in catalog.columns:
                labels.append(catalog.columns[column_name].label or humanize_identifier(column_name))
            else:
                labels.append(humanize_identifier(column_name))

        table_rows: list[list[Any]] = []
        for row in rows:
            table_rows.append([jsonable_value(row.get(column_name)) for column_name in ordered_columns])

        return TableData(columns=labels, rows=table_rows)

    def _build_chart(
        self,
        *,
        catalog: DatasetCatalog,
        plan: QueryPlan,
        rows: list[dict[str, Any]],
        kpis: list[KPI],
    ) -> ChartData:
        # Use plan.visualization if set; otherwise infer from intent
        viz = plan.visualization
        if not viz:
            viz = "line" if plan.intent == "time_series_report" else "bar"

        # Handle pivot_table separately
        if viz == "pivot_table" and len(plan.dimensions) >= 2 and plan.metrics:
            return self._build_pivot_chart(catalog=catalog, plan=plan, rows=rows)

        if rows and plan.dimensions:
            x_dimension = plan.dimensions[0]
            x_values = [jsonable_value(row.get(x_dimension)) for row in rows]

            if viz == "scatter" and len(plan.metrics) >= 2:
                # x = first metric, y = second metric, label = dimension
                series = [
                    ChartSeries(
                        name=catalog.metrics_index[plan.metrics[1]].label,
                        data=[jsonable_value(row.get(plan.metrics[1])) for row in rows],
                    )
                ]
                x_values = [jsonable_value(row.get(plan.metrics[0])) for row in rows]
                return ChartData(type="scatter", x=x_values, series=series)

            series = [
                ChartSeries(
                    name=catalog.metrics_index[metric_name].label if metric_name in catalog.metrics_index else metric_name,
                    data=[jsonable_value(row.get(metric_name)) for row in rows],
                )
                for metric_name in plan.metrics
            ]
            return ChartData(type=viz, x=x_values, series=series)

        # Fallback: KPI-based chart
        return ChartData(
            type="bar",
            x=[kpi.label for kpi in kpis],
            series=[ChartSeries(name="Valor", data=[kpi.value for kpi in kpis])],
        )

    def _build_pivot_chart(self, *, catalog: DatasetCatalog, plan: QueryPlan, rows: list[dict[str, Any]]) -> ChartData:
        row_dim = plan.dimensions[0]
        col_dim = plan.dimensions[1]
        metric = plan.metrics[0]
        metric_label = catalog.metrics_index[metric].label if metric in catalog.metrics_index else metric

        row_values = list(dict.fromkeys(jsonable_value(r.get(row_dim)) for r in rows))
        col_values = list(dict.fromkeys(jsonable_value(r.get(col_dim)) for r in rows))

        # Build lookup
        lookup: dict[tuple, Any] = {}
        for r in rows:
            key = (jsonable_value(r.get(row_dim)), jsonable_value(r.get(col_dim)))
            lookup[key] = jsonable_value(r.get(metric))

        data: list[list[Any]] = []
        row_totals: list[Any] = []
        for rv in row_values:
            row_data = [lookup.get((rv, cv)) for cv in col_values]
            data.append(row_data)
            numeric = [v for v in row_data if v is not None]
            row_totals.append(sum(numeric) if numeric else None)

        col_totals: list[Any] = []
        for cv in col_values:
            col_data = [lookup.get((rv, cv)) for rv in row_values]
            numeric = [v for v in col_data if v is not None]
            col_totals.append(sum(numeric) if numeric else None)

        all_vals = [v for v in row_totals if v is not None]
        grand_total = sum(all_vals) if all_vals else None

        row_dim_label = catalog.dimension_definitions[row_dim].label if row_dim in catalog.dimension_definitions else row_dim
        col_dim_label = catalog.dimension_definitions[col_dim].label if col_dim in catalog.dimension_definitions else col_dim

        return ChartData(
            type="pivot_table",
            pivot=PivotTableData(
                row_dimension=row_dim_label,
                col_dimension=col_dim_label,
                metric=metric_label,
                rows=row_values,
                cols=col_values,
                data=data,
                row_totals=row_totals,
                col_totals=col_totals,
                grand_total=grand_total,
            ),
        )

    def _build_highlights(self, *, catalog: DatasetCatalog, plan: QueryPlan, execution: QueryExecutionResult) -> list[str]:
        highlights: list[str] = []
        if execution.comparison_applied and execution.previous_totals:
            for metric_name in plan.metrics:
                current = execution.current_totals.get(metric_name)
                previous = execution.previous_totals.get(metric_name)
                change, _ = percent_change(current, previous)
                if change:
                    highlights.append(f"{catalog.metrics_index[metric_name].label} {change} vs periodo anterior")

        if execution.rows and plan.dimensions and plan.metrics:
            top_row = execution.rows[0]
            first_dimension = plan.dimensions[0]
            first_metric = plan.metrics[0]
            if first_dimension in catalog.dimension_definitions and first_metric in catalog.metrics_index:
                highlights.append(
                    f"{catalog.dimension_definitions[first_dimension].label} lider: {top_row.get(first_dimension)} "
                    f"con {top_row.get(first_metric)} en {catalog.metrics_index[first_metric].label.lower()}"
                )

        return highlights
