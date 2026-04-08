const state = {
  datasets: [],
  filters: {
    datasetId: "",
    status: "",
    cacheHit: "",
    from: "",
    to: "",
    userId: "",
  },
  limit: 50,
  offset: 0,
  total: 0,
};

const STORAGE_KEYS = {
  apiKey: "csv-agent-api-key",
  userId: "csv-agent-user-id",
};

const palette = {
  input: "#166a63",
  output: "#d97a36",
  thinking: "#7a3b69",
  line: "#1f2a2c",
};

const elements = {
  apiKeyInput: document.getElementById("api-key-input"),
  actorUserIdInput: document.getElementById("actor-user-id-input"),
  authSave: document.getElementById("auth-save"),
  authStatusText: document.getElementById("auth-status-text"),
  datasetChip: document.getElementById("analytics-dataset-chip"),
  refresh: document.getElementById("analytics-refresh"),
  filterForm: document.getElementById("analytics-filter-form"),
  dataset: document.getElementById("filter-dataset"),
  status: document.getElementById("filter-status"),
  cacheHit: document.getElementById("filter-cache-hit"),
  from: document.getElementById("filter-from"),
  to: document.getElementById("filter-to"),
  userId: document.getElementById("filter-user-id"),
  statusText: document.getElementById("analytics-status-text"),
  summaryGrid: document.getElementById("analytics-summary-grid"),
  timeseriesChart: document.getElementById("analytics-timeseries-chart"),
  breakdownStatus: document.getElementById("breakdown-status"),
  breakdownModel: document.getElementById("breakdown-model"),
  breakdownStage: document.getElementById("breakdown-stage"),
  tableContainer: document.getElementById("analytics-table-container"),
  paginationText: document.getElementById("pagination-text"),
  pagePrev: document.getElementById("page-prev"),
  pageNext: document.getElementById("page-next"),
};

let userIdDebounce = null;

document.addEventListener("DOMContentLoaded", () => {
  hydrateStateFromUrl();
  bindEvents();
  bootstrap();
});

async function bootstrap() {
  renderSummaryLoading();
  renderChartLoading();
  renderTableLoading();
  syncAuthInputs();
  if (!hasApiAccess()) {
    setStatus("Captura la API key y el user id para consultar analytics.", "warn");
    setAuthStatus("Falta configurar el acceso.", "warn");
    updateDatasetChip();
    updatePaginationState(false);
    return;
  }
  setAuthStatus("Acceso listo.", "success");
  await loadDatasets();
  syncControlsFromState();
  await loadAnalyticsData();
}

function bindEvents() {
  elements.authSave?.addEventListener("click", () => {
    saveAuthInputs();
    bootstrap();
  });
  elements.refresh.addEventListener("click", () => loadAnalyticsData());
  elements.filterForm.addEventListener("submit", (event) => {
    event.preventDefault();
    applyFilters({ resetOffset: true });
  });

  for (const control of [
    elements.dataset,
    elements.status,
    elements.cacheHit,
    elements.from,
    elements.to,
  ]) {
    control.addEventListener("change", () => applyFilters({ resetOffset: true }));
  }

  for (const control of [elements.userId]) {
    control.addEventListener("input", () => {
    clearTimeout(userIdDebounce);
    userIdDebounce = window.setTimeout(() => applyFilters({ resetOffset: true }), 300);
    });
  }

  elements.pagePrev.addEventListener("click", () => {
    if (state.offset <= 0) return;
    state.offset = Math.max(0, state.offset - state.limit);
    loadAnalyticsData();
  });

  elements.pageNext.addEventListener("click", () => {
    if (state.offset + state.limit >= state.total) return;
    state.offset += state.limit;
    loadAnalyticsData();
  });
}

async function loadDatasets() {
  ensureApiAccess();
  const response = await fetch("/datasets", {
    headers: buildApiHeaders(),
  });
  const payload = await parseJsonResponse(response);
  if (!response.ok) throw new Error(payload.detail || "No se pudieron cargar los datasets.");

  state.datasets = Array.isArray(payload) ? payload : [];
  renderDatasetOptions();

  if (state.filters.datasetId && !state.datasets.some((dataset) => dataset.id === state.filters.datasetId)) {
    state.filters.datasetId = "";
    elements.dataset.value = "";
  }
  updateDatasetChip();
}

function renderDatasetOptions() {
  const currentValue = state.filters.datasetId;
  elements.dataset.innerHTML = `<option value="">Todos los datasets</option>${
    state.datasets.map((dataset) => (
      `<option value="${escapeAttr(dataset.id)}">${escapeHtml(dataset.display_name)}</option>`
    )).join("")
  }`;
  elements.dataset.value = currentValue;
}

async function loadAnalyticsData() {
  ensureApiAccess();
  syncStateFromControls();
  syncUrlFromState();
  updateDatasetChip();
  updatePaginationState(true);
  setStatus("Cargando metricas...", "warn");

  try {
    const summaryQuery = buildApiQuery({ includePagination: false });
    const queriesQuery = buildApiQuery({ includePagination: true });
    const timeseriesQuery = buildApiQuery({ includePagination: false });

    const [summaryResponse, queriesResponse, timeseriesResponse] = await Promise.all([
      fetch(`/metrics/summary?${summaryQuery.toString()}`, { headers: buildApiHeaders() }),
      fetch(`/metrics/queries?${queriesQuery.toString()}`, { headers: buildApiHeaders() }),
      fetch(`/metrics/timeseries?${timeseriesQuery.toString()}`, { headers: buildApiHeaders() }),
    ]);
    const [summaryPayload, queriesPayload, timeseriesPayload] = await Promise.all([
      parseJsonResponse(summaryResponse),
      parseJsonResponse(queriesResponse),
      parseJsonResponse(timeseriesResponse),
    ]);

    if (!summaryResponse.ok) {
      throw new Error(summaryPayload.detail || "No se pudo cargar el resumen de metricas.");
    }
    if (!queriesResponse.ok) {
      throw new Error(queriesPayload.detail || "No se pudo cargar la bitacora de consultas.");
    }
    if (!timeseriesResponse.ok) {
      throw new Error(timeseriesPayload.detail || "No se pudo cargar la serie temporal de metricas.");
    }

    state.total = Number(queriesPayload.total || 0);
    state.offset = Number(queriesPayload.offset || 0);

    renderSummary(summaryPayload);
    renderTimeseriesChart(timeseriesPayload.items || []);
    renderQueriesTable(queriesPayload.items || []);
    updatePaginationState(false);
    setStatus(
      state.total
        ? `${formatNumber(state.total)} consulta${state.total === 1 ? "" : "s"} encontradas`
        : "Sin consultas para los filtros actuales.",
      state.total ? "success" : "",
    );
  } catch (error) {
    renderSummaryError(error.message);
    renderChartError(error.message);
    renderQueriesError(error.message);
    updatePaginationState(false);
    setStatus(error.message, "error");
  }
}

function applyFilters({ resetOffset }) {
  syncStateFromControls();
  if (resetOffset) state.offset = 0;
  loadAnalyticsData();
}

function hydrateStateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  state.filters.datasetId = params.get("dataset_id") || "";
  state.filters.status = params.get("status") || "";
  state.filters.cacheHit = params.get("cache_hit") || "";
  state.filters.userId = params.get("user_id") || "";
  state.filters.from = toDateTimeLocalValue(params.get("from"));
  state.filters.to = toDateTimeLocalValue(params.get("to"));
  state.offset = Math.max(0, Number(params.get("offset") || 0));
}

function syncControlsFromState() {
  elements.dataset.value = state.filters.datasetId;
  elements.status.value = state.filters.status;
  elements.cacheHit.value = state.filters.cacheHit;
  elements.userId.value = state.filters.userId;
  elements.from.value = state.filters.from;
  elements.to.value = state.filters.to;
}

function syncStateFromControls() {
  state.filters.datasetId = elements.dataset.value.trim();
  state.filters.status = elements.status.value.trim();
  state.filters.cacheHit = elements.cacheHit.value.trim();
  state.filters.userId = elements.userId.value.trim();
  state.filters.from = elements.from.value;
  state.filters.to = elements.to.value;
}

function syncUrlFromState() {
  const params = new URLSearchParams();
  if (state.filters.datasetId) params.set("dataset_id", state.filters.datasetId);
  if (state.filters.status) params.set("status", state.filters.status);
  if (state.filters.cacheHit) params.set("cache_hit", state.filters.cacheHit);
  if (state.filters.userId) params.set("user_id", state.filters.userId);

  const fromIso = toIsoDateTime(state.filters.from);
  const toIso = toIsoDateTime(state.filters.to);
  if (fromIso) params.set("from", fromIso);
  if (toIso) params.set("to", toIso);
  if (state.offset > 0) params.set("offset", String(state.offset));

  const query = params.toString();
  const nextUrl = query ? `/analytics?${query}` : "/analytics";
  window.history.replaceState({}, "", nextUrl);
}

function buildApiQuery({ includePagination }) {
  const params = new URLSearchParams();
  if (state.filters.datasetId) params.set("dataset_id", state.filters.datasetId);
  if (state.filters.status) params.set("status", state.filters.status);
  if (state.filters.cacheHit) params.set("cache_hit", state.filters.cacheHit);
  if (state.filters.userId) params.set("user_id", state.filters.userId);

  const fromIso = toIsoDateTime(state.filters.from);
  const toIso = toIsoDateTime(state.filters.to);
  if (fromIso) params.set("from", fromIso);
  if (toIso) params.set("to", toIso);

  if (includePagination) {
    params.set("limit", String(state.limit));
    params.set("offset", String(state.offset));
  }
  return params;
}

function updateDatasetChip() {
  const dataset = state.datasets.find((item) => item.id === state.filters.datasetId);
  elements.datasetChip.textContent = dataset ? dataset.display_name : "Todos los datasets";
}

function renderSummary(summary) {
  const cards = [
    ["Consultas", formatNumber(summary.query_count || 0)],
    ["Tokens entrada", formatNumber(summary.total_input_token_count || 0)],
    ["Tokens salida", formatNumber(summary.total_output_token_count || 0)],
    ["Tokens thinking", formatNumber(summary.total_thinking_token_count || 0)],
    ["Tokens totales", formatNumber(summary.total_token_count || 0)],
    ["Costo entrada", formatCurrencyMxn(summary.total_input_cost_mxn || 0)],
    ["Costo thinking", formatCurrencyMxn(summary.total_thinking_cost_mxn || 0)],
    ["Costo total", formatCurrencyMxn(summary.total_cost_mxn || 0)],
    ["Latencia p95", `${formatNumber(summary.p95_total_latency_ms || 0)} ms`],
  ];

  elements.summaryGrid.innerHTML = cards.map(([label, value]) => `
    <article class="card analytics-kpi-card">
      <p class="eyebrow">${escapeHtml(label)}</p>
      <strong class="analytics-kpi-value">${escapeHtml(value)}</strong>
    </article>
  `).join("");

  elements.breakdownStatus.innerHTML = renderBreakdownTable(
    ["Status", "Consultas", "Tokens", "Total MXN"],
    (summary.by_status || []).map((item) => [
      item.status,
      formatNumber(item.query_count),
      formatNumber(item.total_token_count),
      formatCurrencyMxn(item.total_cost_mxn),
    ]),
    "Sin registros por status."
  );

  elements.breakdownModel.innerHTML = renderBreakdownTable(
    ["Modelo", "Calls", "Tokens", "Total MXN"],
    (summary.by_model || []).map((item) => [
      item.model,
      formatNumber(item.call_count),
      formatNumber(item.total_token_count),
      formatCurrencyMxn(item.total_cost_mxn),
    ]),
    "Sin registros por modelo."
  );

  elements.breakdownStage.innerHTML = renderBreakdownTable(
    ["Etapa", "Calls", "Latencia prom.", "Total MXN"],
    (summary.by_stage || []).map((item) => [
      item.stage,
      formatNumber(item.call_count),
      `${formatDecimal(item.avg_latency_ms)} ms`,
      formatCurrencyMxn(item.total_cost_mxn),
    ]),
    "Sin registros por etapa."
  );
}

function renderTimeseriesChart(items) {
  if (!items.length) {
    elements.timeseriesChart.innerHTML = `
      <div class="empty-state">
        <h3>Sin serie temporal</h3>
        <p>No hay consultas para construir la progresion de costos.</p>
      </div>`;
    return;
  }

  const width = Math.max(960, items.length * 66);
  const height = 340;
  const margin = { top: 24, right: 70, bottom: 78, left: 60 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const dayWidth = innerWidth / items.length;
  const barWidth = Math.max(14, dayWidth * 0.56);

  const dailyMax = Math.max(...items.map((item) => (
    Number(item.input_cost_mxn || 0) + Number(item.output_cost_mxn || 0) + Number(item.thinking_cost_mxn || 0)
  )), 1);
  let runningTotal = 0;
  const cumulativeValues = items.map((item) => {
    runningTotal += Number(item.total_cost_mxn || 0);
    return runningTotal;
  });
  const cumulativeMax = Math.max(...cumulativeValues, 1);
  const leftTicks = buildTicks(dailyMax);
  const rightTicks = buildTicks(cumulativeMax);

  const barX = (index) => margin.left + index * dayWidth + (dayWidth - barWidth) / 2;
  const yLeft = (value) => margin.top + innerHeight - ((value / dailyMax) * innerHeight);
  const yRight = (value) => margin.top + innerHeight - ((value / cumulativeMax) * innerHeight);

  let svg = `<svg class="chart-svg analytics-chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica diaria de costos en MXN">`;

  leftTicks.forEach((tick) => {
    const y = yLeft(tick);
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
    svg += `<text x="${margin.left - 8}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5f6d70">${escapeHtml(shortMoney(tick))}</text>`;
  });
  rightTicks.forEach((tick) => {
    const y = yRight(tick);
    svg += `<text x="${width - margin.right + 8}" y="${y + 4}" font-size="10" fill="#5f6d70">${escapeHtml(shortMoney(tick))}</text>`;
  });

  items.forEach((item, index) => {
    const x = barX(index);
    const stack = [
      { key: "input_cost_mxn", color: palette.input },
      { key: "output_cost_mxn", color: palette.output },
      { key: "thinking_cost_mxn", color: palette.thinking },
    ];

    let accumulated = 0;
    stack.forEach((segment) => {
      const value = Number(item[segment.key] || 0);
      if (value <= 0) return;
      const yTop = yLeft(accumulated + value);
      const yBottom = yLeft(accumulated);
      svg += `<rect x="${x}" y="${yTop}" width="${barWidth}" height="${Math.max(2, yBottom - yTop)}" rx="6" fill="${segment.color}">
        <title>${escapeHtml(formatShortDate(item.date))} · ${escapeHtml(segment.key.replaceAll("_", " "))}: ${escapeHtml(formatCurrencyMxn(value))}</title>
      </rect>`;
      accumulated += value;
    });

    const lineY = yRight(cumulativeValues[index]);
    svg += `<circle cx="${x + barWidth / 2}" cy="${lineY}" r="4" fill="${palette.line}" />`;
    svg += `<text x="${x + barWidth / 2}" y="${height - 26}" text-anchor="middle" font-size="10" fill="#5f6d70">${escapeHtml(shortDateLabel(item.date))}</text>`;
  });

  const linePoints = items.map((item, index) => `${barX(index) + barWidth / 2},${yRight(cumulativeValues[index])}`).join(" ");
  svg += `<polyline fill="none" stroke="${palette.line}" stroke-width="2.5" points="${linePoints}" />`;

  const legendItems = [
    ["Entrada", palette.input],
    ["Salida", palette.output],
    ["Thinking", palette.thinking],
    ["Acumulado", palette.line],
  ];
  legendItems.forEach(([label, color], index) => {
    const lx = margin.left + index * 150;
    const ly = height - 8;
    const marker = label === "Acumulado"
      ? `<line x1="${lx}" y1="${ly - 4}" x2="${lx + 14}" y2="${ly - 4}" stroke="${color}" stroke-width="2.5" />`
      : `<rect x="${lx}" y="${ly - 12}" width="12" height="12" rx="4" fill="${color}" />`;
    svg += `${marker}<text x="${lx + 18}" y="${ly - 2}" font-size="11" fill="#1f2a2c">${escapeHtml(label)}</text>`;
  });

  svg += `<text x="${margin.left}" y="${margin.top - 6}" font-size="11" fill="#5f6d70">Costo diario MXN</text>`;
  svg += `<text x="${width - margin.right}" y="${margin.top - 6}" text-anchor="end" font-size="11" fill="#5f6d70">Acumulado MXN</text>`;
  svg += "</svg>";

  const totalMxn = cumulativeValues[cumulativeValues.length - 1] || 0;
  elements.timeseriesChart.innerHTML = `
    <div class="analytics-chart-scroller">${svg}</div>
    <p class="status-text success">Acumulado del periodo: ${escapeHtml(formatCurrencyMxn(totalMxn))}</p>
  `;
}

function renderQueriesTable(items) {
  if (!items.length) {
    elements.tableContainer.innerHTML = `
      <div class="empty-state">
        <h3>Sin consultas</h3>
        <p>No hay resultados para los filtros actuales.</p>
      </div>`;
    return;
  }

  const rows = items.map((item) => `
    <tr>
      <td>${escapeHtml(formatTimestamp(item.timestamp))}</td>
      <td><span class="audit-status ${statusTone(item.status)}">${escapeHtml(item.status)}</span></td>
      <td>${escapeHtml(item.dataset_id)}</td>
      <td>${escapeHtml(item.user_id)}</td>
      <td class="analytics-question-cell">${escapeHtml(item.question)}</td>
      <td>${formatNumber(item.llm_totals?.input_token_count || 0)}</td>
      <td>${formatNumber(item.llm_totals?.output_token_count || 0)}</td>
      <td>${formatNumber(item.llm_totals?.thinking_token_count || 0)}</td>
      <td>${formatNumber(item.llm_totals?.total_token_count || 0)}</td>
      <td>${formatCurrencyMxn(item.llm_totals?.input_cost_mxn || 0)}</td>
      <td>${formatCurrencyMxn(item.llm_totals?.output_cost_mxn || 0)}</td>
      <td>${formatCurrencyMxn(item.llm_totals?.thinking_cost_mxn || 0)}</td>
      <td>${formatCurrencyMxn(item.llm_totals?.total_cost_mxn || 0)}</td>
      <td>${item.cache_hit ? "cache" : "fresh"}</td>
      <td>${renderAuditDetails(item)}</td>
    </tr>
  `).join("");

  elements.tableContainer.innerHTML = `
    <div class="table-wrapper analytics-table-wrapper">
      <table>
        <thead>
          <tr>
            <th>Timestamp</th>
            <th>Status</th>
            <th>Dataset</th>
            <th>User</th>
            <th>Question</th>
            <th>Input tokens</th>
            <th>Output tokens</th>
            <th>Thinking tokens</th>
            <th>Total tokens</th>
            <th>Input MXN</th>
            <th>Output MXN</th>
            <th>Thinking MXN</th>
            <th>Total MXN</th>
            <th>Cache</th>
            <th>Detalle</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function renderAuditDetails(item) {
  const stageChips = [
    ["cache", item.stages?.cache_lookup_ms || 0],
    ["intent", item.stages?.intent_ms || 0],
    ["query", item.stages?.query_execution_ms || 0],
    ["build", item.stages?.response_build_ms || 0],
    ["summary", item.stages?.summary_ms || 0],
  ].map(([label, value]) => `<span class="telemetry-chip"><strong>${escapeHtml(label)}</strong> ${formatNumber(value)} ms</span>`).join("");

  const responseText = item.response_summary || item.error_message || "Sin texto asociado.";
  const columnsUsed = item.columns_used?.length
    ? item.columns_used.map((column) => `<span class="telemetry-chip">${escapeHtml(column)}</span>`).join("")
    : `<span class="telemetry-chip">Sin columnas registradas</span>`;

  const summaryChips = [
    `Latencia ${formatNumber(item.total_latency_ms || 0)} ms`,
    `Input ${formatNumber(item.llm_totals?.input_token_count || 0)} tokens`,
    `Output ${formatNumber(item.llm_totals?.output_token_count || 0)} tokens`,
    `Thinking ${formatNumber(item.llm_totals?.thinking_token_count || 0)} tokens`,
    `FX ${formatDecimal(item.llm_totals?.usd_to_mxn_rate || 0)} MXN/USD`,
    `Total ${formatCurrencyMxn(item.llm_totals?.total_cost_mxn || 0)}`,
  ].map((text) => `<span class="telemetry-chip">${escapeHtml(text)}</span>`).join("");

  const calls = Array.isArray(item.llm_calls) && item.llm_calls.length
    ? renderBreakdownTable(
        [
          "Etapa",
          "Modelo",
          "Latencia",
          "Input tok",
          "Output tok",
          "Thinking tok",
          "Total tok",
          "Input MXN",
          "Output MXN",
          "Thinking MXN",
          "Total MXN",
          "Total USD",
        ],
        item.llm_calls.map((call) => [
          call.stage,
          call.model,
          `${formatNumber(call.latency_ms || 0)} ms`,
          formatNumber(call.input_token_count || 0),
          formatNumber(call.output_token_count || 0),
          formatNumber(call.thinking_token_count || 0),
          formatNumber(call.total_token_count || 0),
          formatCurrencyMxn(call.input_cost_mxn || 0),
          formatCurrencyMxn(call.output_cost_mxn || 0),
          formatCurrencyMxn(call.thinking_cost_mxn || 0),
          formatCurrencyMxn(call.total_cost_mxn || 0),
          formatCurrencyUsd(call.total_cost_usd || 0),
        ]),
        "Sin llamadas LLM."
      )
    : `<p class="telemetry-empty">Sin llamadas LLM.</p>`;

  return `
    <details class="audit-row-details">
      <summary>Ver</summary>
      <div class="audit-detail-block">
        <p class="analytics-detail-text">${escapeHtml(responseText)}</p>
        <div class="analytics-chip-row">${summaryChips}</div>
        <div class="analytics-chip-row">${columnsUsed}</div>
        <div class="analytics-chip-row">${stageChips}</div>
        ${calls}
      </div>
    </details>
  `;
}

function renderBreakdownTable(columns, rows, emptyMessage) {
  if (!rows.length) {
    return `
      <div class="empty-state compact">
        <p>${escapeHtml(emptyMessage)}</p>
      </div>`;
  }

  return `
    <div class="table-wrapper analytics-breakdown-table">
      <table>
        <thead>
          <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(String(cell))}</td>`).join("")}</tr>`).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderSummaryLoading() {
  elements.summaryGrid.innerHTML = Array.from({ length: 9 }, (_, index) => `
    <article class="card analytics-kpi-card">
      <p class="eyebrow">Carga ${index + 1}</p>
      <strong class="analytics-kpi-value">...</strong>
    </article>
  `).join("");
  elements.breakdownStatus.innerHTML = `<p class="status-text">Cargando...</p>`;
  elements.breakdownModel.innerHTML = `<p class="status-text">Cargando...</p>`;
  elements.breakdownStage.innerHTML = `<p class="status-text">Cargando...</p>`;
}

function renderChartLoading() {
  elements.timeseriesChart.innerHTML = `
    <div class="empty-state">
      <h3>Cargando serie</h3>
      <p>Preparando la progresion diaria de costos.</p>
    </div>`;
}

function renderTableLoading() {
  elements.tableContainer.innerHTML = `
    <div class="empty-state">
      <h3>Cargando bitacora</h3>
      <p>Consultando las metricas registradas.</p>
    </div>`;
}

function renderSummaryError(message) {
  elements.summaryGrid.innerHTML = `
    <div class="empty-state">
      <h3>No pude cargar el resumen</h3>
      <p>${escapeHtml(message)}</p>
    </div>`;
  elements.breakdownStatus.innerHTML = "";
  elements.breakdownModel.innerHTML = "";
  elements.breakdownStage.innerHTML = "";
}

function renderChartError(message) {
  elements.timeseriesChart.innerHTML = `
    <div class="empty-state">
      <h3>No pude cargar la grafica</h3>
      <p>${escapeHtml(message)}</p>
    </div>`;
}

function renderQueriesError(message) {
  elements.tableContainer.innerHTML = `
    <div class="empty-state">
      <h3>No pude cargar la bitacora</h3>
      <p>${escapeHtml(message)}</p>
    </div>`;
}

function updatePaginationState(isLoading) {
  elements.pagePrev.disabled = isLoading || state.offset <= 0;
  elements.pageNext.disabled = isLoading || (state.offset + state.limit >= state.total);

  if (!state.total) {
    elements.paginationText.textContent = "0 resultados";
    return;
  }

  const start = state.offset + 1;
  const end = Math.min(state.offset + state.limit, state.total);
  elements.paginationText.textContent = `${formatNumber(start)}-${formatNumber(end)} de ${formatNumber(state.total)}`;
}

function setStatus(text, tone) {
  elements.statusText.textContent = text;
  elements.statusText.className = tone ? `status-text ${tone}` : "status-text";
}

function syncAuthInputs() {
  if (elements.apiKeyInput) {
    elements.apiKeyInput.value = ensureApiKey();
  }
  if (elements.actorUserIdInput) {
    elements.actorUserIdInput.value = ensureUserId();
  }
}

function saveAuthInputs() {
  const apiKey = (elements.apiKeyInput?.value || "").trim();
  const userId = (elements.actorUserIdInput?.value || "").trim() || `web-${generateId()}`;
  window.localStorage.setItem(STORAGE_KEYS.apiKey, apiKey);
  window.localStorage.setItem(STORAGE_KEYS.userId, userId);
  if (elements.actorUserIdInput) {
    elements.actorUserIdInput.value = userId;
  }
  setAuthStatus(apiKey ? "Acceso guardado." : "Captura la API key para continuar.", apiKey ? "success" : "warn");
}

function ensureApiKey() {
  return (window.localStorage.getItem(STORAGE_KEYS.apiKey) || "").trim();
}

function ensureUserId() {
  let userId = window.localStorage.getItem(STORAGE_KEYS.userId);
  if (!userId) {
    userId = `web-${generateId()}`;
    window.localStorage.setItem(STORAGE_KEYS.userId, userId);
  }
  return userId;
}

function hasApiAccess() {
  return Boolean(ensureApiKey() && ensureUserId());
}

function ensureApiAccess() {
  if (!hasApiAccess()) {
    throw new Error("Captura la API key y el user id para usar esta pantalla.");
  }
}

function buildApiHeaders(extraHeaders = {}) {
  ensureApiAccess();
  return {
    "X-API-Key": ensureApiKey(),
    "X-User-Id": ensureUserId(),
    ...extraHeaders,
  };
}

function setAuthStatus(text, tone = "") {
  if (!elements.authStatusText) return;
  elements.authStatusText.textContent = text;
  elements.authStatusText.className = tone ? `status-text ${tone}` : "status-text";
}

async function parseJsonResponse(response) {
  const text = await response.text();
  if (!text) return {};
  try { return JSON.parse(text); } catch { return { detail: text }; }
}

function toIsoDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}

function toDateTimeLocalValue(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function formatTimestamp(value) {
  const date = parseDateish(value);
  if (Number.isNaN(date.getTime())) return String(value || "");
  return new Intl.DateTimeFormat("es-MX", {
    dateStyle: "short",
    timeStyle: "short",
  }).format(date);
}

function formatShortDate(value) {
  const date = parseDateish(value);
  if (Number.isNaN(date.getTime())) return String(value || "");
  return new Intl.DateTimeFormat("es-MX", { dateStyle: "medium" }).format(date);
}

function shortDateLabel(value) {
  const date = parseDateish(value);
  if (Number.isNaN(date.getTime())) return String(value || "");
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${day}/${month}`;
}

function parseDateish(value) {
  if (!value) return new Date("");
  const raw = String(value);
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return new Date(`${raw}T00:00:00`);
  }
  return new Date(raw);
}

function formatNumber(value) {
  const n = Number(value);
  if (Number.isNaN(n)) return String(value);
  return new Intl.NumberFormat("es-MX").format(n);
}

function formatDecimal(value) {
  const n = Number(value);
  if (Number.isNaN(n)) return String(value);
  return new Intl.NumberFormat("es-MX", { maximumFractionDigits: 4 }).format(n);
}

function formatCurrencyMxn(value) {
  const n = Number(value) || 0;
  return new Intl.NumberFormat("es-MX", {
    style: "currency",
    currency: "MXN",
    minimumFractionDigits: 4,
    maximumFractionDigits: 6,
  }).format(n);
}

function formatCurrencyUsd(value) {
  const n = Number(value) || 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 4,
    maximumFractionDigits: 6,
  }).format(n);
}

function shortMoney(value) {
  const n = Number(value) || 0;
  if (n >= 1000) return `$${formatDecimal(n / 1000)}k`;
  return `$${formatDecimal(n)}`;
}

function buildTicks(maxValue) {
  const safeMax = Math.max(Number(maxValue) || 0, 1);
  return Array.from({ length: 5 }, (_, index) => (safeMax * index) / 4);
}

function statusTone(status) {
  if (status === "ok") return "success";
  if (status === "assistant_message" || status === "needs_clarification") return "warn";
  return "error";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function generateId() {
  try {
    if (window.crypto && typeof window.crypto.randomUUID === "function") {
      return window.crypto.randomUUID();
    }
  } catch (_) {}

  try {
    if (window.crypto && typeof window.crypto.getRandomValues === "function") {
      const bytes = new Uint8Array(16);
      window.crypto.getRandomValues(bytes);
      bytes[6] = (bytes[6] & 15) | 64;
      bytes[8] = (bytes[8] & 63) | 128;
      const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
      return [
        hex.slice(0, 4).join(""),
        hex.slice(4, 6).join(""),
        hex.slice(6, 8).join(""),
        hex.slice(8, 10).join(""),
        hex.slice(10, 16).join(""),
      ].join("-");
    }
  } catch (_) {}

  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 12)}`;
}
