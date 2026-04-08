const STORAGE_KEYS = {
  apiBase: "metrics-api-base",
  apiKey: "metrics-api-key",
  userId: "metrics-user-id",
};

const DEFAULT_STATE = {
  datasets: [],
  summary: null,
  timeseries: [],
  queries: [],
  rankingSample: [],
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

const state = structuredClone(DEFAULT_STATE);

const palette = {
  costInput: "#17645f",
  costOutput: "#cb7f3d",
  costThinking: "#8c4d68",
  queriesLine: "#223837",
};

const elements = {
  heroHealthScore: document.getElementById("hero-health-score"),
  heroHealthLabel: document.getElementById("hero-health-label"),
  heroHealthCopy: document.getElementById("hero-health-copy"),
  heroPeriodText: document.getElementById("hero-period-text"),
  heroCostText: document.getElementById("hero-cost-text"),
  heroQueryText: document.getElementById("hero-query-text"),
  heroErrorText: document.getElementById("hero-error-text"),
  apiBaseInput: document.getElementById("api-base-input"),
  apiKeyInput: document.getElementById("api-key-input"),
  actorUserIdInput: document.getElementById("actor-user-id-input"),
  authSave: document.getElementById("auth-save"),
  refreshDashboard: document.getElementById("refresh-dashboard"),
  authStatusText: document.getElementById("auth-status-text"),
  dashboardStatusText: document.getElementById("dashboard-status-text"),
  datasetChip: document.getElementById("selected-dataset-chip"),
  filterDataset: document.getElementById("filter-dataset"),
  filterStatus: document.getElementById("filter-status"),
  filterCacheHit: document.getElementById("filter-cache-hit"),
  filterFrom: document.getElementById("filter-from"),
  filterTo: document.getElementById("filter-to"),
  filterUserId: document.getElementById("filter-user-id"),
  resetFilters: document.getElementById("reset-filters"),
  kpiGrid: document.getElementById("kpi-grid"),
  executiveStory: document.getElementById("executive-story"),
  focusList: document.getElementById("focus-list"),
  trendChart: document.getElementById("trend-chart"),
  costMix: document.getElementById("cost-mix"),
  topUsers: document.getElementById("top-users"),
  topDatasets: document.getElementById("top-datasets"),
  breakdownStatus: document.getElementById("breakdown-status"),
  breakdownModel: document.getElementById("breakdown-model"),
  breakdownStage: document.getElementById("breakdown-stage"),
  auditTable: document.getElementById("audit-table"),
  paginationText: document.getElementById("pagination-text"),
  pagePrev: document.getElementById("page-prev"),
  pageNext: document.getElementById("page-next"),
  exportCsv: document.getElementById("export-csv"),
  exportJson: document.getElementById("export-json"),
};

let filterDebounce = null;

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  hydrateAccessInputs();
  syncFilterControlsFromState();
  renderLoadingState();
  bootstrap();
});

function bindEvents() {
  elements.authSave?.addEventListener("click", async () => {
    try {
      saveAccess();
      await bootstrap();
    } catch (error) {
      setDashboardStatus(error.message || "No se pudo guardar el acceso.", "error");
    }
  });

  elements.refreshDashboard?.addEventListener("click", () => refreshDashboard({ reloadDatasets: true }));

  elements.resetFilters?.addEventListener("click", () => {
    resetFilters();
    refreshDashboard({ reloadDatasets: false });
  });

  for (const control of [
    elements.filterDataset,
    elements.filterStatus,
    elements.filterCacheHit,
    elements.filterFrom,
    elements.filterTo,
  ]) {
    control?.addEventListener("change", () => {
      applyFilters({ resetOffset: true });
    });
  }

  elements.filterUserId?.addEventListener("input", () => {
    clearTimeout(filterDebounce);
    filterDebounce = window.setTimeout(() => applyFilters({ resetOffset: true }), 280);
  });

  elements.pagePrev?.addEventListener("click", () => {
    if (state.offset <= 0) return;
    state.offset = Math.max(0, state.offset - state.limit);
    refreshDashboard({ reloadDatasets: false });
  });

  elements.pageNext?.addEventListener("click", () => {
    if (state.offset + state.limit >= state.total) return;
    state.offset += state.limit;
    refreshDashboard({ reloadDatasets: false });
  });

  elements.exportCsv?.addEventListener("click", () => exportCurrentQueriesAsCsv());
  elements.exportJson?.addEventListener("click", () => exportCurrentQueriesAsJson());
}

async function bootstrap() {
  if (!hasAccess()) {
    setAuthStatus("Captura API base, API key y User ID para consultar auditoria.", "warn");
    setDashboardStatus("Sin conexion. Falta configurar el acceso.", "warn");
    renderEmptyDashboard("Necesitas acceso valido para cargar las metricas.");
    return;
  }

  setAuthStatus("Acceso listo para consultar.", "success");
  await refreshDashboard({ reloadDatasets: true });
}

async function refreshDashboard({ reloadDatasets }) {
  try {
    syncStateFromFilterControls();
    setDashboardStatus("Cargando auditoria...", "warn");
    setPaginationLoading(true);
    if (reloadDatasets || !state.datasets.length) {
      await loadDatasets();
    }

    const summaryQuery = buildApiQuery({ includePagination: false });
    const queriesQuery = buildApiQuery({ includePagination: true, limit: state.limit, offset: state.offset });
    const rankingQuery = buildApiQuery({ includePagination: true, limit: 200, offset: 0 });
    const timeseriesQuery = buildApiQuery({ includePagination: false });

    const [summaryResponse, queriesResponse, rankingResponse, timeseriesResponse] = await Promise.all([
      fetchJson("/metrics/summary", { query: summaryQuery }),
      fetchJson("/metrics/queries", { query: queriesQuery }),
      fetchJson("/metrics/queries", { query: rankingQuery }),
      fetchJson("/metrics/timeseries", { query: timeseriesQuery }),
    ]);

    state.summary = summaryResponse;
    state.timeseries = timeseriesResponse.items || [];
    state.queries = queriesResponse.items || [];
    state.rankingSample = rankingResponse.items || [];
    state.total = Number(queriesResponse.total || 0);
    state.offset = Number(queriesResponse.offset || 0);

    renderHero();
    renderKpis();
    renderExecutiveStory();
    renderFocusCards();
    renderTrendChart();
    renderCostMix();
    renderRankings();
    renderBreakdowns();
    renderAuditTable();
    updateDatasetChip();
    setPaginationLoading(false);
    setDashboardStatus(
      state.total
        ? `${formatNumber(state.total)} consulta${state.total === 1 ? "" : "s"} encontradas para los filtros actuales.`
        : "No hay consultas registradas para los filtros actuales.",
      state.total ? "success" : "warn",
    );
  } catch (error) {
    renderEmptyDashboard(error.message || "No se pudo cargar la auditoria.");
    setPaginationLoading(false);
    setDashboardStatus(error.message || "No se pudo cargar la auditoria.", "error");
  }
}

async function loadDatasets() {
  const payload = await fetchJson("/datasets");
  state.datasets = Array.isArray(payload) ? payload : [];
  renderDatasetOptions();
}

function renderDatasetOptions() {
  const currentValue = state.filters.datasetId;
  elements.filterDataset.innerHTML = [
    '<option value="">Todos los datasets</option>',
    ...state.datasets.map((dataset) => (
      `<option value="${escapeAttr(dataset.id)}">${escapeHtml(dataset.display_name)}</option>`
    )),
  ].join("");
  elements.filterDataset.value = currentValue;
  updateDatasetChip();
}

function renderHero() {
  const summary = state.summary || {};
  const queryCount = Number(summary.query_count || 0);
  const errorCount = Number(summary.error_count || 0);
  const errorRate = queryCount ? errorCount / queryCount : 0;
  const healthScore = computeHealthScore(summary);
  const healthTone = scoreTone(healthScore);

  elements.heroHealthScore.textContent = queryCount ? String(healthScore) : "--";
  elements.heroHealthLabel.textContent = queryCount ? healthTone.label : "Pendiente";
  elements.heroHealthLabel.className = "hero-score-label";
  elements.heroHealthLabel.style.background = healthTone.background;
  elements.heroHealthLabel.style.color = healthTone.color;
  elements.heroHealthCopy.textContent = buildHeroNarrative(summary);
  elements.heroPeriodText.textContent = describePeriod();
  elements.heroCostText.textContent = formatCurrencyMxn(summary.total_cost_mxn || 0);
  elements.heroQueryText.textContent = formatNumber(queryCount);
  elements.heroErrorText.textContent = formatPercent(errorRate);
}

function renderKpis() {
  const summary = state.summary || {};
  const queryCount = Number(summary.query_count || 0);
  const cacheRate = queryCount ? Number(summary.cache_hit_count || 0) / queryCount : 0;
  const errorRate = queryCount ? Number(summary.error_count || 0) / queryCount : 0;
  const avgCost = queryCount ? Number(summary.total_cost_mxn || 0) / queryCount : 0;
  const successCount = Math.max(0, queryCount - Number(summary.error_count || 0));
  const cards = [
    {
      label: "Consultas totales",
      value: formatNumber(queryCount),
      note: `${formatNumber(successCount)} salieron sin error registrado`,
    },
    {
      label: "Costo total MXN",
      value: formatCurrencyMxn(summary.total_cost_mxn || 0),
      note: `Promedio por consulta: ${formatCurrencyMxn(avgCost)}`,
    },
    {
      label: "Tokens totales",
      value: formatNumber(summary.total_token_count || 0),
      note: `${formatNumber(summary.total_input_token_count || 0)} entrada / ${formatNumber(summary.total_output_token_count || 0)} salida`,
    },
    {
      label: "Error rate",
      value: formatPercent(errorRate),
      note: `${formatNumber(summary.error_count || 0)} eventos con error`,
    },
    {
      label: "Uso de cache",
      value: formatPercent(cacheRate),
      note: `${formatNumber(summary.cache_hit_count || 0)} respuestas reutilizadas`,
    },
    {
      label: "P95 de latencia",
      value: `${formatNumber(summary.p95_total_latency_ms || 0)} ms`,
      note: `Promedio general: ${formatDecimal(summary.avg_total_latency_ms || 0)} ms`,
    },
    {
      label: "Thinking cost MXN",
      value: formatCurrencyMxn(summary.total_thinking_cost_mxn || 0),
      note: `Thinking tokens: ${formatNumber(summary.total_thinking_token_count || 0)}`,
    },
    {
      label: "Costo estimado USD",
      value: formatCurrencyUsd(summary.total_estimated_cost_usd || 0),
      note: `Referencia operativa para presupuesto`,
    },
  ];

  elements.kpiGrid.innerHTML = cards.map((card) => `
    <article class="kpi-card">
      <p class="eyebrow">${escapeHtml(card.label)}</p>
      <strong>${escapeHtml(card.value)}</strong>
      <p class="kpi-footnote">${escapeHtml(card.note)}</p>
    </article>
  `).join("");
}

function renderExecutiveStory() {
  const summary = state.summary || {};
  const queryCount = Number(summary.query_count || 0);
  const errorCount = Number(summary.error_count || 0);
  const errorRate = queryCount ? errorCount / queryCount : 0;
  const trend = buildTrendInsight();
  const avgCost = queryCount ? Number(summary.total_cost_mxn || 0) / queryCount : 0;
  const topUser = aggregateBy(state.rankingSample, (item) => item.user_id);
  const topDataset = aggregateBy(state.rankingSample, (item) => {
    const dataset = state.datasets.find((candidate) => candidate.id === item.dataset_id);
    return dataset?.display_name || item.dataset_id || "Sin dataset";
  });
  const story = [
    {
      title: "Demanda del periodo",
      text: queryCount
        ? `Se registraron ${formatNumber(queryCount)} consultas en el periodo filtrado. ${trend.description}`
        : "No hay consultas registradas para el periodo seleccionado.",
    },
    {
      title: "Inversion y eficiencia",
      text: queryCount
        ? `El gasto estimado fue de ${formatCurrencyMxn(summary.total_cost_mxn || 0)}. El costo promedio por consulta se ubica en ${formatCurrencyMxn(avgCost)} y el cache cubrio ${formatPercent(queryCount ? Number(summary.cache_hit_count || 0) / queryCount : 0)} del flujo.`
        : "Sin consumo registrado todavia.",
    },
    {
      title: "Foco de uso",
      text: topUser.length || topDataset.length
        ? `${topUser.length ? `El usuario mas activo en la muestra reciente es ${topUser[0].label} con ${formatNumber(topUser[0].count)} consultas.` : ""} ${topDataset.length ? `El dataset con mayor traccion es ${topDataset[0].label} con ${formatNumber(topDataset[0].count)} consultas.` : ""}`.trim()
        : "Sin suficiente actividad reciente para identificar concentracion de uso.",
    },
    {
      title: "Calidad operativa",
      text: queryCount
        ? `La tasa de error observada es ${formatPercent(errorRate)} y el percentil 95 de latencia esta en ${formatNumber(summary.p95_total_latency_ms || 0)} ms.`
        : "No hay actividad para evaluar calidad operativa.",
    },
  ];

  elements.executiveStory.innerHTML = `
    <div class="story-list">
      ${story.map((item) => `
        <article class="story-item">
          <h3>${escapeHtml(item.title)}</h3>
          <p>${escapeHtml(item.text)}</p>
        </article>
      `).join("")}
    </div>
  `;
}

function renderFocusCards() {
  const summary = state.summary || {};
  const queryCount = Number(summary.query_count || 0);
  const errorRate = queryCount ? Number(summary.error_count || 0) / queryCount : 0;
  const cacheRate = queryCount ? Number(summary.cache_hit_count || 0) / queryCount : 0;
  const thinkingShare = Number(summary.total_cost_mxn || 0)
    ? Number(summary.total_thinking_cost_mxn || 0) / Number(summary.total_cost_mxn || 1)
    : 0;

  const cards = [
    errorRate > 0.08
      ? {
          tone: "risk",
          title: "Riesgo de servicio",
          text: `La tasa de error esta en ${formatPercent(errorRate)}. Conviene revisar preguntas fallidas, datasets usados y latencia asociada.`,
        }
      : {
          tone: "good",
          title: "Estabilidad aceptable",
          text: queryCount
            ? `La tasa de error se mantiene en ${formatPercent(errorRate)}, dentro de un rango razonable para operacion diaria.`
            : "Sin actividad para evaluar estabilidad.",
        },
    cacheRate < 0.2
      ? {
          tone: "watch",
          title: "Espacio para mejorar cache",
          text: `Solo ${formatPercent(cacheRate)} de las respuestas vino de cache. Si hay consultas repetitivas, aqui hay una oportunidad de eficiencia.`,
        }
      : {
          tone: "good",
          title: "Buena reutilizacion",
          text: `${formatPercent(cacheRate)} del flujo aprovecha cache. Esto reduce tiempo de respuesta y costo marginal.`,
        },
    thinkingShare > 0.35
      ? {
          tone: "watch",
          title: "Thinking con peso relevante",
          text: `El thinking representa ${formatPercent(thinkingShare)} del costo total. Revisa si el nivel de razonamiento esta alineado al valor del caso de uso.`,
        }
      : {
          tone: "good",
          title: "Costo de thinking controlado",
          text: `El thinking pesa ${formatPercent(thinkingShare)} del costo total, con espacio razonable para seguir escalando demanda.`,
        },
  ];

  elements.focusList.innerHTML = cards.map((card) => `
    <article class="focus-card" data-tone="${escapeAttr(card.tone)}">
      <h3>${escapeHtml(card.title)}</h3>
      <p>${escapeHtml(card.text)}</p>
    </article>
  `).join("");
}

function renderTrendChart() {
  const items = state.timeseries || [];
  if (!items.length) {
    elements.trendChart.innerHTML = renderEmptyState(
      "Sin serie temporal",
      "No hay actividad para construir la tendencia del periodo."
    );
    return;
  }

  const width = Math.max(920, items.length * 76);
  const height = 360;
  const margin = { top: 28, right: 64, bottom: 88, left: 66 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const step = innerWidth / items.length;
  const barWidth = Math.max(18, step * 0.56);

  const costTotals = items.map((item) => (
    Number(item.input_cost_mxn || 0) +
    Number(item.output_cost_mxn || 0) +
    Number(item.thinking_cost_mxn || 0)
  ));
  const queryCounts = items.map((item) => Number(item.query_count || 0));
  const maxCost = Math.max(...costTotals, 1);
  const maxQueries = Math.max(...queryCounts, 1);
  const leftTicks = buildTicks(maxCost);
  const rightTicks = buildTicks(maxQueries);

  const xFor = (index) => margin.left + index * step + (step - barWidth) / 2;
  const yCost = (value) => margin.top + innerHeight - (value / maxCost) * innerHeight;
  const yQuery = (value) => margin.top + innerHeight - (value / maxQueries) * innerHeight;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica diaria de costo y volumen">`;

  leftTicks.forEach((tick) => {
    const y = yCost(tick);
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(34,56,55,0.08)" />`;
    svg += `<text x="${margin.left - 10}" y="${y + 4}" text-anchor="end" font-size="10" fill="#627472">${escapeHtml(shortMoney(tick))}</text>`;
  });

  rightTicks.forEach((tick) => {
    const y = yQuery(tick);
    svg += `<text x="${width - margin.right + 10}" y="${y + 4}" font-size="10" fill="#627472">${escapeHtml(formatNumber(Math.round(tick)))}</text>`;
  });

  items.forEach((item, index) => {
    const x = xFor(index);
    const stack = [
      { value: Number(item.input_cost_mxn || 0), color: palette.costInput, label: "Entrada" },
      { value: Number(item.output_cost_mxn || 0), color: palette.costOutput, label: "Salida" },
      { value: Number(item.thinking_cost_mxn || 0), color: palette.costThinking, label: "Thinking" },
    ];

    let running = 0;
    stack.forEach((segment) => {
      if (segment.value <= 0) return;
      const yTop = yCost(running + segment.value);
      const yBottom = yCost(running);
      svg += `<rect x="${x}" y="${yTop}" width="${barWidth}" height="${Math.max(2, yBottom - yTop)}" rx="6" fill="${segment.color}">
        <title>${escapeHtml(formatShortDate(item.date))} · ${escapeHtml(segment.label)} · ${escapeHtml(formatCurrencyMxn(segment.value))}</title>
      </rect>`;
      running += segment.value;
    });

    const cx = x + barWidth / 2;
    const cy = yQuery(Number(item.query_count || 0));
    svg += `<circle cx="${cx}" cy="${cy}" r="4" fill="${palette.queriesLine}" />`;
    svg += `<text x="${cx}" y="${height - 26}" text-anchor="middle" font-size="10" fill="#627472">${escapeHtml(shortDateLabel(item.date))}</text>`;
  });

  const linePoints = items
    .map((item, index) => `${xFor(index) + barWidth / 2},${yQuery(Number(item.query_count || 0))}`)
    .join(" ");
  svg += `<polyline fill="none" stroke="${palette.queriesLine}" stroke-width="2.5" points="${linePoints}" />`;

  svg += `<text x="${margin.left}" y="${margin.top - 8}" font-size="11" fill="#627472">Costo diario MXN</text>`;
  svg += `<text x="${width - margin.right}" y="${margin.top - 8}" text-anchor="end" font-size="11" fill="#627472">Consultas por dia</text>`;
  svg += "</svg>";

  const totalCost = costTotals.reduce((sum, value) => sum + value, 0);
  const totalQueries = queryCounts.reduce((sum, value) => sum + value, 0);

  elements.trendChart.innerHTML = `
    <div class="chart-scroll">${svg}</div>
    <p class="status-text success">Periodo visible: ${formatNumber(totalQueries)} consultas por un costo total de ${escapeHtml(formatCurrencyMxn(totalCost))}.</p>
  `;
}

function renderCostMix() {
  const summary = state.summary || {};
  const total = Number(summary.total_cost_mxn || 0);
  if (!total) {
    elements.costMix.innerHTML = renderEmptyState(
      "Sin costo registrado",
      "Cuando haya actividad del agente apareceran los componentes de costo."
    );
    return;
  }

  const mix = [
    {
      label: "Entrada",
      value: Number(summary.total_input_cost_mxn || 0),
      color: palette.costInput,
      note: `${formatNumber(summary.total_input_token_count || 0)} tokens`,
    },
    {
      label: "Salida",
      value: Number(summary.total_output_cost_mxn || 0),
      color: palette.costOutput,
      note: `${formatNumber(summary.total_output_token_count || 0)} tokens`,
    },
    {
      label: "Thinking",
      value: Number(summary.total_thinking_cost_mxn || 0),
      color: palette.costThinking,
      note: `${formatNumber(summary.total_thinking_token_count || 0)} tokens`,
    },
  ];

  const mixBar = mix.map((item) => {
    const pct = total ? (item.value / total) * 100 : 0;
    return `<span style="width:${pct}%;background:${item.color}"></span>`;
  }).join("");

  elements.costMix.innerHTML = `
    <div class="mix-bar">${mixBar}</div>
    ${mix.map((item) => `
      <div class="mix-item">
        <span class="swatch" style="background:${item.color}"></span>
        <div>
          <strong>${escapeHtml(item.label)}</strong>
          <div class="rank-subtext">${escapeHtml(item.note)}</div>
        </div>
        <div>
          <strong>${escapeHtml(formatCurrencyMxn(item.value))}</strong>
          <div class="rank-subtext">${escapeHtml(formatPercent(total ? item.value / total : 0))} del total</div>
        </div>
      </div>
    `).join("")}
  `;
}

function renderRankings() {
  const userRows = aggregateBy(state.rankingSample, (item) => item.user_id);
  const datasetRows = aggregateBy(state.rankingSample, (item) => {
    const dataset = state.datasets.find((candidate) => candidate.id === item.dataset_id);
    return dataset?.display_name || item.dataset_id || "Sin dataset";
  });

  elements.topUsers.innerHTML = renderRankList(userRows, {
    emptyTitle: "Sin usuarios recientes",
    emptyText: "Todavia no hay actividad suficiente para construir ranking.",
  });
  elements.topDatasets.innerHTML = renderRankList(datasetRows, {
    emptyTitle: "Sin datasets recientes",
    emptyText: "Todavia no hay actividad suficiente para construir ranking.",
  });
}

function renderRankList(rows, { emptyTitle, emptyText }) {
  if (!rows.length) {
    return renderEmptyState(emptyTitle, emptyText);
  }

  const maxCount = Math.max(...rows.map((row) => row.count), 1);
  return `
    <div class="rank-list">
      ${rows.slice(0, 6).map((row, index) => `
        <article class="rank-item">
          <div class="rank-header">
            <div>
              <div class="rank-name">#${index + 1} ${escapeHtml(row.label)}</div>
              <div class="rank-subtext">${formatNumber(row.count)} consultas · ${formatCurrencyMxn(row.totalCost)} · ${formatNumber(row.totalTokens)} tokens</div>
            </div>
            <strong>${formatNumber(row.count)}</strong>
          </div>
          <div class="progress"><span style="width:${(row.count / maxCount) * 100}%"></span></div>
        </article>
      `).join("")}
    </div>
  `;
}

function renderBreakdowns() {
  const summary = state.summary || {};
  elements.breakdownStatus.innerHTML = renderBreakdownList(summary.by_status || [], {
    mainValue: (item) => item.query_count,
    secondaryText: (item) => `${formatCurrencyMxn(item.total_cost_mxn || 0)} · ${formatNumber(item.total_token_count || 0)} tokens`,
    label: (item) => statusLabel(item.status),
  });
  elements.breakdownModel.innerHTML = renderBreakdownList(summary.by_model || [], {
    mainValue: (item) => item.call_count,
    secondaryText: (item) => `${formatCurrencyMxn(item.total_cost_mxn || 0)} · ${formatNumber(item.total_token_count || 0)} tokens`,
    label: (item) => item.model,
  });
  elements.breakdownStage.innerHTML = renderBreakdownList(summary.by_stage || [], {
    mainValue: (item) => item.call_count,
    secondaryText: (item) => `${formatDecimal(item.avg_latency_ms || 0)} ms prom. · ${formatCurrencyMxn(item.total_cost_mxn || 0)}`,
    label: (item) => stageLabel(item.stage),
  });
}

function renderBreakdownList(items, config) {
  if (!items.length) {
    return renderEmptyState("Sin datos", "No hay registros suficientes para construir esta distribucion.");
  }

  const maxValue = Math.max(...items.map((item) => Number(config.mainValue(item) || 0)), 1);
  return `
    <div class="breakdown-list">
      ${items.map((item) => {
        const value = Number(config.mainValue(item) || 0);
        const width = (value / maxValue) * 100;
        return `
          <article class="breakdown-item">
            <div class="breakdown-header">
              <div class="breakdown-name">${escapeHtml(config.label(item))}</div>
              <strong>${escapeHtml(formatNumber(value))}</strong>
            </div>
            <div class="progress"><span style="width:${width}%"></span></div>
            <div class="breakdown-subtext">${escapeHtml(config.secondaryText(item))}</div>
          </article>
        `;
      }).join("")}
    </div>
  `;
}

function renderAuditTable() {
  if (!state.queries.length) {
    elements.auditTable.innerHTML = renderEmptyState(
      "Sin consultas",
      "No hay registros para los filtros y la pagina seleccionados."
    );
    updatePaginationText();
    return;
  }

  const rows = state.queries.map((item) => `
    <tr>
      <td>${escapeHtml(formatTimestamp(item.timestamp))}</td>
      <td><span class="status-pill ${statusTone(item.status)}">${escapeHtml(statusLabel(item.status))}</span></td>
      <td>${escapeHtml(item.dataset_id || "Sin dataset")}</td>
      <td>${escapeHtml(item.user_id || "-")}</td>
      <td class="question-cell">${escapeHtml(item.question || "")}</td>
      <td>${escapeHtml(formatNumber(item.llm_totals?.total_token_count || 0))}</td>
      <td>${escapeHtml(formatCurrencyMxn(item.llm_totals?.total_cost_mxn || 0))}</td>
      <td>${escapeHtml(`${formatNumber(item.total_latency_ms || 0)} ms`)}</td>
      <td>${item.cache_hit ? "cache" : "fresh"}</td>
      <td>${renderAuditDetails(item)}</td>
    </tr>
  `).join("");

  elements.auditTable.innerHTML = `
    <div class="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>Fecha</th>
            <th>Status</th>
            <th>Dataset</th>
            <th>User ID</th>
            <th>Pregunta</th>
            <th>Tokens</th>
            <th>Costo MXN</th>
            <th>Latencia</th>
            <th>Cache</th>
            <th>Detalle</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;

  updatePaginationText();
}

function renderAuditDetails(item) {
  const summaryText = item.response_summary || item.error_message || "Sin texto resumido.";
  const stageChips = [
    ["cache", item.stages?.cache_lookup_ms || 0],
    ["intent", item.stages?.intent_ms || 0],
    ["query", item.stages?.query_execution_ms || 0],
    ["build", item.stages?.response_build_ms || 0],
    ["summary", item.stages?.summary_ms || 0],
  ].map(([label, value]) => `<span class="chip">${escapeHtml(label)} ${escapeHtml(formatNumber(value))} ms</span>`).join("");

  const callChips = (item.llm_calls || []).map((call) => (
    `<span class="chip">${escapeHtml(stageLabel(call.stage))} · ${escapeHtml(call.model)} · ${escapeHtml(formatCurrencyMxn(call.total_cost_mxn || 0))}</span>`
  )).join("");

  const meta = [
    item.actor_user_name ? `Nombre: ${item.actor_user_name}` : "",
    item.session_token ? `Sesion: ${item.session_token}` : "",
    item.client_id ? `Client: ${item.client_id}` : "",
    item.app_session_id ? `App session: ${item.app_session_id}` : "",
  ].filter(Boolean).join(" · ");

  return `
    <details class="audit-details">
      <summary>Ver detalle</summary>
      <div class="audit-detail-block">
        <div>${escapeHtml(summaryText)}</div>
        <div class="chip-row">
          <span class="chip">Input ${escapeHtml(formatNumber(item.llm_totals?.input_token_count || 0))}</span>
          <span class="chip">Output ${escapeHtml(formatNumber(item.llm_totals?.output_token_count || 0))}</span>
          <span class="chip">Thinking ${escapeHtml(formatNumber(item.llm_totals?.thinking_token_count || 0))}</span>
          <span class="chip">Costo ${escapeHtml(formatCurrencyMxn(item.llm_totals?.total_cost_mxn || 0))}</span>
        </div>
        <div class="chip-row">${stageChips || '<span class="chip">Sin etapas</span>'}</div>
        <div class="chip-row">${callChips || '<span class="chip">Sin llamadas LLM registradas</span>'}</div>
        ${meta ? `<div class="audit-detail-meta">${escapeHtml(meta)}</div>` : ""}
      </div>
    </details>
  `;
}

function renderLoadingState() {
  elements.kpiGrid.innerHTML = Array.from({ length: 8 }, (_, index) => `
    <article class="kpi-card">
      <p class="eyebrow">Carga ${index + 1}</p>
      <strong>...</strong>
      <p class="kpi-footnote">Preparando los indicadores.</p>
    </article>
  `).join("");
  const placeholder = renderEmptyState("Cargando", "Preparando la vista ejecutiva.");
  elements.executiveStory.innerHTML = placeholder;
  elements.focusList.innerHTML = placeholder;
  elements.trendChart.innerHTML = placeholder;
  elements.costMix.innerHTML = placeholder;
  elements.topUsers.innerHTML = placeholder;
  elements.topDatasets.innerHTML = placeholder;
  elements.breakdownStatus.innerHTML = placeholder;
  elements.breakdownModel.innerHTML = placeholder;
  elements.breakdownStage.innerHTML = placeholder;
  elements.auditTable.innerHTML = placeholder;
}

function renderEmptyDashboard(message) {
  const empty = renderEmptyState("Sin informacion disponible", message);
  elements.kpiGrid.innerHTML = empty;
  elements.executiveStory.innerHTML = empty;
  elements.focusList.innerHTML = empty;
  elements.trendChart.innerHTML = empty;
  elements.costMix.innerHTML = empty;
  elements.topUsers.innerHTML = empty;
  elements.topDatasets.innerHTML = empty;
  elements.breakdownStatus.innerHTML = empty;
  elements.breakdownModel.innerHTML = empty;
  elements.breakdownStage.innerHTML = empty;
  elements.auditTable.innerHTML = empty;
  elements.heroHealthScore.textContent = "--";
  elements.heroHealthLabel.textContent = "Pendiente";
  elements.heroHealthLabel.style.background = "rgba(23,100,95,0.1)";
  elements.heroHealthLabel.style.color = "#114b47";
  elements.heroHealthCopy.textContent = message;
  elements.heroPeriodText.textContent = describePeriod();
  elements.heroCostText.textContent = formatCurrencyMxn(0);
  elements.heroQueryText.textContent = "0";
  elements.heroErrorText.textContent = "0%";
  state.total = 0;
  updatePaginationText();
}

function renderEmptyState(title, text) {
  return `
    <div class="empty-state">
      <h3>${escapeHtml(title)}</h3>
      <p>${escapeHtml(text)}</p>
    </div>
  `;
}

function hydrateAccessInputs() {
  elements.apiBaseInput.value = getStoredApiBase();
  elements.apiKeyInput.value = getStoredApiKey();
  elements.actorUserIdInput.value = getStoredUserId();
}

function saveAccess() {
  const normalizedBase = normalizeApiBase(elements.apiBaseInput.value);
  const apiKey = (elements.apiKeyInput.value || "").trim();
  const userId = (elements.actorUserIdInput.value || "").trim() || `auditoria-${generateId()}`;

  if (!normalizedBase) {
    throwAndShowAccessError("La API base no es valida. Usa un formato como http://127.0.0.1:8000");
    return;
  }

  window.localStorage.setItem(STORAGE_KEYS.apiBase, normalizedBase);
  window.localStorage.setItem(STORAGE_KEYS.apiKey, apiKey);
  window.localStorage.setItem(STORAGE_KEYS.userId, userId);

  elements.apiBaseInput.value = normalizedBase;
  elements.actorUserIdInput.value = userId;
  setAuthStatus("Acceso guardado.", apiKey && userId ? "success" : "warn");
}

function throwAndShowAccessError(message) {
  setAuthStatus(message, "error");
  throw new Error(message);
}

function hasAccess() {
  return Boolean(getStoredApiBase() && getStoredApiKey() && getStoredUserId());
}

function getStoredApiBase() {
  return normalizeApiBase(window.localStorage.getItem(STORAGE_KEYS.apiBase) || "");
}

function getStoredApiKey() {
  return (window.localStorage.getItem(STORAGE_KEYS.apiKey) || "").trim();
}

function getStoredUserId() {
  let value = (window.localStorage.getItem(STORAGE_KEYS.userId) || "").trim();
  if (!value) {
    value = `auditoria-${generateId()}`;
    window.localStorage.setItem(STORAGE_KEYS.userId, value);
  }
  return value;
}

function buildHeaders(extraHeaders = {}) {
  if (!hasAccess()) {
    throw new Error("Captura acceso valido antes de consultar la auditoria.");
  }
  return {
    "X-API-Key": getStoredApiKey(),
    "X-User-Id": getStoredUserId(),
    ...extraHeaders,
  };
}

function syncFilterControlsFromState() {
  elements.filterDataset.value = state.filters.datasetId;
  elements.filterStatus.value = state.filters.status;
  elements.filterCacheHit.value = state.filters.cacheHit;
  elements.filterFrom.value = state.filters.from;
  elements.filterTo.value = state.filters.to;
  elements.filterUserId.value = state.filters.userId;
}

function syncStateFromFilterControls() {
  state.filters.datasetId = (elements.filterDataset.value || "").trim();
  state.filters.status = (elements.filterStatus.value || "").trim();
  state.filters.cacheHit = (elements.filterCacheHit.value || "").trim();
  state.filters.from = elements.filterFrom.value || "";
  state.filters.to = elements.filterTo.value || "";
  state.filters.userId = (elements.filterUserId.value || "").trim();
}

function resetFilters() {
  state.filters = {
    datasetId: "",
    status: "",
    cacheHit: "",
    from: "",
    to: "",
    userId: "",
  };
  state.offset = 0;
  syncFilterControlsFromState();
  updateDatasetChip();
}

function applyFilters({ resetOffset }) {
  syncStateFromFilterControls();
  if (resetOffset) {
    state.offset = 0;
  }
  refreshDashboard({ reloadDatasets: false });
}

function buildApiQuery({ includePagination, limit, offset }) {
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
    params.set("limit", String(limit ?? state.limit));
    params.set("offset", String(offset ?? state.offset));
  }
  return params;
}

function updateDatasetChip() {
  const selected = state.datasets.find((dataset) => dataset.id === state.filters.datasetId);
  elements.datasetChip.textContent = selected ? selected.display_name : "Todos los datasets";
}

function buildHeroNarrative(summary) {
  const queryCount = Number(summary.query_count || 0);
  if (!queryCount) {
    return "Sin actividad registrada. Ajusta filtros o confirma que el backend ya tenga auditoria acumulada.";
  }

  const cost = formatCurrencyMxn(summary.total_cost_mxn || 0);
  const errorRate = formatPercent(queryCount ? Number(summary.error_count || 0) / queryCount : 0);
  const trend = buildTrendInsight().shortLabel;
  return `Se registraron ${formatNumber(queryCount)} consultas, con un costo total de ${cost}. La tendencia reciente luce ${trend} y el error rate esta en ${errorRate}.`;
}

function buildTrendInsight() {
  const items = state.timeseries || [];
  if (items.length < 2) {
    return {
      shortLabel: "sin suficiente historial",
      description: "Aun no hay suficiente historial diario para comparar comportamiento.",
    };
  }

  const mid = Math.ceil(items.length / 2);
  const first = items.slice(0, mid).reduce((sum, item) => sum + Number(item.query_count || 0), 0);
  const second = items.slice(mid).reduce((sum, item) => sum + Number(item.query_count || 0), 0);
  const delta = second - first;
  const base = Math.max(first, 1);
  const pct = delta / base;

  if (pct > 0.15) {
    return {
      shortLabel: "al alza",
      description: `La segunda mitad del periodo trae ${formatPercent(pct)} mas demanda que la primera.`,
    };
  }
  if (pct < -0.15) {
    return {
      shortLabel: "a la baja",
      description: `La demanda cae ${formatPercent(Math.abs(pct))} frente a la primera mitad del periodo.`,
    };
  }
  return {
    shortLabel: "estable",
    description: "La demanda luce estable entre la primera y la segunda mitad del periodo.",
  };
}

function computeHealthScore(summary) {
  const queryCount = Number(summary.query_count || 0);
  if (!queryCount) return 0;

  const errorRate = Number(summary.error_count || 0) / queryCount;
  const cacheRate = Number(summary.cache_hit_count || 0) / queryCount;
  const latency = Number(summary.p95_total_latency_ms || 0);

  const errorPenalty = Math.min(55, errorRate * 230);
  const latencyPenalty = latency <= 1500 ? 0 : Math.min(30, (latency - 1500) / 180);
  const cacheBonus = Math.min(10, cacheRate * 18);
  const score = Math.round(Math.max(0, Math.min(100, 100 - errorPenalty - latencyPenalty + cacheBonus)));
  return score;
}

function scoreTone(score) {
  if (score >= 85) {
    return {
      label: "Solida",
      background: "rgba(23,100,95,0.12)",
      color: "#17645f",
    };
  }
  if (score >= 65) {
    return {
      label: "Atendible",
      background: "rgba(203,127,61,0.16)",
      color: "#9a6a1d",
    };
  }
  return {
    label: "Fragil",
    background: "rgba(143,58,47,0.14)",
    color: "#8f3a2f",
  };
}

function aggregateBy(items, getLabel) {
  const map = new Map();
  for (const item of items) {
    const rawLabel = getLabel(item);
    const label = String(rawLabel || "Sin identificar").trim() || "Sin identificar";
    const existing = map.get(label) || {
      label,
      count: 0,
      totalCost: 0,
      totalTokens: 0,
    };
    existing.count += 1;
    existing.totalCost += Number(item.llm_totals?.total_cost_mxn || 0);
    existing.totalTokens += Number(item.llm_totals?.total_token_count || 0);
    map.set(label, existing);
  }
  return Array.from(map.values()).sort((a, b) => b.count - a.count || b.totalCost - a.totalCost);
}

function updatePaginationText() {
  if (!state.total) {
    elements.paginationText.textContent = "0 resultados";
    return;
  }
  const start = state.offset + 1;
  const end = Math.min(state.offset + state.limit, state.total);
  elements.paginationText.textContent = `${formatNumber(start)}-${formatNumber(end)} de ${formatNumber(state.total)}`;
}

function setPaginationLoading(isLoading) {
  elements.pagePrev.disabled = isLoading || state.offset <= 0;
  elements.pageNext.disabled = isLoading || state.offset + state.limit >= state.total;
  elements.exportCsv.disabled = isLoading || !state.queries.length;
  elements.exportJson.disabled = isLoading || !state.queries.length;
  updatePaginationText();
}

function setAuthStatus(text, tone) {
  elements.authStatusText.textContent = text;
  elements.authStatusText.className = tone ? `status-text ${tone}` : "status-text";
}

function setDashboardStatus(text, tone) {
  elements.dashboardStatusText.textContent = text;
  elements.dashboardStatusText.className = tone ? `status-text ${tone}` : "status-text";
}

async function fetchJson(path, { query } = {}) {
  const base = getStoredApiBase();
  const url = new URL(path, base);
  if (query instanceof URLSearchParams && query.toString()) {
    url.search = query.toString();
  }

  const response = await fetch(url.toString(), {
    headers: buildHeaders(),
  });
  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw new Error(payload.detail || `HTTP ${response.status}`);
  }
  return payload;
}

async function parseJsonResponse(response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

function normalizeApiBase(value) {
  let raw = String(value || "").trim();
  if (!raw) return "";
  if (/^https?:[^/]/i.test(raw)) {
    raw = raw.replace(/^https?:/i, (prefix) => `${prefix}//`);
  }
  if (!/^https?:\/\//i.test(raw)) {
    raw = `http://${raw.replace(/^\/+/, "")}`;
  }
  try {
    const url = new URL(raw);
    return `${url.origin}${url.pathname === "/" ? "" : url.pathname.replace(/\/+$/, "")}`;
  } catch {
    return "";
  }
}

function toIsoDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : date.toISOString();
}

function describePeriod() {
  const fromIso = toIsoDateTime(state.filters.from);
  const toIso = toIsoDateTime(state.filters.to);
  if (!fromIso && !toIso) return "Todo el historial";
  if (fromIso && toIso) return `${formatShortDate(fromIso)} a ${formatShortDate(toIso)}`;
  if (fromIso) return `Desde ${formatShortDate(fromIso)}`;
  return `Hasta ${formatShortDate(toIso)}`;
}

function formatTimestamp(value) {
  const date = new Date(value);
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
  if (Number.isNaN(date.getTime())) return "";
  return `${String(date.getDate()).padStart(2, "0")}/${String(date.getMonth() + 1).padStart(2, "0")}`;
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
  return new Intl.NumberFormat("es-MX", { maximumFractionDigits: 2 }).format(n);
}

function formatPercent(value) {
  const n = Number(value) || 0;
  return new Intl.NumberFormat("es-MX", {
    style: "percent",
    maximumFractionDigits: 1,
  }).format(n);
}

function formatCurrencyMxn(value) {
  const n = Number(value) || 0;
  return new Intl.NumberFormat("es-MX", {
    style: "currency",
    currency: "MXN",
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  }).format(n);
}

function formatCurrencyUsd(value) {
  const n = Number(value) || 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 4,
  }).format(n);
}

function shortMoney(value) {
  const n = Number(value) || 0;
  if (n >= 1000000) return `$${formatDecimal(n / 1000000)}M`;
  if (n >= 1000) return `$${formatDecimal(n / 1000)}k`;
  return `$${formatDecimal(n)}`;
}

function buildTicks(maxValue) {
  const safeMax = Math.max(Number(maxValue) || 0, 1);
  return Array.from({ length: 5 }, (_, index) => (safeMax * index) / 4);
}

function statusLabel(value) {
  const labels = {
    ok: "Resuelta",
    assistant_message: "Mensaje",
    needs_clarification: "Aclaracion",
    validation_error: "Validacion",
    rate_limited: "Rate limited",
    gemini_error: "Error Gemini",
    gemini_unavailable: "Gemini no disponible",
    not_found: "No encontrado",
    error: "Error",
  };
  return labels[value] || value || "Sin status";
}

function stageLabel(value) {
  const labels = {
    intent: "Intent",
    summary: "Summary",
  };
  return labels[value] || value || "Etapa";
}

function statusTone(status) {
  if (status === "ok") return "success";
  if (status === "assistant_message" || status === "needs_clarification") return "warn";
  return "error";
}

function exportCurrentQueriesAsCsv() {
  if (!state.queries.length) return;
  const headers = [
    "timestamp",
    "status",
    "dataset_id",
    "user_id",
    "question",
    "total_tokens",
    "total_cost_mxn",
    "total_latency_ms",
    "cache_hit",
    "response_summary",
    "error_message",
  ];

  const rows = state.queries.map((item) => [
    item.timestamp || "",
    item.status || "",
    item.dataset_id || "",
    item.user_id || "",
    item.question || "",
    item.llm_totals?.total_token_count || 0,
    item.llm_totals?.total_cost_mxn || 0,
    item.total_latency_ms || 0,
    item.cache_hit ? "true" : "false",
    item.response_summary || "",
    item.error_message || "",
  ]);

  const csv = [headers, ...rows]
    .map((row) => row.map(csvCell).join(","))
    .join("\r\n");

  downloadBlob(csv, "auditoria-consultas.csv", "text/csv;charset=utf-8");
}

function exportCurrentQueriesAsJson() {
  if (!state.queries.length) return;
  const payload = JSON.stringify(state.queries, null, 2);
  downloadBlob(payload, "auditoria-consultas.json", "application/json;charset=utf-8");
}

function csvCell(value) {
  const stringValue = String(value ?? "");
  if (/[",\r\n]/.test(stringValue)) {
    return `"${stringValue.replaceAll('"', '""')}"`;
  }
  return stringValue;
}

function downloadBlob(content, filename, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
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

  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
