const state = {
  datasets: [],
  selectedDatasetId: null,
  messages: [],
  isUploading: false,
  isSubmitting: false,
};

const palette = ["#166a63", "#d97a36", "#355c7d", "#8b5a2b", "#7a3b69", "#2d7a4f", "#a84a35"];
const STORAGE_KEYS = {
  chatHistory: "chat-history",
  userId: "csv-agent-user-id",
};

const elements = {
  uploadForm: document.getElementById("upload-form"),
  uploadFile: document.getElementById("upload-file"),
  uploadDisplayName: document.getElementById("upload-display-name"),
  uploadSubmit: document.getElementById("upload-submit"),
  uploadStatusText: document.getElementById("upload-status-text"),
  uploadStatusBadge: document.getElementById("upload-status-badge"),
  refreshDatasets: document.getElementById("refresh-datasets"),
  datasetList: document.getElementById("dataset-list"),
  activeDatasetChip: document.getElementById("active-dataset-chip"),
  selectedDatasetCard: document.getElementById("selected-dataset-card"),
  chatTranscript: document.getElementById("chat-transcript"),
  queryForm: document.getElementById("query-form"),
  queryInput: document.getElementById("query-input"),
  querySubmit: document.getElementById("query-submit"),
  queryStatusText: document.getElementById("query-status-text"),
  suggestions: document.getElementById("suggestions"),
  messageTemplate: document.getElementById("message-template"),
  clearChat: document.getElementById("clear-chat"),
  analyticsLink: document.getElementById("analytics-link"),
};

document.addEventListener("DOMContentLoaded", () => {
  bindEvents();
  bootstrap();
});

async function bootstrap() {
  ensureUserId();
  loadSessionHistory();
  if (!state.messages.length) {
    pushMessage({
      role: "agent",
      text: "Carga un CSV o selecciona un dataset para empezar. El agente se adapta a la estructura del archivo y te puede orientar con sugerencias basadas en ese dataset.",
    });
  }
  await loadDatasets();
  await hydrateActiveDataset();
  updateComposerState();
}

function bindEvents() {
  elements.uploadForm.addEventListener("submit", handleUploadSubmit);
  elements.refreshDatasets.addEventListener("click", loadDatasets);
  elements.queryForm.addEventListener("submit", handleQuerySubmit);
  elements.suggestions.addEventListener("click", handleSuggestionClick);
  elements.chatTranscript.addEventListener("click", handleTranscriptClick);

  // Send on Enter, new line on Shift+Enter
  elements.clearChat?.addEventListener("click", () => {
    state.messages = [];
    sessionStorage.removeItem(STORAGE_KEYS.chatHistory);
    renderChat();
    pushMessage({ role: "agent", text: "Chat limpiado. Puedes hacer una nueva consulta." });
  });

  elements.queryInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (!elements.querySubmit.disabled) {
        elements.queryForm.dispatchEvent(new Event("submit"));
      }
    }
  });
}

// ─── Session persistence ───────────────────────────────────────────────────

function saveSessionHistory() {
  try {
    const serializable = state.messages.filter((m) => m.role !== "thinking").map((m) => ({
      id: m.id,
      role: m.role,
      text: m.text,
      html: m.html || false,
      hints: m.hints || [],
      subtitle: m.subtitle || null,
      meta: m.meta || null,
      telemetry: m.telemetry || null,
    }));
    sessionStorage.setItem(STORAGE_KEYS.chatHistory, JSON.stringify(serializable));
  } catch (_) {}
}

function loadSessionHistory() {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEYS.chatHistory);
    if (!raw) return;
    const messages = JSON.parse(raw);
    if (Array.isArray(messages)) {
      state.messages = messages;
      renderChat();
    }
  } catch (_) {}
}

// ─── Data loading ──────────────────────────────────────────────────────────

async function loadDatasets() {
  setUploadStatus("Cargando datasets...", "warn", "Catalogo");
  try {
    const response = await fetch("/datasets");
    if (!response.ok) throw new Error("No se pudieron cargar los datasets.");
    const datasets = await response.json();
    state.datasets = datasets;

    if (!state.selectedDatasetId && datasets.length) {
      state.selectedDatasetId = datasets[0].id;
    } else if (state.selectedDatasetId && !datasets.some((d) => d.id === state.selectedDatasetId)) {
      state.selectedDatasetId = datasets[0]?.id ?? null;
    }

    renderDatasets();
    renderSelectedDataset();
    updateComposerState();
    updateAnalyticsLink();
    const label = datasets.length ? `${datasets.length} dataset${datasets.length === 1 ? "" : "s"}` : "Sin datasets";
    setUploadStatus(label, datasets.length ? "success" : "warn", "Catalogo");
  } catch (error) {
    renderDatasetsError(error.message);
    setUploadStatus(error.message, "error", "Error");
  }
}

async function hydrateActiveDataset() {
  try {
    const response = await fetch("/datasets/active", {
      headers: { "X-User-Id": ensureUserId() },
    });
    const payload = await parseJsonResponse(response);
    if (response.status === 404) {
      return;
    }
    if (!response.ok) {
      throw new Error(payload.detail || "No se pudo cargar el dataset activo.");
    }

    state.selectedDatasetId = payload.id;
    renderDatasets();
    renderSelectedDataset();
    updateComposerState();
    updateAnalyticsLink();
  } catch (error) {
    setUploadStatus(error.message, "error", "Error");
  }
}

async function setActiveDataset(datasetId) {
  const response = await fetch("/datasets/active", {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
      "X-User-Id": ensureUserId(),
    },
    body: JSON.stringify({ dataset_id: datasetId }),
  });
  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw new Error(payload.detail || "No se pudo activar el dataset.");
  }

  state.selectedDatasetId = payload.id;
  renderDatasets();
  renderSelectedDataset();
  updateComposerState();
  updateAnalyticsLink();
  return payload;
}

async function handleUploadSubmit(event) {
  event.preventDefault();
  if (state.isUploading) return;

  const file = elements.uploadFile.files[0];
  if (!file) {
    setUploadStatus("Selecciona un archivo CSV.", "error", "Falta archivo");
    return;
  }

  state.isUploading = true;
  toggleUploadUI();
  setUploadStatus("Subiendo y perfilando dataset...", "warn", "Subiendo");

  try {
    const formData = new FormData();
    formData.append("file", file);
    const displayName = elements.uploadDisplayName.value.trim();
    if (displayName) formData.append("metadata", JSON.stringify({ display_name: displayName }));

    const response = await fetch("/datasets/upload", {
      method: "POST",
      headers: { "X-User-Id": ensureUserId() },
      body: formData,
    });
    const payload = await parseJsonResponse(response);
    if (!response.ok) throw new Error(payload.detail || "No se pudo subir el dataset.");

    state.selectedDatasetId = payload.id;
    await loadDatasets();
    elements.uploadForm.reset();
    setUploadStatus(`Dataset listo: ${payload.display_name} (${formatNumber(payload.row_count)} filas)`, "success", "Listo");
    pushMessage({
      role: "system",
      text: `Dataset cargado: ${payload.display_name}. Ya puedes consultarlo en el chat.`,
    });
  } catch (error) {
    setUploadStatus(error.message, "error", "Error");
    pushMessage({ role: "system", text: error.message });
  } finally {
    state.isUploading = false;
    toggleUploadUI();
  }
}

async function handleQuerySubmit(event) {
  event.preventDefault();
  if (state.isSubmitting) return;

  const question = elements.queryInput.value.trim();
  const selectedDataset = getSelectedDataset();
  const history = buildHistoryPayload();
  if (!selectedDataset || !question) return;

  state.isSubmitting = true;
  let thinkingId = null;

  try {
    updateComposerState("Interpretando tu pregunta...");
    pushMessage({ role: "user", text: question });
    thinkingId = pushMessage({ role: "thinking", text: renderThinkingMarkup(), html: true });
    elements.queryInput.value = "";

    const response = await fetch("/query", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-User-Id": ensureUserId() },
      body: JSON.stringify({ dataset_id: selectedDataset.id, question, history }),
    });

    const payload = await parseJsonResponse(response);
    removeMessage(thinkingId);

    if (!response.ok) {
      const detail = typeof payload.detail === "string" ? payload.detail : "Ocurrio un error procesando tu consulta. Intenta reformular la pregunta.";
      const friendly =
        response.status === 502 || response.status === 503
          ? "El servicio de IA no esta disponible. Intenta de nuevo en unos segundos."
          : detail;
      pushMessage({
        role: response.status === 422 ? "agent" : "system",
        text: friendly,
        subtitle: response.status === 422 ? "Intenta reformular la pregunta." : null,
        telemetry: payload.telemetry || null,
      });
      return;
    }

    if (payload.status === "ok") {
      pushMessage({
        role: "agent",
        html: true,
        text: renderInlineResult(payload),
        telemetry: payload.telemetry || null,
      });
    } else if (payload.status === "assistant_message") {
      pushMessage({
        role: "agent",
        text: payload.message,
        hints: payload.hints || [],
        subtitle: payload.reason,
        telemetry: payload.telemetry || null,
      });
    } else if (payload.status === "needs_clarification") {
      pushMessage({
        role: "agent",
        text: payload.question,
        hints: payload.hints || [],
        subtitle: payload.reason,
        telemetry: payload.telemetry || null,
      });
    } else {
      throw new Error("La API devolvio un estado desconocido.");
    }
  } catch (error) {
    if (thinkingId) {
      removeMessage(thinkingId);
    }
    pushMessage({ role: "system", text: error?.message || "Ocurrio un error enviando tu consulta." });
  } finally {
    state.isSubmitting = false;
    updateComposerState();
  }
}

// ─── Inline result rendering ───────────────────────────────────────────────

function renderInlineResult(payload) {
  const summaryHtml = `<p class="inline-summary">${escapeHtml(payload.summary)}</p>`;

  const kpiHtml = payload.kpis?.length
    ? `<div class="inline-kpi-grid">${payload.kpis.map((kpi) => {
        const dir = kpi.direction || "flat";
        return `<div class="inline-kpi">
          <span class="inline-kpi-label">${escapeHtml(kpi.label)}</span>
          <span class="inline-kpi-value">${escapeHtml(formatValue(kpi.value))}</span>
          ${kpi.change ? `<span class="kpi-change ${dir}">${escapeHtml(kpi.change)}</span>` : ""}
        </div>`;
      }).join("")}</div>`
    : "";

  const chartHtml = payload.chart
    ? `<div class="inline-chart-wrapper">${renderChart(payload.chart)}</div>`
    : "";

  const tableHtml = payload.table?.columns?.length
    ? `<details class="inline-table-details"><summary>Ver tabla (${payload.table.rows.length} filas)</summary>${renderTable(payload.table)}</details>`
    : "";

  return summaryHtml + kpiHtml + chartHtml + tableHtml;
}

// ─── Chart rendering ───────────────────────────────────────────────────────

function renderChart(chart) {
  if (!chart) return `<div class="empty-state compact"><p>Sin datos para graficar.</p></div>`;

  if (chart.type === "pivot_table" && chart.pivot) return renderPivotTable(chart.pivot);
  if (!chart.series?.length || !chart.x?.length) return `<div class="empty-state compact"><p>Sin datos para graficar.</p></div>`;

  switch (chart.type) {
    case "line": return renderLineChart(chart);
    case "area": return renderAreaChart(chart);
    case "pie": return renderPieChart(chart);
    case "scatter": return renderScatterChart(chart);
    default: return renderBarChart(chart);
  }
}

function renderBarChart(chart) {
  const width = 680;
  const height = 300;
  const margin = { top: 20, right: 20, bottom: 64, left: 52 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const seriesCount = chart.series.length;
  const maxValue = Math.max(...chart.series.flatMap((s) => s.data.map((v) => Number(v) || 0)));
  const safeMax = maxValue || 1;
  const groupWidth = innerWidth / chart.x.length;
  const barWidth = Math.max(8, (groupWidth * 0.72) / seriesCount);

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de barras">`;
  // Y gridlines
  for (let i = 0; i <= 4; i++) {
    const y = margin.top + (innerHeight * (4 - i)) / 4;
    const val = (safeMax * i) / 4;
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
    svg += `<text x="${margin.left - 6}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5f6d70">${shortLabel(formatValue(val))}</text>`;
  }
  svg += `<line x1="${margin.left}" y1="${margin.top + innerHeight}" x2="${width - margin.right}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.24)" />`;

  chart.x.forEach((label, i) => {
    const groupX = margin.left + i * groupWidth + groupWidth * 0.14;
    chart.series.forEach((serie, si) => {
      const value = Number(serie.data[i]) || 0;
      const barHeight = (value / safeMax) * innerHeight;
      const x = groupX + si * barWidth;
      const y = margin.top + innerHeight - barHeight;
      svg += `<rect x="${x}" y="${y}" width="${barWidth - 3}" height="${Math.max(barHeight, 2)}" rx="6" fill="${palette[si % palette.length]}" />`;
    });
    svg += `<text x="${margin.left + i * groupWidth + groupWidth / 2}" y="${height - 22}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(shortLabel(label))}</text>`;
  });

  chart.series.forEach((serie, i) => {
    const lx = margin.left + i * 140;
    svg += `<rect x="${lx}" y="${height - 14}" width="10" height="10" rx="3" fill="${palette[i % palette.length]}" />`;
    svg += `<text x="${lx + 15}" y="${height - 5}" font-size="11" fill="#1f2a2c">${escapeHtml(serie.name)}</text>`;
  });

  svg += "</svg>";
  return svg;
}

function renderLineChart(chart) {
  const width = 680;
  const height = 300;
  const margin = { top: 18, right: 20, bottom: 64, left: 52 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const maxValue = Math.max(...chart.series.flatMap((s) => s.data.map((v) => Number(v) || 0)));
  const safeMax = maxValue || 1;
  const stepX = chart.x.length > 1 ? innerWidth / (chart.x.length - 1) : innerWidth / 2;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de linea">`;
  for (let i = 0; i <= 4; i++) {
    const y = margin.top + (innerHeight * (4 - i)) / 4;
    const val = (safeMax * i) / 4;
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
    svg += `<text x="${margin.left - 6}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5f6d70">${shortLabel(formatValue(val))}</text>`;
  }
  svg += `<line x1="${margin.left}" y1="${margin.top + innerHeight}" x2="${width - margin.right}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.24)" />`;

  chart.x.forEach((label, i) => {
    const x = margin.left + i * stepX;
    svg += `<text x="${x}" y="${height - 22}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(shortLabel(label))}</text>`;
  });

  chart.series.forEach((serie, si) => {
    const pts = serie.data.map((raw, i) => {
      const v = Number(raw) || 0;
      return { x: margin.left + i * stepX, y: margin.top + innerHeight - (v / safeMax) * innerHeight };
    });
    svg += `<polyline fill="none" stroke="${palette[si % palette.length]}" stroke-width="2.5" points="${pts.map((p) => `${p.x},${p.y}`).join(" ")}" />`;
    pts.forEach((p) => { svg += `<circle cx="${p.x}" cy="${p.y}" r="4" fill="${palette[si % palette.length]}" />`; });
    const lx = margin.left + si * 140;
    svg += `<rect x="${lx}" y="${height - 14}" width="10" height="10" rx="3" fill="${palette[si % palette.length]}" />`;
    svg += `<text x="${lx + 15}" y="${height - 5}" font-size="11" fill="#1f2a2c">${escapeHtml(serie.name)}</text>`;
  });

  svg += "</svg>";
  return svg;
}

function renderAreaChart(chart) {
  const width = 680;
  const height = 300;
  const margin = { top: 18, right: 20, bottom: 64, left: 52 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const maxValue = Math.max(...chart.series.flatMap((s) => s.data.map((v) => Number(v) || 0)));
  const safeMax = maxValue || 1;
  const stepX = chart.x.length > 1 ? innerWidth / (chart.x.length - 1) : innerWidth / 2;
  const baseY = margin.top + innerHeight;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de area">`;
  for (let i = 0; i <= 4; i++) {
    const y = margin.top + (innerHeight * (4 - i)) / 4;
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
  }
  svg += `<line x1="${margin.left}" y1="${baseY}" x2="${width - margin.right}" y2="${baseY}" stroke="rgba(31,42,44,0.24)" />`;

  chart.x.forEach((label, i) => {
    svg += `<text x="${margin.left + i * stepX}" y="${height - 22}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(shortLabel(label))}</text>`;
  });

  chart.series.forEach((serie, si) => {
    const color = palette[si % palette.length];
    const pts = serie.data.map((raw, i) => {
      const v = Number(raw) || 0;
      return { x: margin.left + i * stepX, y: margin.top + innerHeight - (v / safeMax) * innerHeight };
    });
    const pathD = `M ${pts[0].x},${baseY} ${pts.map((p) => `L ${p.x},${p.y}`).join(" ")} L ${pts[pts.length - 1].x},${baseY} Z`;
    svg += `<path d="${pathD}" fill="${color}" fill-opacity="0.18" />`;
    svg += `<polyline fill="none" stroke="${color}" stroke-width="2.5" points="${pts.map((p) => `${p.x},${p.y}`).join(" ")}" />`;
    const lx = margin.left + si * 140;
    svg += `<rect x="${lx}" y="${height - 14}" width="10" height="10" rx="3" fill="${color}" />`;
    svg += `<text x="${lx + 15}" y="${height - 5}" font-size="11" fill="#1f2a2c">${escapeHtml(serie.name)}</text>`;
  });

  svg += "</svg>";
  return svg;
}

function renderPieChart(chart) {
  const size = 300;
  const cx = size / 2;
  const cy = size / 2;
  const r = 110;
  const total = chart.series[0]?.data.reduce((a, v) => a + (Number(v) || 0), 0) || 1;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${size} ${size}" role="img" aria-label="Grafica de torta">`;
  let startAngle = -Math.PI / 2;

  chart.x.forEach((label, i) => {
    const value = Number(chart.series[0]?.data[i]) || 0;
    const angle = (value / total) * 2 * Math.PI;
    const endAngle = startAngle + angle;
    const x1 = cx + r * Math.cos(startAngle);
    const y1 = cy + r * Math.sin(startAngle);
    const x2 = cx + r * Math.cos(endAngle);
    const y2 = cy + r * Math.sin(endAngle);
    const largeArc = angle > Math.PI ? 1 : 0;
    const color = palette[i % palette.length];
    svg += `<path d="M ${cx},${cy} L ${x1},${y1} A ${r},${r} 0 ${largeArc},1 ${x2},${y2} Z" fill="${color}" stroke="#fff" stroke-width="2">`;
    svg += `<title>${escapeHtml(String(label))}: ${formatValue(value)} (${((value / total) * 100).toFixed(1)}%)</title></path>`;
    // Percentage label
    const midAngle = startAngle + angle / 2;
    const lx = cx + (r * 0.65) * Math.cos(midAngle);
    const ly = cy + (r * 0.65) * Math.sin(midAngle);
    if (angle > 0.25) {
      svg += `<text x="${lx}" y="${ly}" text-anchor="middle" font-size="11" fill="#fff" font-weight="700">${((value / total) * 100).toFixed(0)}%</text>`;
    }
    startAngle = endAngle;
  });

  // Legend
  chart.x.forEach((label, i) => {
    const ly = size - 14 - (chart.x.length - 1 - i) * 16;
    svg += `<rect x="4" y="${ly - 8}" width="10" height="10" rx="3" fill="${palette[i % palette.length]}" />`;
    svg += `<text x="18" y="${ly}" font-size="10" fill="#1f2a2c">${escapeHtml(shortLabel(String(label)))}</text>`;
  });

  svg += "</svg>";
  return svg;
}

function renderScatterChart(chart) {
  const width = 680;
  const height = 300;
  const margin = { top: 20, right: 20, bottom: 64, left: 60 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const xVals = chart.x.map((v) => Number(v) || 0);
  const yVals = chart.series[0]?.data.map((v) => Number(v) || 0) || [];
  const xMax = Math.max(...xVals) || 1;
  const yMax = Math.max(...yVals) || 1;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de dispersion">`;
  svg += `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.2)" />`;
  svg += `<line x1="${margin.left}" y1="${margin.top + innerHeight}" x2="${width - margin.right}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.2)" />`;

  xVals.forEach((xv, i) => {
    const yv = yVals[i] || 0;
    const cx = margin.left + (xv / xMax) * innerWidth;
    const cy = margin.top + innerHeight - (yv / yMax) * innerHeight;
    svg += `<circle cx="${cx}" cy="${cy}" r="5" fill="${palette[0]}" fill-opacity="0.72" />`;
  });

  svg += `<text x="${width / 2}" y="${height - 5}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(chart.series[0]?.name || "X")}</text>`;
  svg += "</svg>";
  return svg;
}

function renderPivotTable(pivot) {
  if (!pivot) return `<div class="empty-state compact"><p>Sin datos para tabla pivote.</p></div>`;

  const fmtVal = (v) => (v === null || v === undefined) ? "-" : escapeHtml(formatValue(v));

  let html = `<div class="table-wrapper"><table class="pivot-table">`;
  // Header
  html += `<thead><tr><th>${escapeHtml(pivot.row_dimension)}</th>`;
  for (const col of pivot.cols) {
    html += `<th>${escapeHtml(String(col))}</th>`;
  }
  html += `<th class="pivot-total">Total</th></tr></thead>`;
  // Body
  html += `<tbody>`;
  pivot.rows.forEach((row, ri) => {
    html += `<tr><td><strong>${escapeHtml(String(row))}</strong></td>`;
    pivot.data[ri].forEach((cell) => {
      html += `<td>${fmtVal(cell)}</td>`;
    });
    html += `<td class="pivot-total">${fmtVal(pivot.row_totals[ri])}</td></tr>`;
  });
  // Col totals
  html += `<tr class="pivot-total-row"><td><strong>Total</strong></td>`;
  for (const ct of pivot.col_totals) {
    html += `<td>${fmtVal(ct)}</td>`;
  }
  html += `<td class="pivot-total">${fmtVal(pivot.grand_total)}</td></tr>`;
  html += `</tbody></table></div>`;
  return html;
}

function renderTable(table) {
  if (!table?.columns?.length) return `<div class="empty-state compact"><p>Sin filas para mostrar.</p></div>`;
  const head = table.columns.map((c) => `<th>${escapeHtml(c)}</th>`).join("");
  const rows = table.rows.length
    ? table.rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(formatValue(cell))}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${table.columns.length}">Sin filas para mostrar.</td></tr>`;
  return `<div class="table-wrapper"><table><thead><tr>${head}</tr></thead><tbody>${rows}</tbody></table></div>`;
}

// ─── Dataset rendering ─────────────────────────────────────────────────────

function renderDatasets() {
  const selected = getSelectedDataset();
  if (!state.datasets.length) {
    elements.datasetList.innerHTML = `
      <div class="empty-state">
        <h3>Sin datasets cargados</h3>
        <p>Sube un CSV desde el panel superior para poblar el catalogo.</p>
      </div>`;
    elements.activeDatasetChip.textContent = "Sin dataset seleccionado";
    updateAnalyticsLink();
    return;
  }

  elements.datasetList.innerHTML = "";
  for (const dataset of state.datasets) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `dataset-item${dataset.id === state.selectedDatasetId ? " active" : ""}`;
    button.innerHTML = `
      <div class="dataset-row">
        <strong>${escapeHtml(dataset.display_name)}</strong>
        <span class="badge">${formatNumber(dataset.row_count)} filas</span>
      </div>
      <div class="dataset-meta">${escapeHtml(dataset.filename)}</div>
      <div class="dataset-meta">${dataset.metrics_allowed.length} metricas · ${dataset.dimensions_allowed.length} dimensiones</div>`;
    button.addEventListener("click", async () => {
      try {
        setUploadStatus("Actualizando dataset activo...", "warn", "Catalogo");
        const active = await setActiveDataset(dataset.id);
        setUploadStatus(`Dataset activo: ${active.display_name}`, "success", "Activo");
      } catch (error) {
        setUploadStatus(error.message, "error", "Error");
        pushMessage({ role: "system", text: error.message });
      }
    });
    elements.datasetList.appendChild(button);
  }
  elements.activeDatasetChip.textContent = selected ? selected.display_name : "Sin dataset seleccionado";
  updateAnalyticsLink();
}

function renderDatasetsError(message) {
  elements.datasetList.innerHTML = `
    <div class="empty-state">
      <h3>No pude cargar el catalogo</h3>
      <p>${escapeHtml(message)}</p>
    </div>`;
}

function renderSelectedDataset() {
  const dataset = getSelectedDataset();
  if (!dataset) {
    elements.selectedDatasetCard.innerHTML = `
      <div class="empty-state compact">
        <h3>Selecciona un dataset</h3>
        <p>Al cargar o elegir un CSV veras aqui su resumen operativo.</p>
      </div>`;
    renderSuggestions(null);
    return;
  }

  const metrics = (dataset.suggested_metrics || [])
    .map((name) => metricLabelByName(dataset, name))
    .filter(Boolean)
    .slice(0, 4)
    .join(", ");
  const dimensions = (dataset.suggested_dimensions || [])
    .map((name) => dimensionLabelByName(dataset, name))
    .filter(Boolean)
    .slice(0, 4)
    .join(", ");

  elements.selectedDatasetCard.innerHTML = `
    <p class="eyebrow">Dataset activo</p>
    <h3>${escapeHtml(dataset.display_name)}</h3>
    <p class="muted">${escapeHtml(dataset.filename)}</p>
    <div class="meta-list">
      <div class="meta-pill"><strong>Filas</strong><br />${formatNumber(dataset.row_count)}</div>
      <div class="meta-pill"><strong>Fecha</strong><br />${escapeHtml(dataset.default_date_column || "No detectada")}</div>
      <div class="meta-pill"><strong>Metrica sugerida</strong><br />${escapeHtml(metricLabelByName(dataset, dataset.default_metric) || "Sin metrica sugerida")}</div>
      <div class="meta-pill"><strong>Dimensiones sugeridas</strong><br />${escapeHtml(dimensions || "Sin dimensiones sugeridas")}</div>
    </div>`;

  renderSuggestions(dataset);
}

// ─── Dynamic suggestions ───────────────────────────────────────────────────

function renderSuggestions(dataset) {
  if (!dataset) {
    elements.suggestions.innerHTML = "";
    return;
  }

  const suggestions = buildSuggestions(dataset);
  elements.suggestions.innerHTML = suggestions
    .map((q) => `<button class="suggestion" type="button" data-question="${escapeAttr(q)}">${escapeHtml(q)}</button>`)
    .join("");
}

function buildSuggestions(dataset) {
  const suggestions = [];

  const metricLabel = metricLabelByName(dataset, dataset.default_metric);
  const dim = dataset.suggested_dimensions?.[0];
  const dimLabel = dim ? dimensionLabelByName(dataset, dim) : null;

  if (metricLabel) {
    suggestions.push(`resumen general de ${metricLabel}`);
  } else {
    suggestions.push("resumen general del dataset");
  }

  if (metricLabel && dimLabel) {
    suggestions.push(`${metricLabel} por ${dimLabel}`);
    suggestions.push(`top 5 ${dimLabel} por ${metricLabel}`);
  }

  if (metricLabel && dataset.default_date_column) {
    suggestions.push(`tendencia mensual de ${metricLabel}`);
  }

  return suggestions.slice(0, 4);
}

function handleSuggestionClick(event) {
  const button = event.target.closest(".suggestion");
  if (!button) return;
  elements.queryInput.value = button.dataset.question || "";
  elements.queryInput.focus();
}

function handleTranscriptClick(event) {
  const button = event.target.closest(".hint-action");
  if (!button) return;
  const hint = button.dataset.hint || "";
  elements.queryInput.value = hint;
  elements.queryInput.focus();
}

// ─── Chat rendering ────────────────────────────────────────────────────────

function renderChat() {
  elements.chatTranscript.innerHTML = "";
  for (const message of state.messages) {
    const fragment = elements.messageTemplate.content.cloneNode(true);
    const article = fragment.querySelector(".message");
    const label = fragment.querySelector(".message-label");
    const body = fragment.querySelector(".message-body");

    article.classList.add(message.role);
    article.dataset.messageId = message.id;
    label.textContent = labelForRole(message.role, message.subtitle);

    if (message.html) {
      body.innerHTML = message.text;
    } else {
      body.textContent = message.text;
    }

    if (message.hints?.length) {
      const list = document.createElement("div");
      list.className = "hint-actions";
      for (const hint of message.hints) {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "suggestion hint-action";
        item.dataset.hint = hint;
        item.textContent = hint;
        list.appendChild(item);
      }
      body.appendChild(list);
    }

    if (message.meta && !message.html) {
      const meta = document.createElement("p");
      meta.className = "status-text";
      meta.textContent = message.meta;
      body.appendChild(meta);
    }

    if (message.telemetry) {
      const telemetry = document.createElement("div");
      telemetry.className = "telemetry-shell";
      telemetry.innerHTML = renderTelemetry(message.telemetry);
      body.appendChild(telemetry);
    }

    elements.chatTranscript.appendChild(fragment);
  }
  elements.chatTranscript.scrollTop = elements.chatTranscript.scrollHeight;
  saveSessionHistory();
}

// ─── State helpers ─────────────────────────────────────────────────────────

function updateComposerState(overrideText) {
  const hasDataset = Boolean(getSelectedDataset());
  const disabled = !hasDataset || state.isSubmitting;
  elements.querySubmit.disabled = disabled;
  elements.queryInput.disabled = !hasDataset || state.isSubmitting;

  if (overrideText) {
    elements.queryStatusText.textContent = overrideText;
    elements.queryStatusText.className = "status-text warn";
    return;
  }

  if (!hasDataset) {
    elements.queryStatusText.textContent = "Selecciona un dataset para habilitar el chat.";
    elements.queryStatusText.className = "status-text";
  } else if (state.isSubmitting) {
    elements.queryStatusText.textContent = "Ejecutando consulta...";
    elements.queryStatusText.className = "status-text warn";
  } else {
    elements.queryStatusText.textContent = "Listo para consultar.";
    elements.queryStatusText.className = "status-text success";
  }
}

function toggleUploadUI() {
  elements.uploadSubmit.disabled = state.isUploading;
  elements.uploadFile.disabled = state.isUploading;
  elements.uploadDisplayName.disabled = state.isUploading;
}

function setUploadStatus(text, tone, badgeLabel) {
  elements.uploadStatusText.textContent = text;
  elements.uploadStatusText.className = `status-text ${tone}`;
  elements.uploadStatusBadge.textContent = badgeLabel;
}

function pushMessage(message) {
  const id = generateId();
  state.messages.push({ id, ...message });
  renderChat();
  return id;
}

function removeMessage(messageId) {
  state.messages = state.messages.filter((m) => m.id !== messageId);
  renderChat();
}

function getSelectedDataset() {
  return state.datasets.find((d) => d.id === state.selectedDatasetId) || null;
}

function buildHistoryPayload() {
  return state.messages
    .filter((m) => m.role === "user" || m.role === "agent")
    .slice(-8)
    .map((m) => ({ role: m.role, text: m.html ? extractTextFromHtml(m.text) : m.text }));
}

function extractTextFromHtml(html) {
  const div = document.createElement("div");
  div.innerHTML = html;
  return div.querySelector(".inline-summary")?.textContent || div.textContent || "";
}

function ensureUserId() {
  let userId = window.localStorage.getItem(STORAGE_KEYS.userId);
  if (!userId) {
    userId = `web-${generateId()}`;
    window.localStorage.setItem(STORAGE_KEYS.userId, userId);
  }
  return userId;
}

function updateAnalyticsLink() {
  if (!elements.analyticsLink) return;
  const dataset = getSelectedDataset();
  elements.analyticsLink.href = dataset ? `/analytics?dataset_id=${encodeURIComponent(dataset.id)}` : "/analytics";
}

function labelForRole(role, subtitle) {
  if (role === "user") return "Tu consulta";
  if (role === "thinking") return "Agente";
  if (role === "system") return "Sistema";
  return subtitle ? `Agente · ${subtitle}` : "Agente";
}

function renderThinkingMarkup() {
  return `<span class="typing-dots" aria-label="Pensando"><span></span><span></span><span></span></span>`;
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

// ─── Parse/format helpers ──────────────────────────────────────────────────

async function parseJsonResponse(response) {
  const text = await response.text();
  if (!text) return {};
  try { return JSON.parse(text); } catch { return { detail: text }; }
}

function formatNumber(value) {
  const n = Number(value);
  if (Number.isNaN(n)) return String(value);
  return new Intl.NumberFormat("es-MX").format(n);
}

function formatValue(value) {
  if (value === null || value === undefined || value === "") return "N/A";
  if (typeof value === "number") {
    return new Intl.NumberFormat("es-MX", { maximumFractionDigits: 2 }).format(value);
  }
  return String(value);
}

function metricLabelByName(dataset, metricName) {
  if (!metricName) return null;
  const metric = dataset.metrics_allowed?.find((item) => item.name === metricName);
  if (!metric) return metricName;
  if (metricName === "row_count") return "conteo de registros";
  return (metric.source_column || metric.label || metric.name).toString().replaceAll("_", " ").toLowerCase();
}

function dimensionLabelByName(dataset, dimensionName) {
  if (!dimensionName) return null;
  const clean = String(dimensionName).replaceAll("_", " ");
  return clean.toLowerCase();
}

function shortLabel(value) {
  const text = String(value);
  return text.length > 12 ? `${text.slice(0, 12)}...` : text;
}

function renderTelemetry(telemetry) {
  const totals = telemetry.llm_totals || {};
  const stages = telemetry.stages || {};
  const calls = Array.isArray(telemetry.llm_calls) ? telemetry.llm_calls : [];
  const stageItems = [
    ["cache", stages.cache_lookup_ms || 0],
    ["intent", stages.intent_ms || 0],
    ["query", stages.query_execution_ms || 0],
    ["build", stages.response_build_ms || 0],
    ["summary", stages.summary_ms || 0],
  ];
  const stageHtml = stageItems
    .map(([label, value]) => `<span class="telemetry-chip"><strong>${escapeHtml(label)}</strong> ${formatNumber(value)} ms</span>`)
    .join("");
  const callsHtml = calls.length
    ? calls.map((call) => `
        <div class="telemetry-call">
          <div class="telemetry-call-head">
            <span>${escapeHtml(call.stage || "llm")}</span>
            <span>${escapeHtml(call.model || "modelo")}</span>
            <span>${formatNumber(call.latency_ms || 0)} ms</span>
          </div>
          <div class="telemetry-call-body">
            <span>input ${formatNumber(call.input_token_count || 0)}</span>
            <span>output ${formatNumber(call.output_token_count || 0)}</span>
            <span>thinking ${formatNumber(call.thinking_token_count || 0)}</span>
            <span>total ${formatNumber(call.total_token_count || 0)}</span>
            <span>entrada ${formatCurrencyMxn(call.input_cost_mxn || 0)}</span>
            <span>salida ${formatCurrencyMxn(call.output_cost_mxn || 0)}</span>
            <span>thinking ${formatCurrencyMxn(call.thinking_cost_mxn || 0)}</span>
            <span>total ${formatCurrencyMxn(call.total_cost_mxn || 0)}</span>
            <span>${formatCurrencyUsd(call.total_cost_usd || 0)}</span>
          </div>
        </div>
      `).join("")
    : `<p class="telemetry-empty">Sin llamadas LLM en esta respuesta.</p>`;

  return `
    <div class="telemetry-strip">
      <span class="telemetry-badge ${telemetry.cache_hit ? "cache" : "fresh"}">${telemetry.cache_hit ? "cache" : "fresh"}</span>
      <span><strong>${formatNumber(totals.input_token_count || 0)}</strong> input</span>
      <span><strong>${formatNumber(totals.output_token_count || 0)}</strong> output</span>
      <span><strong>${formatNumber(totals.thinking_token_count || 0)}</strong> thinking</span>
      <span><strong>${formatNumber(totals.total_token_count || 0)}</strong> tokens</span>
      <span><strong>${formatCurrencyMxn(totals.total_cost_mxn || 0)}</strong></span>
      <span><strong>${formatNumber(telemetry.total_latency_ms || 0)}</strong> ms</span>
    </div>
    <details class="telemetry-details">
      <summary>Ver consumo por etapa</summary>
      <div class="telemetry-stage-list">${stageHtml}</div>
      <div class="telemetry-stage-list">
        <span class="telemetry-chip"><strong>entrada</strong> ${formatCurrencyMxn(totals.input_cost_mxn || 0)}</span>
        <span class="telemetry-chip"><strong>salida</strong> ${formatCurrencyMxn(totals.output_cost_mxn || 0)}</span>
        <span class="telemetry-chip"><strong>thinking</strong> ${formatCurrencyMxn(totals.thinking_cost_mxn || 0)}</span>
        <span class="telemetry-chip"><strong>total</strong> ${formatCurrencyMxn(totals.total_cost_mxn || 0)}</span>
        <span class="telemetry-chip"><strong>fx</strong> ${formatNumber(totals.usd_to_mxn_rate || 0)} MXN/USD</span>
      </div>
      <div class="telemetry-call-list">${callsHtml}</div>
    </details>
  `;
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

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return String(value).replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
