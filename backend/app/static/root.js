const state = {
  datasets: [],
  activeDataset: null,
  messages: [],
  isUploading: false,
  isSubmitting: false,
};

const palette = ["#166a63", "#d97a36", "#355c7d", "#8b5a2b", "#7a3b69", "#2d7a4f", "#a84a35"];
const STORAGE_KEYS = {
  chatHistory: "chat-history-prod",
  userId: "csv-agent-user-id",
};

const elements = {
  activeDatasetChip: document.getElementById("active-dataset-chip"),
  datasetStateText: document.getElementById("dataset-state-text"),
  selectorStage: document.getElementById("selector-stage"),
  selectorList: document.getElementById("selector-list"),
  uploadStage: document.getElementById("upload-stage"),
  uploadForm: document.getElementById("upload-form"),
  uploadFile: document.getElementById("upload-file"),
  uploadDisplayName: document.getElementById("upload-display-name"),
  uploadSubmit: document.getElementById("upload-submit"),
  uploadStatusText: document.getElementById("upload-status-text"),
  uploadStatusBadge: document.getElementById("upload-status-badge"),
  chatTranscript: document.getElementById("chat-transcript"),
  promptCard: document.getElementById("prompt-card"),
  queryForm: document.getElementById("query-form"),
  queryInput: document.getElementById("query-input"),
  querySubmit: document.getElementById("query-submit"),
  queryStatusText: document.getElementById("query-status-text"),
  suggestions: document.getElementById("suggestions"),
  messageTemplate: document.getElementById("message-template"),
  clearChat: document.getElementById("clear-chat"),
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
      text: "Activa un CSV para comenzar. Cuando haya un dataset activo, podras consultarlo desde este chat.",
    });
  }

  await hydrateExperience();
  renderChat();
  updateViewState();
  updateComposerState();
}

function bindEvents() {
  elements.uploadForm?.addEventListener("submit", handleUploadSubmit);
  elements.queryForm.addEventListener("submit", handleQuerySubmit);
  elements.suggestions.addEventListener("click", handleSuggestionClick);
  elements.selectorList.addEventListener("click", handleSelectorClick);
  elements.chatTranscript.addEventListener("click", handleTranscriptClick);

  elements.clearChat?.addEventListener("click", () => {
    state.messages = [];
    sessionStorage.removeItem(STORAGE_KEYS.chatHistory);
    renderChat();
    pushMessage({
      role: "agent",
      text: state.activeDataset
        ? `Chat limpiado. Puedes seguir consultando ${state.activeDataset.display_name}.`
        : "Chat limpiado. Activa un CSV para continuar.",
    });
  });

  elements.queryInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      if (!elements.querySubmit.disabled) {
        elements.queryForm.dispatchEvent(new Event("submit"));
      }
    }
  });
}

async function hydrateExperience() {
  setDatasetState("Cargando dataset activo...", "warn");

  try {
    const activeDataset = await loadActiveDataset();
    if (activeDataset) {
      state.activeDataset = activeDataset;
      return;
    }

    await loadDatasets();
  } catch (error) {
    setDatasetState(error.message, "error");
  }
}

async function loadActiveDataset() {
  const response = await fetch("/datasets/active", {
    headers: { "X-User-Id": ensureUserId() },
  });
  const payload = await parseJsonResponse(response);

  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error(payload.detail || "No se pudo cargar el dataset activo.");
  }
  return payload;
}

async function loadDatasets() {
  const response = await fetch("/datasets");
  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw new Error(payload.detail || "No se pudieron cargar los datasets.");
  }

  state.datasets = Array.isArray(payload) ? payload : [];
}

async function activateDataset(datasetId) {
  setDatasetState("Activando CSV...", "warn");

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

  state.activeDataset = payload;
  updateViewState();
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
  setUploadStatus("Subiendo y activando CSV...", "warn", "Subiendo");

  try {
    const formData = new FormData();
    formData.append("file", file);
    const displayName = elements.uploadDisplayName.value.trim();
    if (displayName) {
      formData.append("metadata", JSON.stringify({ display_name: displayName }));
    }

    const response = await fetch("/datasets/upload", {
      method: "POST",
      headers: { "X-User-Id": ensureUserId() },
      body: formData,
    });
    const payload = await parseJsonResponse(response);
    if (!response.ok) {
      throw new Error(payload.detail || "No se pudo subir el dataset.");
    }

    state.activeDataset = payload;
    state.datasets = [payload];
    elements.uploadForm.reset();
    setUploadStatus(
      `Dataset listo: ${payload.display_name} (${formatNumber(payload.row_count)} filas)`,
      "success",
      "Listo",
    );
    updateViewState();
  } catch (error) {
    setUploadStatus(error.message, "error", "Error");
  } finally {
    state.isUploading = false;
    toggleUploadUI();
  }
}

async function handleQuerySubmit(event) {
  event.preventDefault();
  if (state.isSubmitting) return;

  const question = elements.queryInput.value.trim();
  const history = buildHistoryPayload();
  if (!state.activeDataset || !question) return;

  state.isSubmitting = true;
  let thinkingId = null;

  try {
    updateComposerState("Interpretando tu pregunta...");
    pushMessage({ role: "user", text: question });
    thinkingId = pushMessage({ role: "thinking", text: renderThinkingMarkup(), html: true });
    elements.queryInput.value = "";

    const response = await fetch("/query", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-User-Id": ensureUserId(),
      },
      body: JSON.stringify({
        dataset_id: state.activeDataset.id,
        question,
        history,
      }),
    });

    const payload = await parseJsonResponse(response);
    removeMessage(thinkingId);

    if (!response.ok) {
      const detail = typeof payload.detail === "string"
        ? payload.detail
        : "Ocurrio un error procesando tu consulta. Intenta reformular la pregunta.";
      const friendly = response.status === 502 || response.status === 503
        ? "El servicio de IA no esta disponible. Intenta de nuevo en unos segundos."
        : detail;
      pushMessage({
        role: response.status === 422 ? "agent" : "system",
        text: friendly,
        subtitle: response.status === 422 ? "Intenta reformular la pregunta." : null,
      });
      return;
    }

    if (payload.status === "ok") {
      pushMessage({
        role: "agent",
        html: true,
        text: renderInlineResult(payload),
      });
    } else if (payload.status === "assistant_message") {
      pushMessage({
        role: "agent",
        text: payload.message,
        hints: payload.hints || [],
        subtitle: payload.reason,
      });
    } else if (payload.status === "needs_clarification") {
      pushMessage({
        role: "agent",
        text: payload.question,
        hints: payload.hints || [],
        subtitle: payload.reason,
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

function handleSuggestionClick(event) {
  const button = event.target.closest(".suggestion");
  if (!button) return;
  elements.queryInput.value = button.dataset.question || "";
  elements.queryInput.focus();
}

function handleSelectorClick(event) {
  const button = event.target.closest("[data-dataset-id]");
  if (!button) return;

  activateDataset(button.dataset.datasetId).catch((error) => {
    setDatasetState(error.message, "error");
  });
}

function handleTranscriptClick(event) {
  const button = event.target.closest(".hint-action");
  if (!button) return;
  elements.queryInput.value = button.dataset.hint || "";
  elements.queryInput.focus();
}

function updateViewState() {
  const hasActiveDataset = Boolean(state.activeDataset);
  const hasDatasets = state.datasets.length > 0;

  elements.activeDatasetChip.textContent = hasActiveDataset
    ? state.activeDataset.display_name
    : "Sin dataset activo";
  elements.chatTranscript.hidden = !hasActiveDataset;
  elements.promptCard.hidden = !hasActiveDataset;
  elements.selectorStage.hidden = hasActiveDataset || !hasDatasets;
  elements.uploadStage.hidden = hasActiveDataset || hasDatasets;

  if (hasActiveDataset) {
    setDatasetState(`CSV activo: ${state.activeDataset.display_name}`, "success");
    renderSuggestions(state.activeDataset);
  } else if (hasDatasets) {
    setDatasetState("Selecciona el CSV que quieres activar para este navegador.", "warn");
    renderSelectorList();
    renderSuggestions(null);
  } else {
    setDatasetState("No hay datasets activos. Sube un CSV para comenzar.", "warn");
    renderSuggestions(null);
  }

  renderChat();
  updateComposerState();
}

function renderSelectorList() {
  elements.selectorList.innerHTML = state.datasets.map((dataset) => `
    <button class="dataset-item prod-choice-item" type="button" data-dataset-id="${escapeAttr(dataset.id)}">
      <div class="dataset-row">
        <strong>${escapeHtml(dataset.display_name)}</strong>
        <span class="badge">${formatNumber(dataset.row_count)} filas</span>
      </div>
      <div class="dataset-meta">${escapeHtml(dataset.filename)}</div>
      <div class="dataset-meta">${dataset.metrics_allowed.length} metricas · ${dataset.dimensions_allowed.length} dimensiones</div>
    </button>
  `).join("");
}

function renderSuggestions(dataset) {
  if (!dataset) {
    elements.suggestions.innerHTML = "";
    return;
  }

  const suggestions = buildSuggestions(dataset);
  elements.suggestions.innerHTML = suggestions
    .map((question) => (
      `<button class="suggestion" type="button" data-question="${escapeAttr(question)}">${escapeHtml(question)}</button>`
    ))
    .join("");
}

function buildSuggestions(dataset) {
  const suggestions = [];

  const metricLabel = metricLabelByName(dataset, dataset.default_metric);
  const dimensionName = dataset.suggested_dimensions?.[0];
  const dimensionLabel = dimensionName ? dimensionLabelByName(dataset, dimensionName) : null;

  if (metricLabel) {
    suggestions.push(`resumen general de ${metricLabel}`);
  } else {
    suggestions.push("resumen general del dataset");
  }

  if (metricLabel && dimensionLabel) {
    suggestions.push(`${metricLabel} por ${dimensionLabel}`);
    suggestions.push(`top 5 ${dimensionLabel} por ${metricLabel}`);
  }

  if (metricLabel && dataset.default_date_column) {
    suggestions.push(`tendencia mensual de ${metricLabel}`);
  }

  return suggestions.slice(0, 4);
}

function saveSessionHistory() {
  try {
    const serializable = state.messages
      .filter((message) => message.role !== "thinking")
      .map((message) => ({
        id: message.id,
        role: message.role,
        text: message.text,
        html: message.html || false,
        hints: message.hints || [],
        subtitle: message.subtitle || null,
        meta: message.meta || null,
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
    }
  } catch (_) {}
}

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

    elements.chatTranscript.appendChild(fragment);
  }
  elements.chatTranscript.scrollTop = elements.chatTranscript.scrollHeight;
  saveSessionHistory();
}

function updateComposerState(overrideText) {
  const hasDataset = Boolean(state.activeDataset);
  const disabled = !hasDataset || state.isSubmitting;
  elements.querySubmit.disabled = disabled;
  elements.queryInput.disabled = disabled;

  if (overrideText) {
    elements.queryStatusText.textContent = overrideText;
    elements.queryStatusText.className = "status-text warn";
    return;
  }

  if (!hasDataset) {
    elements.queryStatusText.textContent = "Activa un dataset para habilitar el chat.";
    elements.queryStatusText.className = "status-text";
  } else if (state.isSubmitting) {
    elements.queryStatusText.textContent = "Ejecutando consulta...";
    elements.queryStatusText.className = "status-text warn";
  } else {
    elements.queryStatusText.textContent = "Listo para consultar.";
    elements.queryStatusText.className = "status-text success";
  }
}

function setDatasetState(text, tone) {
  elements.datasetStateText.textContent = text;
  elements.datasetStateText.className = tone ? `status-text ${tone}` : "status-text";
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
  state.messages = state.messages.filter((message) => message.id !== messageId);
  renderChat();
}

function buildHistoryPayload() {
  return state.messages
    .filter((message) => message.role === "user" || message.role === "agent")
    .slice(-8)
    .map((message) => ({
      role: message.role,
      text: message.html ? extractTextFromHtml(message.text) : message.text,
    }));
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

function labelForRole(role, subtitle) {
  if (role === "user") return "Tu consulta";
  if (role === "thinking") return "Agente";
  if (role === "system") return "Sistema";
  return subtitle ? `Agente · ${subtitle}` : "Agente";
}

function renderThinkingMarkup() {
  return `<span class="typing-dots" aria-label="Pensando"><span></span><span></span><span></span></span>`;
}

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
  const maxValue = Math.max(...chart.series.flatMap((series) => series.data.map((value) => Number(value) || 0)));
  const safeMax = maxValue || 1;
  const groupWidth = innerWidth / chart.x.length;
  const barWidth = Math.max(8, (groupWidth * 0.72) / seriesCount);

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de barras">`;
  for (let index = 0; index <= 4; index += 1) {
    const y = margin.top + (innerHeight * (4 - index)) / 4;
    const value = (safeMax * index) / 4;
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
    svg += `<text x="${margin.left - 6}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5f6d70">${shortLabel(formatValue(value))}</text>`;
  }
  svg += `<line x1="${margin.left}" y1="${margin.top + innerHeight}" x2="${width - margin.right}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.24)" />`;

  chart.x.forEach((label, index) => {
    const groupX = margin.left + index * groupWidth + groupWidth * 0.14;
    chart.series.forEach((series, seriesIndex) => {
      const value = Number(series.data[index]) || 0;
      const barHeight = (value / safeMax) * innerHeight;
      const x = groupX + seriesIndex * barWidth;
      const y = margin.top + innerHeight - barHeight;
      svg += `<rect x="${x}" y="${y}" width="${barWidth - 3}" height="${Math.max(barHeight, 2)}" rx="6" fill="${palette[seriesIndex % palette.length]}" />`;
    });
    svg += `<text x="${margin.left + index * groupWidth + groupWidth / 2}" y="${height - 22}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(shortLabel(label))}</text>`;
  });

  chart.series.forEach((series, index) => {
    const legendX = margin.left + index * 140;
    svg += `<rect x="${legendX}" y="${height - 14}" width="10" height="10" rx="3" fill="${palette[index % palette.length]}" />`;
    svg += `<text x="${legendX + 15}" y="${height - 5}" font-size="11" fill="#1f2a2c">${escapeHtml(series.name)}</text>`;
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
  const maxValue = Math.max(...chart.series.flatMap((series) => series.data.map((value) => Number(value) || 0)));
  const safeMax = maxValue || 1;
  const stepX = chart.x.length > 1 ? innerWidth / (chart.x.length - 1) : innerWidth / 2;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de linea">`;
  for (let index = 0; index <= 4; index += 1) {
    const y = margin.top + (innerHeight * (4 - index)) / 4;
    const value = (safeMax * index) / 4;
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
    svg += `<text x="${margin.left - 6}" y="${y + 4}" text-anchor="end" font-size="10" fill="#5f6d70">${shortLabel(formatValue(value))}</text>`;
  }
  svg += `<line x1="${margin.left}" y1="${margin.top + innerHeight}" x2="${width - margin.right}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.24)" />`;

  chart.x.forEach((label, index) => {
    const x = margin.left + index * stepX;
    svg += `<text x="${x}" y="${height - 22}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(shortLabel(label))}</text>`;
  });

  chart.series.forEach((series, seriesIndex) => {
    const points = series.data.map((raw, index) => {
      const value = Number(raw) || 0;
      return {
        x: margin.left + index * stepX,
        y: margin.top + innerHeight - (value / safeMax) * innerHeight,
      };
    });
    svg += `<polyline fill="none" stroke="${palette[seriesIndex % palette.length]}" stroke-width="2.5" points="${points.map((point) => `${point.x},${point.y}`).join(" ")}" />`;
    points.forEach((point) => {
      svg += `<circle cx="${point.x}" cy="${point.y}" r="4" fill="${palette[seriesIndex % palette.length]}" />`;
    });
    const legendX = margin.left + seriesIndex * 140;
    svg += `<rect x="${legendX}" y="${height - 14}" width="10" height="10" rx="3" fill="${palette[seriesIndex % palette.length]}" />`;
    svg += `<text x="${legendX + 15}" y="${height - 5}" font-size="11" fill="#1f2a2c">${escapeHtml(series.name)}</text>`;
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
  const maxValue = Math.max(...chart.series.flatMap((series) => series.data.map((value) => Number(value) || 0)));
  const safeMax = maxValue || 1;
  const stepX = chart.x.length > 1 ? innerWidth / (chart.x.length - 1) : innerWidth / 2;
  const baseY = margin.top + innerHeight;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de area">`;
  for (let index = 0; index <= 4; index += 1) {
    const y = margin.top + (innerHeight * (4 - index)) / 4;
    svg += `<line x1="${margin.left}" y1="${y}" x2="${width - margin.right}" y2="${y}" stroke="rgba(31,42,44,0.08)" />`;
  }
  svg += `<line x1="${margin.left}" y1="${baseY}" x2="${width - margin.right}" y2="${baseY}" stroke="rgba(31,42,44,0.24)" />`;

  chart.x.forEach((label, index) => {
    svg += `<text x="${margin.left + index * stepX}" y="${height - 22}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(shortLabel(label))}</text>`;
  });

  chart.series.forEach((series, seriesIndex) => {
    const color = palette[seriesIndex % palette.length];
    const points = series.data.map((raw, index) => {
      const value = Number(raw) || 0;
      return {
        x: margin.left + index * stepX,
        y: margin.top + innerHeight - (value / safeMax) * innerHeight,
      };
    });
    const path = `M ${points[0].x},${baseY} ${points.map((point) => `L ${point.x},${point.y}`).join(" ")} L ${points[points.length - 1].x},${baseY} Z`;
    svg += `<path d="${path}" fill="${color}" fill-opacity="0.18" />`;
    svg += `<polyline fill="none" stroke="${color}" stroke-width="2.5" points="${points.map((point) => `${point.x},${point.y}`).join(" ")}" />`;
    const legendX = margin.left + seriesIndex * 140;
    svg += `<rect x="${legendX}" y="${height - 14}" width="10" height="10" rx="3" fill="${color}" />`;
    svg += `<text x="${legendX + 15}" y="${height - 5}" font-size="11" fill="#1f2a2c">${escapeHtml(series.name)}</text>`;
  });

  svg += "</svg>";
  return svg;
}

function renderPieChart(chart) {
  const size = 300;
  const centerX = size / 2;
  const centerY = size / 2;
  const radius = 110;
  const total = chart.series[0]?.data.reduce((acc, value) => acc + (Number(value) || 0), 0) || 1;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${size} ${size}" role="img" aria-label="Grafica de torta">`;
  let startAngle = -Math.PI / 2;

  chart.x.forEach((label, index) => {
    const value = Number(chart.series[0]?.data[index]) || 0;
    const angle = (value / total) * 2 * Math.PI;
    const endAngle = startAngle + angle;
    const x1 = centerX + radius * Math.cos(startAngle);
    const y1 = centerY + radius * Math.sin(startAngle);
    const x2 = centerX + radius * Math.cos(endAngle);
    const y2 = centerY + radius * Math.sin(endAngle);
    const largeArc = angle > Math.PI ? 1 : 0;
    const color = palette[index % palette.length];
    svg += `<path d="M ${centerX},${centerY} L ${x1},${y1} A ${radius},${radius} 0 ${largeArc},1 ${x2},${y2} Z" fill="${color}" stroke="#fff" stroke-width="2">`;
    svg += `<title>${escapeHtml(String(label))}: ${formatValue(value)} (${((value / total) * 100).toFixed(1)}%)</title></path>`;
    const midAngle = startAngle + angle / 2;
    const labelX = centerX + (radius * 0.65) * Math.cos(midAngle);
    const labelY = centerY + (radius * 0.65) * Math.sin(midAngle);
    if (angle > 0.25) {
      svg += `<text x="${labelX}" y="${labelY}" text-anchor="middle" font-size="11" fill="#fff" font-weight="700">${((value / total) * 100).toFixed(0)}%</text>`;
    }
    startAngle = endAngle;
  });

  chart.x.forEach((label, index) => {
    const legendY = size - 14 - (chart.x.length - 1 - index) * 16;
    svg += `<rect x="4" y="${legendY - 8}" width="10" height="10" rx="3" fill="${palette[index % palette.length]}" />`;
    svg += `<text x="18" y="${legendY}" font-size="10" fill="#1f2a2c">${escapeHtml(shortLabel(String(label)))}</text>`;
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
  const xValues = chart.x.map((value) => Number(value) || 0);
  const yValues = chart.series[0]?.data.map((value) => Number(value) || 0) || [];
  const xMax = Math.max(...xValues) || 1;
  const yMax = Math.max(...yValues) || 1;

  let svg = `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Grafica de dispersion">`;
  svg += `<line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.2)" />`;
  svg += `<line x1="${margin.left}" y1="${margin.top + innerHeight}" x2="${width - margin.right}" y2="${margin.top + innerHeight}" stroke="rgba(31,42,44,0.2)" />`;

  xValues.forEach((value, index) => {
    const yValue = yValues[index] || 0;
    const cx = margin.left + (value / xMax) * innerWidth;
    const cy = margin.top + innerHeight - (yValue / yMax) * innerHeight;
    svg += `<circle cx="${cx}" cy="${cy}" r="5" fill="${palette[0]}" fill-opacity="0.72" />`;
  });

  svg += `<text x="${width / 2}" y="${height - 5}" text-anchor="middle" font-size="11" fill="#5f6d70">${escapeHtml(chart.series[0]?.name || "X")}</text>`;
  svg += "</svg>";
  return svg;
}

function renderPivotTable(pivot) {
  if (!pivot) return `<div class="empty-state compact"><p>Sin datos para tabla pivote.</p></div>`;

  const formatCell = (value) => (value === null || value === undefined) ? "-" : escapeHtml(formatValue(value));

  let html = `<div class="table-wrapper"><table class="pivot-table">`;
  html += `<thead><tr><th>${escapeHtml(pivot.row_dimension)}</th>`;
  for (const column of pivot.cols) {
    html += `<th>${escapeHtml(String(column))}</th>`;
  }
  html += `<th class="pivot-total">Total</th></tr></thead>`;
  html += `<tbody>`;
  pivot.rows.forEach((row, rowIndex) => {
    html += `<tr><td><strong>${escapeHtml(String(row))}</strong></td>`;
    pivot.data[rowIndex].forEach((cell) => {
      html += `<td>${formatCell(cell)}</td>`;
    });
    html += `<td class="pivot-total">${formatCell(pivot.row_totals[rowIndex])}</td></tr>`;
  });
  html += `<tr class="pivot-total-row"><td><strong>Total</strong></td>`;
  for (const columnTotal of pivot.col_totals) {
    html += `<td>${formatCell(columnTotal)}</td>`;
  }
  html += `<td class="pivot-total">${formatCell(pivot.grand_total)}</td></tr>`;
  html += `</tbody></table></div>`;
  return html;
}

function renderTable(table) {
  if (!table?.columns?.length) return `<div class="empty-state compact"><p>Sin filas para mostrar.</p></div>`;
  const head = table.columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const rows = table.rows.length
    ? table.rows.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(formatValue(cell))}</td>`).join("")}</tr>`).join("")
    : `<tr><td colspan="${table.columns.length}">Sin filas para mostrar.</td></tr>`;
  return `<div class="table-wrapper"><table><thead><tr>${head}</tr></thead><tbody>${rows}</tbody></table></div>`;
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

function formatNumber(value) {
  const number = Number(value);
  if (Number.isNaN(number)) return String(value);
  return new Intl.NumberFormat("es-MX").format(number);
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
  return String(dimensionName).replaceAll("_", " ").toLowerCase();
}

function shortLabel(value) {
  const text = String(value);
  return text.length > 12 ? `${text.slice(0, 12)}...` : text;
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
  return String(value).replaceAll('"', "&quot;").replaceAll("'", "&#39;");
}
