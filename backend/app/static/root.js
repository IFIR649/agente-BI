const VIEW_NAMES = {
  loading: "loading",
  waiting: "waiting",
  select: "select",
  upload: "upload",
  chat: "chat",
};

const state = {
  datasets: [],
  activeDataset: null,
  messages: [],
  view: VIEW_NAMES.loading,
  mode: "session",
  isUploading: false,
  isSubmitting: false,
  heartbeatTimer: null,
};

const palette = ["#166a63", "#d97a36", "#355c7d", "#8b5a2b", "#7a3b69", "#2d7a4f", "#a84a35"];
const STORAGE_KEYS = {
  chatHistoryPrefix: "chat-history-prod",
  apiKey: "csv-agent-api-key",
  userId: "csv-agent-user-id",
  sessionToken: "csv-agent-session-token",
  sessionUserId: "csv-agent-session-user-id",
};

const elements = {
  authCard: document.getElementById("auth-card"),
  apiKeyInput: document.getElementById("api-key-input"),
  actorUserIdInput: document.getElementById("actor-user-id-input"),
  authSave: document.getElementById("auth-save"),
  authStatusText: document.getElementById("auth-status-text"),
  activeDatasetChip: document.getElementById("active-dataset-chip"),
  logoutSession: document.getElementById("logout-session"),
  datasetStateText: document.getElementById("dataset-state-text"),
  loadingStage: document.getElementById("loading-stage"),
  sessionStage: document.getElementById("session-stage"),
  sessionStageText: document.getElementById("session-stage-text"),
  selectorStage: document.getElementById("selector-stage"),
  selectorList: document.getElementById("selector-list"),
  uploadStage: document.getElementById("upload-stage"),
  uploadForm: document.getElementById("upload-form"),
  uploadFile: document.getElementById("upload-file"),
  uploadDisplayName: document.getElementById("upload-display-name"),
  uploadSubmit: document.getElementById("upload-submit"),
  uploadStatusText: document.getElementById("upload-status-text"),
  uploadStatusBadge: document.getElementById("upload-status-badge"),
  chatStage: document.getElementById("chat-stage"),
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
  captureTokenModeFromUrl();
  bindEvents();
  bootstrap();
});

async function bootstrap() {
  stopHeartbeat();
  state.mode = hasStoredTokenSession() ? "token" : "session";
  syncAuthInputs();
  clearChatState();
  setView(VIEW_NAMES.loading);
  renderChat();
  renderSuggestions(null);
  updateComposerState();
  updateAccessVisibility();

  if (isTokenMode()) {
    setAuthStatus("Sesion temporal vinculada desde la API.", "success");
    try {
      await hydrateTokenExperience();
      syncViewWithState();
      updateViewState();
      return;
    } catch (error) {
      clearTokenSession();
      state.mode = "session";
      updateAccessVisibility();
      setAuthStatus((error?.message || "La sesion temporal ya no es valida.") + " Reabre el chat desde Visual FoxPro.", "error");
      clearChatState();
    }
  }

  state.mode = "session";
  setAuthStatus("Este chat se habilita cuando Visual FoxPro crea una sesion temporal y abre esta URL.", "warn");
  setDatasetState("Esperando una sesion temporal del sistema.", "warn");
  syncViewWithState();
  updateViewState();
}

function bindEvents() {
  elements.authSave?.addEventListener("click", () => {
    saveAuthInputs();
    bootstrap();
  });
  elements.logoutSession?.addEventListener("click", handleTokenLogout);
  elements.uploadForm?.addEventListener("submit", handleUploadSubmit);
  elements.queryForm.addEventListener("submit", handleQuerySubmit);
  elements.suggestions.addEventListener("click", handleSuggestionClick);
  elements.selectorList.addEventListener("click", handleSelectorClick);
  elements.chatTranscript.addEventListener("click", handleTranscriptClick);

  elements.clearChat?.addEventListener("click", () => {
    if (!state.activeDataset) {
      return;
    }

    clearCurrentChatHistory();
    state.messages = [];
    renderChat();
    pushMessage({
      role: "agent",
      text: `Chat limpiado. Puedes seguir consultando ${state.activeDataset.display_name}.`,
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

async function hydrateManualExperience() {
  setDatasetState("Cargando dataset activo...", "warn");

  const activeDataset = await loadActiveDataset();
  if (activeDataset) {
    enterChatView(activeDataset);
    return;
  }

  clearChatState();
  renderChat();
  renderSuggestions(null);
  await loadDatasets();
}

async function hydrateTokenExperience() {
  setDatasetState("Recuperando sesion temporal...", "warn");
  const sessionState = await loadTokenSessionState();
  enterChatView(sessionState.dataset, { focusComposer: false });
  startHeartbeat();
}

async function loadActiveDataset() {
  ensureApiAccess();
  const response = await fetch("/datasets/active", {
    headers: buildApiHeaders(),
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

async function loadTokenSessionState() {
  const response = await fetch("/chat/session", {
    headers: buildSessionHeaders(),
  });
  const payload = await parseJsonResponse(response);

  if (response.status === 401 || response.status === 403) {
    throw new Error(payload.detail || "La sesion temporal expiro o no es valida.");
  }
  if (response.status === 412) {
    throw new Error(payload.detail || "La sesion temporal no tiene un dataset cargado.");
  }
  if (!response.ok) {
    throw new Error(payload.detail || "No se pudo recuperar la sesion temporal.");
  }

  return payload;
}

async function loadDatasets() {
  ensureApiAccess();
  const response = await fetch("/datasets", {
    headers: buildApiHeaders(),
  });
  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw new Error(payload.detail || "No se pudieron cargar los datasets.");
  }

  state.datasets = Array.isArray(payload) ? payload : [];
}

async function activateDataset(datasetId) {
  setDatasetState("Activando CSV...", "warn");
  ensureApiAccess();

  const response = await fetch("/datasets/active", {
    method: "PUT",
    headers: buildApiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({ dataset_id: datasetId }),
  });
  const payload = await parseJsonResponse(response);
  if (!response.ok) {
    throw new Error(payload.detail || "No se pudo activar el dataset.");
  }

  enterChatView(payload, { focusComposer: true });
}

async function handleUploadSubmit(event) {
  event.preventDefault();
  if (state.isUploading) {
    return;
  }

  const file = elements.uploadFile.files[0];
  if (!file) {
    setUploadStatus("Selecciona un archivo CSV.", "error", "Falta archivo");
    return;
  }

  state.isUploading = true;
  toggleUploadUI();
  setUploadStatus("Subiendo y activando CSV...", "warn", "Subiendo");

  try {
    ensureApiAccess();
    const formData = new FormData();
    formData.append("file", file);
    const displayName = elements.uploadDisplayName.value.trim();
    if (displayName) {
      formData.append("metadata", JSON.stringify({ display_name: displayName }));
    }

    const response = await fetch("/datasets/upload", {
      method: "POST",
      headers: buildApiHeaders(),
      body: formData,
    });
    const payload = await parseJsonResponse(response);
    if (!response.ok) {
      throw new Error(payload.detail || "No se pudo subir el dataset.");
    }

    state.datasets = mergeDatasetIntoList(payload);
    elements.uploadForm.reset();
    setUploadStatus(
      `Dataset listo: ${payload.display_name} (${formatNumber(payload.row_count)} filas)`,
      "success",
      "Listo",
    );
    enterChatView(payload, { focusComposer: true });
  } catch (error) {
    setUploadStatus(error.message, "error", "Error");
  } finally {
    state.isUploading = false;
    toggleUploadUI();
  }
}

async function handleQuerySubmit(event) {
  event.preventDefault();
  if (state.isSubmitting) {
    return;
  }

  const question = elements.queryInput.value.trim();
  const history = buildHistoryPayload();
  if (!state.activeDataset || !question) {
    return;
  }

  state.isSubmitting = true;
  let thinkingId = null;

  try {
    updateComposerState("Interpretando tu pregunta...");
    pushMessage({ role: "user", text: question });
    thinkingId = pushMessage({ role: "thinking", text: renderThinkingMarkup(), html: true });
    elements.queryInput.value = "";

    const requestInit = buildChatMessageRequest(question, history);
    const response = await fetch(requestInit.url, {
      method: "POST",
      headers: requestInit.headers,
      body: requestInit.body,
    });

    const payload = await parseJsonResponse(response);
    removeMessage(thinkingId);

    if ((response.status === 401 || response.status === 403) && isTokenMode()) {
      pushMessage({
        role: "system",
        text: payload.detail || "La sesion temporal expiro o ya no es valida. Reabre el chat desde VFP.",
      });
      clearTokenSession();
      bootstrap();
      return;
    }

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
  if (!button) {
    return;
  }
  elements.queryInput.value = button.dataset.question || "";
  elements.queryInput.focus();
}

function handleSelectorClick(event) {
  const button = event.target.closest("[data-dataset-id]");
  if (!button) {
    return;
  }

  activateDataset(button.dataset.datasetId).catch((error) => {
    setDatasetState(error.message, "error");
  });
}

function handleTranscriptClick(event) {
  const button = event.target.closest(".hint-action");
  if (!button) {
    return;
  }
  elements.queryInput.value = button.dataset.hint || "";
  elements.queryInput.focus();
}

function enterChatView(dataset, options = {}) {
  state.activeDataset = dataset;
  state.datasets = mergeDatasetIntoList(dataset);
  setView(VIEW_NAMES.chat);
  restoreChatHistory(dataset.id);
  ensureChatGreeting();
  if (isTokenMode()) {
    startHeartbeat();
  }
  updateViewState();

  if (options.focusComposer) {
    elements.queryInput.focus();
  }
}

function syncViewWithState() {
  if (isTokenMode()) {
    setView(state.activeDataset ? VIEW_NAMES.chat : VIEW_NAMES.loading);
    return;
  }
  setView(VIEW_NAMES.waiting);
}

function setView(view) {
  state.view = view;
  setStageVisible(elements.loadingStage, view === VIEW_NAMES.loading);
  setStageVisible(elements.sessionStage, view === VIEW_NAMES.waiting);
  setStageVisible(elements.selectorStage, view === VIEW_NAMES.select);
  setStageVisible(elements.uploadStage, view === VIEW_NAMES.upload);
  setStageVisible(elements.chatStage, view === VIEW_NAMES.chat);
}

function setStageVisible(element, visible) {
  if (!element) {
    return;
  }
  element.style.display = visible ? "" : "none";
  element.hidden = !visible;
  element.setAttribute("aria-hidden", visible ? "false" : "true");
}

function updateViewState() {
  const hasActiveDataset = Boolean(state.activeDataset);
  const tokenMode = isTokenMode();

  elements.activeDatasetChip.textContent = hasActiveDataset
    ? state.activeDataset.display_name
    : "Sin dataset activo";
  elements.clearChat.disabled = !hasActiveDataset;
  if (elements.logoutSession) {
    elements.logoutSession.hidden = !tokenMode;
    elements.logoutSession.style.display = tokenMode ? "" : "none";
  }

  if (state.view === VIEW_NAMES.chat && hasActiveDataset) {
    setDatasetState(
      tokenMode
        ? `Sesion temporal activa · CSV: ${state.activeDataset.display_name}`
        : `CSV activo: ${state.activeDataset.display_name}`,
      "success",
    );
    renderSuggestions(state.activeDataset);
  } else if (state.view === VIEW_NAMES.waiting) {
    setDatasetState("Esperando una sesion temporal vinculada al CSV.", "warn");
    elements.selectorList.innerHTML = "";
    renderSuggestions(null);
  } else if (state.view === VIEW_NAMES.select) {
    setDatasetState("Selecciona el CSV que quieres activar para este navegador.", "warn");
    renderSelectorList();
    renderSuggestions(null);
  } else if (state.view === VIEW_NAMES.upload) {
    setDatasetState("No hay datasets activos. Sube un CSV para comenzar.", "warn");
    elements.selectorList.innerHTML = "";
    renderSuggestions(null);
  } else {
    elements.selectorList.innerHTML = "";
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

function restoreChatHistory(datasetId) {
  state.messages = [];
  try {
    const raw = sessionStorage.getItem(historyStorageKey(datasetId));
    if (!raw) {
      return;
    }
    const messages = JSON.parse(raw);
    if (Array.isArray(messages)) {
      state.messages = messages;
    }
  } catch (_) {}
}

function saveSessionHistory() {
  if (!state.activeDataset) {
    return;
  }

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
    sessionStorage.setItem(historyStorageKey(state.activeDataset.id), JSON.stringify(serializable));
  } catch (_) {}
}

function clearCurrentChatHistory() {
  if (!state.activeDataset) {
    return;
  }
  try {
    sessionStorage.removeItem(historyStorageKey(state.activeDataset.id));
  } catch (_) {}
}

function historyStorageKey(datasetId) {
  return `${STORAGE_KEYS.chatHistoryPrefix}:${currentUserId()}:${datasetId}`;
}

function clearChatState() {
  state.activeDataset = null;
  state.messages = [];
}

function renderChat() {
  elements.chatTranscript.innerHTML = "";
  if (state.view !== VIEW_NAMES.chat || !state.activeDataset) {
    return;
  }

  for (const message of state.messages) {
    const fragment = elements.messageTemplate.content.cloneNode(true);
    const article = fragment.querySelector(".message");
    const label = fragment.querySelector(".message-label");
    const body = fragment.querySelector(".message-body");

    article.classList.add(message.role);
    article.dataset.messageId = message.id;
    label.textContent = labelForRole(message.role, message.subtitle);
    body.classList.toggle("plain-text", false);
    body.classList.toggle("rich-text", false);

    if (message.html) {
      body.innerHTML = message.text;
      body.classList.add("rich-text");
    } else if (shouldRenderRichTextMessage(message)) {
      body.innerHTML = renderRichTextMessage(message.text);
      body.classList.add("rich-text");
    } else {
      body.textContent = message.text;
      body.classList.add("plain-text");
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

function ensureChatGreeting() {
  if (!state.activeDataset || state.messages.length) {
    return;
  }
  pushMessage({
    role: "agent",
    text: `CSV activo: ${state.activeDataset.display_name}. Puedes empezar a consultar este dataset.`,
  });
}

function updateComposerState(overrideText) {
  const hasDataset = Boolean(state.activeDataset);
  const chatReady = hasDataset && state.view === VIEW_NAMES.chat;
  const disabled = !chatReady || state.isSubmitting;
  elements.querySubmit.disabled = disabled;
  elements.queryInput.disabled = disabled;

  if (overrideText) {
    elements.queryStatusText.textContent = overrideText;
    elements.queryStatusText.className = "status-text warn";
    return;
  }

  if (!chatReady) {
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

function shouldRenderRichTextMessage(message) {
  return !message.html && (message.role === "agent" || message.role === "system");
}

function renderRichTextMessage(text) {
  const normalized = String(text ?? "")
    .replace(/\r\n?/g, "\n")
    .trim();

  if (!normalized) {
    return "";
  }

  const chunks = [];
  const lines = normalized.split("\n");
  const paragraph = [];

  const flushParagraph = () => {
    if (!paragraph.length) {
      return;
    }
    chunks.push(`<p>${paragraph.map((line) => renderInlineMarkdown(line)).join("<br>")}</p>`);
    paragraph.length = 0;
  };

  for (let index = 0; index < lines.length; ) {
    const rawLine = lines[index];
    const trimmed = rawLine.trim();

    if (!trimmed) {
      flushParagraph();
      index += 1;
      continue;
    }

    const headingMatch = rawLine.match(/^\s*(#{1,3})\s+(.+)$/);
    if (headingMatch) {
      flushParagraph();
      const level = Math.min(6, headingMatch[1].length + 3);
      chunks.push(`<h${level}>${renderInlineMarkdown(headingMatch[2].trim())}</h${level}>`);
      index += 1;
      continue;
    }

    if (isBlockquoteLine(rawLine)) {
      flushParagraph();
      const quoteLines = [];
      while (index < lines.length && isBlockquoteLine(lines[index])) {
        quoteLines.push(lines[index].replace(/^\s*>\s?/, ""));
        index += 1;
      }
      chunks.push(`<blockquote>${quoteLines.map((line) => renderInlineMarkdown(line)).join("<br>")}</blockquote>`);
      continue;
    }

    if (isUnorderedListLine(rawLine)) {
      flushParagraph();
      const items = [];
      while (index < lines.length && isUnorderedListLine(lines[index])) {
        items.push(lines[index].replace(/^\s*(?:[*+-])\s+/, ""));
        index += 1;
      }
      chunks.push(`<ul>${items.map((line) => `<li>${renderInlineMarkdown(line)}</li>`).join("")}</ul>`);
      continue;
    }

    if (isOrderedListLine(rawLine)) {
      flushParagraph();
      const items = [];
      while (index < lines.length && isOrderedListLine(lines[index])) {
        items.push(lines[index].replace(/^\s*\d+[.)]\s+/, ""));
        index += 1;
      }
      chunks.push(`<ol>${items.map((line) => `<li>${renderInlineMarkdown(line)}</li>`).join("")}</ol>`);
      continue;
    }

    paragraph.push(trimmed);
    index += 1;
  }

  flushParagraph();
  return chunks.join("");
}

function isBlockquoteLine(line) {
  return /^\s*>\s?/.test(line);
}

function isUnorderedListLine(line) {
  return /^\s*(?:[*+-])\s+/.test(line);
}

function isOrderedListLine(line) {
  return /^\s*\d+[.)]\s+/.test(line);
}

function renderInlineMarkdown(text) {
  let safe = escapeHtml(text ?? "");
  const codeTokens = [];

  safe = safe.replace(/`([^`]+)`/g, (_, code) => {
    const token = `%%CODE_TOKEN_${codeTokens.length}%%`;
    codeTokens.push(`<code>${code}</code>`);
    return token;
  });

  safe = safe.replace(/\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g, (_, label, url) => (
    `<a href="${escapeAttr(url)}" target="_blank" rel="noreferrer noopener">${label}</a>`
  ));
  safe = safe.replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>");
  safe = safe.replace(/(^|[^*])\*([^*\n]+)\*/g, (_, prefix, value) => `${prefix}<em>${value}</em>`);
  safe = safe.replace(/(^|[^_])_([^_\n]+)_/g, (_, prefix, value) => `${prefix}<em>${value}</em>`);

  for (let index = 0; index < codeTokens.length; index += 1) {
    const token = `%%CODE_TOKEN_${index}%%`;
    safe = safe.split(token).join(codeTokens[index]);
  }

  return safe;
}

function mergeDatasetIntoList(dataset) {
  const byId = new Map(state.datasets.map((item) => [item.id, item]));
  byId.set(dataset.id, dataset);
  return Array.from(byId.values());
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
  if (!elements.apiKeyInput || !elements.actorUserIdInput) {
    return;
  }
  const apiKey = (elements.apiKeyInput.value || "").trim();
  const userId = (elements.actorUserIdInput.value || "").trim() || `web-${generateId()}`;
  window.localStorage.setItem(STORAGE_KEYS.apiKey, apiKey);
  window.localStorage.setItem(STORAGE_KEYS.userId, userId);
  elements.actorUserIdInput.value = userId;
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

function getSessionToken() {
  return (window.sessionStorage.getItem(STORAGE_KEYS.sessionToken) || "").trim();
}

function getSessionUserId() {
  return (window.sessionStorage.getItem(STORAGE_KEYS.sessionUserId) || "").trim();
}

function hasStoredTokenSession() {
  return Boolean(getSessionToken() && getSessionUserId());
}

function currentUserId() {
  return isTokenMode() ? getSessionUserId() : ensureUserId();
}

function isTokenMode() {
  return Boolean(state.mode === "token" && getSessionToken() && getSessionUserId());
}

function hasApiAccess() {
  return Boolean(ensureApiKey() && ensureUserId());
}

function ensureApiAccess() {
  if (!hasApiAccess()) {
    throw new Error("Captura la API key y el user id para usar esta pantalla.");
  }
}

function updateAccessVisibility() {
  if (!elements.authCard) {
    return;
  }
  elements.authCard.hidden = true;
  elements.authCard.style.display = "none";
}

function buildApiHeaders(extraHeaders = {}) {
  ensureApiAccess();
  return {
    "X-API-Key": ensureApiKey(),
    "X-User-Id": ensureUserId(),
    ...extraHeaders,
  };
}

function buildSessionHeaders(extraHeaders = {}) {
  const sessionToken = getSessionToken();
  const sessionUserId = getSessionUserId();
  if (!sessionToken || !sessionUserId) {
    throw new Error("La sesion temporal ya no esta disponible.");
  }
  return {
    "X-Session-Token": sessionToken,
    "X-User-Id": sessionUserId,
    ...extraHeaders,
  };
}

function captureTokenModeFromUrl() {
  const url = new URL(window.location.href);
  const sessionToken = (url.searchParams.get("session_token") || "").trim();
  const userId = (url.searchParams.get("user_id") || "").trim();
  if (!sessionToken) {
    return;
  }

  window.sessionStorage.setItem(STORAGE_KEYS.sessionToken, sessionToken);
  if (userId) {
    window.sessionStorage.setItem(STORAGE_KEYS.sessionUserId, userId);
  } else {
    window.sessionStorage.removeItem(STORAGE_KEYS.sessionUserId);
  }

  url.searchParams.delete("session_token");
  url.searchParams.delete("user_id");
  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState({}, "", nextUrl);
}

function clearTokenSession() {
  stopHeartbeat();
  window.sessionStorage.removeItem(STORAGE_KEYS.sessionToken);
  window.sessionStorage.removeItem(STORAGE_KEYS.sessionUserId);
}

function buildChatMessageRequest(question, history) {
  if (isTokenMode()) {
    return {
      url: "/chat/message",
      headers: buildSessionHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ question, history }),
    };
  }

  ensureApiAccess();
  return {
    url: "/query",
    headers: buildApiHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      dataset_id: state.activeDataset.id,
      question,
      history,
    }),
  };
}

function startHeartbeat() {
  if (!isTokenMode()) {
    return;
  }
  stopHeartbeat();
  state.heartbeatTimer = window.setInterval(async () => {
    try {
      const response = await fetch("/chat/heartbeat", {
        method: "POST",
        headers: buildSessionHeaders(),
      });
      if (response.status === 401 || response.status === 403) {
        clearTokenSession();
        setAuthStatus("La sesion temporal expiro. Reabre el chat desde Visual FoxPro.", "error");
        bootstrap();
        return;
      }
    } catch (_) {}
  }, 30000);
}

function stopHeartbeat() {
  if (state.heartbeatTimer) {
    window.clearInterval(state.heartbeatTimer);
    state.heartbeatTimer = null;
  }
}

async function handleTokenLogout() {
  if (!isTokenMode()) {
    return;
  }

  try {
    await fetch("/chat/logout", {
      method: "POST",
      headers: buildSessionHeaders(),
    });
  } catch (_) {}

  clearTokenSession();
  state.mode = "session";
  setAuthStatus("Sesion temporal cerrada. Reabre el chat desde Visual FoxPro.", "warn");
  bootstrap();
}

function setAuthStatus(text, tone = "") {
  if (elements.authStatusText) {
    elements.authStatusText.textContent = text;
    elements.authStatusText.className = tone ? `status-text ${tone}` : "status-text";
  }
  if (elements.sessionStageText) {
    elements.sessionStageText.textContent = text;
    elements.sessionStageText.className = tone ? `status-text ${tone}` : "status-text";
  }
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
}function renderBarChart(chart) {
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

