# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```powershell
# Backend (development — uses --reload, do NOT use on Windows Server)
.\scripts\backend-dev.ps1

# Backend (production — no --reload, no access log)
.\scripts\backend-prod.ps1 --host 0.0.0.0 --port 8000

# Backend tests (all)
.\scripts\backend-test.ps1

# Run a single test file or test
.\scripts\backend-test.ps1 backend\tests\test_chat_api.py
.\scripts\backend-test.ps1 backend\tests\test_api.py -k "test_query"
```

Backend commands must run through the repo-local interpreter at `.\env\Scripts\python.exe`.

> **Windows Server tip:** If the API appears frozen in an interactive console, check for QuickEdit/Selection mode — it pauses the process until selection is cleared.

## Environment

Requires a `.env` file (or environment variables prefixed with `AGENT_`) at the repo root or in the working directory. Minimum required vars:

```
AGENT_API_KEY=<secret>           # mandatory — app won't start without it
AGENT_GEMINI_API_KEY=<key>       # required for production; tests use a fake
```

All config keys map to `Settings` in `backend/app/config.py` with the `AGENT_` prefix. Notable defaults: `session_timeout_seconds=300`, `max_concurrent_sessions=50`, `rate_limit_requests=20/60s`.

## Architecture

This is a **controlled data analysis agent** — Gemini interprets intent, Python executes, Gemini summarizes. The model never generates SQL, Python, or arbitrary code.

### Two API flows

**Session flow (primary):** `POST /chat/bootstrap` (upload CSV → creates session + returns `session_token`) → `POST /chat/message` (ask question, pass `X-Session-Token` + `X-User-Id`) → `POST /chat/heartbeat` (keep-alive, default 300 s timeout) → `POST /chat/logout`. Each session owns one DuckDB in-memory connection with the CSV loaded as table `"dataset"`. Session files are cleaned up on destroy/expire; orphans from crashes are purged on startup.

**Legacy dataset flow:** `POST /datasets/upload` (global upload) → `POST /query` (query by `dataset_id`). Uses `ActiveDatasetStore` instead of `SessionStore`.

### 3-layer agent pattern (never collapse into one call)

1. **Interpret** (`services/intent_parser.py`) — Gemini Flash converts natural language to a structured JSON plan. Temperature 0.1, forced response schema, returns `confidence` field.
2. **Execute** (`services/query_executor.py`) — Pydantic validates the plan against `data_dictionary`, then DuckDB runs parametrized queries. No model involvement.
3. **Summarize** (`services/summary_writer.py`) — Gemini Flash writes a 1-2 sentence summary of the already-computed results. Temperature 0.2.

### data_dictionary is the contract

Generated at CSV upload by `services/dataset_profiler.py`. Defines:
- `metrics_allowed` — formulas like `SUM(ventas)`, with names and descriptions
- `dimensions_allowed` — valid grouping columns
- `aliases` — synonym map (e.g. "tienda" → "sucursal")

This dictionary is injected into every Gemini call as system context. Pydantic rejects any metric or dimension not in the catalog before execution.

### Anti-hallucination controls

- Model never sees raw CSV data — only the schema and computed summary
- If `confidence < 0.7` on the parsed intent → return clarification request to the user, never guess
- All query functions are fixed and parametrized — no free-form code generation by the model
- Validation failure returns a clear error with available options; never executes a partial plan

### Gemini Flash → Pro escalation

Automatically escalate to Gemini 2.5 Pro when:
- `confidence < 0.5`
- Intent is explanatory ("why did sales drop?")
- Multi-step or correlation analysis required
- Flash fails validation 2+ consecutive times for the same question

### Authentication

All protected routes require two headers: `X-API-Key` (checked via HMAC against `AGENT_API_KEY` env var) and `X-User-Id`. The chat flow additionally requires `X-Session-Token`. `AGENT_API_KEY` is mandatory — the app will not start without it.

### Static UI

The app serves three HTML pages from `backend/app/static/`:
- `/` → `index.html` (main chat UI)
- `/test` → `test.html` (API test harness)
- `/analytics` → `analytics.html` (usage analytics)

### FX resolver

`core/fx.py` — `BanxicoFxResolver` fetches the Banxico FIX USD→MXN rate on demand, caching results in a `fx_rates` table inside `logs/audit.db`. Used by `AuditLogger` to record MXN-equivalent values. Tests replace it with `FixedFXResolver(rate=17.0)` from `conftest.py`.

### Multi-turn context

`POST /chat/message` accepts an optional `history: list[ConversationTurn]` field. Each turn has `role` (`user`/`assistant`) and `content`. Pass prior turns to give Gemini conversation context during intent parsing.

### Key decisions

- **DuckDB only** (no Polars) — reads CSV natively, handles analytic SQL, one engine is enough
- **No model-generated SQL or code** — all query logic lives in fixed Python functions
- **In-memory cache first** — `cache.py` uses a TTL dict; add Redis only if load requires it
- **SQLite for audit logs** — `logs/audit.db` is sufficient for 20–100 users; no Postgres needed
- **Central server deployment** — consistent business rules, metrics catalog, and logs across all users

### CSV format support

The profiler handles non-standard CSV formats automatically:
- **Semicolon separator** — DuckDB's `read_csv_auto` auto-detects it (no forced `delimiter`)
- **European numeric format** — columns with values like `6335,3448` are detected as float via regex sampling; stored with `detected_number_format: "european"` in `ColumnProfile`; metrics use `REPLACE(REPLACE(col, '.', ''), ',', '.')::DOUBLE` as formula
- **Dates without leading zero hour** — formats like `01/02/2026 0:00` are normalized before parsing by zero-padding single-digit hours

### Error handling and fallback chain

- `allow_local_gemini_fallback = True` by default — heuristic parser activates when both Flash and Pro fail
- When all parsing paths fail, a `ClarificationNeeded` is raised (never a bare HTTP 502) with available metrics as hints
- Unsupported metrics in a Gemini plan trigger `ClarificationNeeded` (not `PlanValidationError`) so the user gets a helpful message

### Logging

All service modules use `logging.getLogger(__name__)`. Key log points: `_try_model` (model used, success/fail), `parse` (question, final plan, confidence), `generate_structured` (parse failure details).

## Testing

Tests use `QueuedGemini` (in `conftest.py`) to mock Gemini — call `fake.queue_structured(...)` or `fake.queue_text(...)` before hitting any endpoint that triggers a Gemini call. The `app` and `client` fixtures wire up a temporary SQLite/DuckDB environment automatically; `AGENT_API_KEY` is set to `"test-api-key"`. Test headers default to `X-API-Key: test-api-key` + `X-User-Id: test-user`.

`allow_local_gemini_fallback` is set to `False` in the test `Settings` so heuristic fallback never silently masks a missing queue item.

Convenience helpers:
- `install_fake_gemini(app)` — replaces the real `GeminiClient` methods with `QueuedGemini`; call once per `app` fixture.
- `queue_fake_gemini(app, structured=[...], texts=[...])` — queues items on the already-installed fake.

Pre-built dataset fixtures: `uploaded_shape_dataset`, `uploaded_semicolon_dataset`, `uploaded_multi_time_dataset`, `uploaded_weekday_dataset` — each uploads a named CSV and returns the parsed JSON response.
