# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Backend (development)
cd backend && uvicorn app.main:app --reload

# Backend tests
cd backend && pytest tests/

# Frontend (development)
cd frontend && npm run dev

# Frontend build
cd frontend && npm run build

# Docker (full stack)
docker-compose up --build
```

## Architecture

This is a **controlled data analysis agent** — Gemini interprets intent, Python executes, Gemini summarizes. The model never generates SQL, Python, or arbitrary code.

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
