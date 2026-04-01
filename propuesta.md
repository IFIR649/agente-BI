# Propuesta: Agente de Analisis de Datos

## Resumen

Agente controlado por herramientas para analisis de datos sobre CSV. Gemini interpreta intencion, Python ejecuta, Gemini redacta resumen. Sin generacion libre de codigo ni respuestas inventadas.

---

## Arquitectura

```
Usuario (js) --> FastAPI --> Gemini (interpretar intencion)
                                    |
                                    v
                              Plan estructurado (JSON)
                                    |
                                    v
                              Validador (Pydantic)
                                    |
                                    v
                              DuckDB (ejecutar consulta)
                                    |
                                    v
                              Builder de respuesta (KPIs + tabla + chart)
                                    |
                                    v
                              Gemini (redactar resumen corto)
                                    |
                                    v
                              JSON final --> Frontend
```

### Stack

| Capa | Tecnologia | Justificacion |
|------|-----------|---------------|
| API | FastAPI | Async, tipado, OpenAPI gratis |
| Motor de datos | DuckDB | SQL analitico directo sobre CSV, sin ETL |
| Validacion | Pydantic v2 | Schemas estrictos para entrada/salida |
| Modelo | Gemini 2.5 Flash | Bajo costo, baja latencia, structured output |
| Modelo (fallback) | Gemini 2.5 Pro | Preguntas ambiguas o multi-paso |
| Cache | Redis | Cache de intenciones y respuestas frecuentes |
| Frontend | React + TanStack Table + ECharts | Tablas interactivas + graficas rapidas |
| Logs | SQLite | Trazabilidad de cada consulta |

> **Nota sobre Polars:** No se incluye como motor principal. DuckDB cubre lectura de CSV y consultas analiticas sin necesidad de un segundo engine. Se puede agregar si surgen transformaciones complejas pre-carga.

---

## Estructura del proyecto

```
agente-web/
  backend/
    app/
      main.py                  # FastAPI app + CORS + lifespan
      config.py                # Settings (Pydantic BaseSettings)
      routers/
        query.py               # POST /query - endpoint principal
        datasets.py            # GET /datasets, POST /datasets/upload
        health.py              # GET /health
      services/
        intent_parser.py       # Gemini: NL -> plan estructurado
        query_executor.py      # DuckDB: plan -> resultados
        response_builder.py    # KPIs + tabla + chart data
        summary_writer.py      # Gemini: resultados -> resumen corto
        dataset_profiler.py    # Perfila CSV al cargarlo
      models/
        intent.py              # Schemas de intencion (Pydantic)
        response.py            # Schemas de respuesta
        dataset.py             # Schema del catalogo de datos
      core/
        database.py            # Conexion DuckDB
        gemini_client.py       # Cliente Gemini con retry/timeout
        cache.py               # Cache de intenciones (Redis o en memoria)
        rate_limiter.py        # Throttling por usuario
      data/
        uploads/               # CSVs subidos
        catalogs/              # data_dictionary por dataset
      logs/
        audit.db               # SQLite con trazabilidad
    tests/
      test_intent_parser.py
      test_query_executor.py
      test_response_builder.py
      test_api.py
    requirements.txt
    Dockerfile
  frontend/
    src/
      components/
        QueryInput.tsx         # Barra de consulta en lenguaje natural
        KPICards.tsx            # Tarjetas de metricas
        DataTable.tsx           # TanStack Table
        Chart.tsx              # ECharts wrapper
        ResponsePanel.tsx      # Panel que orquesta KPI+tabla+chart
        DatasetSelector.tsx    # Selector de dataset activo
      hooks/
        useQuery.ts            # Hook para POST /query
        useDatasets.ts         # Hook para listar/subir datasets
      types/
        api.ts                 # Tipos de respuesta del backend
      App.tsx
      main.tsx
    package.json
    Dockerfile
  docker-compose.yml
```

---

## Flujo detallado

### 1. Carga de dataset

1. Usuario sube CSV via `/datasets/upload`
2. `dataset_profiler.py` genera `data_dictionary`:

```json
{
  "id": "ventas_2026",
  "filename": "ventas_q1_2026.csv",
  "row_count": 5420,
  "columns": {
    "fecha": {"type": "date", "min": "2026-01-01", "max": "2026-03-31"},
    "sucursal": {"type": "string", "unique_values": ["Puebla", "Cholula", "Atlixco", "CDMX"]},
    "ventas": {"type": "float", "min": 12.50, "max": 9800.00},
    "cliente": {"type": "string", "cardinality": 342},
    "producto": {"type": "string", "cardinality": 28}
  },
  "dimensions_allowed": ["sucursal", "cliente", "producto", "mes", "semana"],
  "metrics_allowed": [
    {"name": "ventas_totales", "formula": "SUM(ventas)", "description": "Suma total de ventas"},
    {"name": "conteo_operaciones", "formula": "COUNT(*)", "description": "Numero de transacciones"},
    {"name": "ticket_promedio", "formula": "AVG(ventas)", "description": "Promedio por transaccion"},
    {"name": "venta_maxima", "formula": "MAX(ventas)", "description": "Transaccion mas alta"}
  ],
  "aliases": {
    "tienda": "sucursal",
    "branch": "sucursal",
    "monto": "ventas",
    "amount": "ventas"
  }
}
```

3. DuckDB registra el CSV como tabla virtual (sin copiar datos)

### 2. Consulta del usuario

**Paso 1: Interpretar (Gemini Flash)**

Input al modelo:
- System prompt estricto con data_dictionary
- Pregunta del usuario
- Temperatura: 0.1
- Response schema forzado (structured output)

Output esperado:

```json
{
  "intent": "aggregate_report",
  "dimensions": ["sucursal"],
  "metrics": ["ventas_totales", "ticket_promedio"],
  "filters": [
    {"field": "fecha", "op": "between", "value": ["2026-01-01", "2026-03-31"]}
  ],
  "sort": {"field": "ventas_totales", "order": "desc"},
  "comparison": "previous_period",
  "visualization": "bar",
  "top_n": 10,
  "confidence": 0.92
}
```

Si `confidence < 0.7`: pedir aclaracion al usuario en vez de adivinar.

**Paso 2: Validar (Pydantic)**

- Columnas existen en el dataset
- Metricas estan en `metrics_allowed`
- Dimensiones estan en `dimensions_allowed`
- Filtros de fecha dentro del rango real
- Si falla: respuesta de error clara, sin ejecutar nada

**Paso 3: Ejecutar (DuckDB)**

- Construir SQL parametrizado desde el plan validado
- Ejecutar contra la tabla virtual
- Si hay `comparison: "previous_period"`, ejecutar segunda consulta con offset de fechas
- Timeout de 5 segundos por consulta

**Paso 4: Construir respuesta (Python)**

```json
{
  "kpis": [
    {"label": "Ventas totales", "value": 125430.55, "change": "+12.4%", "direction": "up"},
    {"label": "Ticket promedio", "value": 382.14, "change": "-2.1%", "direction": "down"}
  ],
  "table": {
    "columns": ["Sucursal", "Ventas Totales", "Ticket Promedio", "Operaciones"],
    "rows": [
      ["Puebla", 50000, 410.5, 122],
      ["Cholula", 42000, 375.2, 112],
      ["Atlixco", 33430, 348.8, 96]
    ]
  },
  "chart": {
    "type": "bar",
    "x": ["Puebla", "Cholula", "Atlixco"],
    "series": [
      {"name": "Ventas Totales", "data": [50000, 42000, 33430]}
    ]
  }
}
```

**Paso 5: Redactar resumen (Gemini Flash)**

Input: datos calculados + pregunta original
Output: 1-2 oraciones. Sin relleno.

Ejemplo: "Ventas +12.4% vs trimestre anterior. Puebla lidera con 40% del total (50k). Ticket promedio bajo 2.1%."

**Paso 6: Registrar (SQLite)**

```json
{
  "timestamp": "2026-03-30T10:15:00",
  "user_id": "usr_042",
  "question": "ventas por sucursal este trimestre",
  "intent_parsed": { ... },
  "validation_passed": true,
  "columns_used": ["sucursal", "ventas", "fecha"],
  "execution_ms": 87,
  "response_summary": "Ventas +12.4% vs trimestre anterior..."
}
```

---

## Controles anti-alucinacion

| Control | Implementacion |
|---------|---------------|
| Temperatura baja | 0.1 en interpretacion, 0.2 en redaccion |
| Schema forzado | Gemini structured output, no texto libre |
| Validacion pre-ejecucion | Pydantic rechaza columnas/metricas inventadas |
| Catalogo cerrado | Solo metricas definidas en data_dictionary |
| Confianza minima | Si confidence < 0.7, pedir aclaracion |
| Datos reales | Gemini nunca ve los datos crudos, solo el resumen calculado por Python |
| Sin codigo generado | El modelo no genera SQL ni Python; las funciones son fijas |
| Normalizacion de sinonimos | Aliases en el catalogo mapean variaciones comunes |

---

## Manejo de ambiguedad

Cuando el usuario pregunte algo vago:

| Pregunta | Problema | Accion |
|----------|----------|--------|
| "como van las ventas?" | Sin dimension, sin periodo | Responder con resumen general del ultimo periodo disponible |
| "comparame todo" | Sin especificar que ni contra que | Pedir aclaracion: "Que metricas quieres comparar y en que periodo?" |
| "dame el ROI" | Metrica no definida | Responder: "La metrica ROI no esta disponible. Metricas disponibles: ventas_totales, ticket_promedio, conteo_operaciones" |
| "ventas del lunes" | Fecha ambigua | Resolver al lunes mas reciente dentro del rango del dataset |

---

## Rate limiting y cache

- **Rate limit:** 20 consultas/minuto por usuario (configurable)
- **Cache de intenciones:** hash(pregunta_normalizada + dataset_id) -> respuesta
- **TTL del cache:** 5 minutos (los datos no cambian en tiempo real)
- **Cache en memoria** para <50 usuarios, Redis si escala mas

---

## Escalado Gemini Flash -> Pro

Criterios para escalar a Gemini 2.5 Pro:

1. `confidence < 0.5` en la interpretacion de Flash
2. Usuario pide explicacion ("por que bajaron las ventas?")
3. Consulta multi-paso (correlaciones, tendencias complejas)
4. Flash falla validacion 2+ veces seguidas

---

## Plan de desarrollo

### Fase 1: Backend core (Semana 1-2)

- [ ] Inicializar proyecto FastAPI + estructura de carpetas
- [ ] Implementar `config.py` con Pydantic BaseSettings
- [ ] Implementar `database.py` - conexion DuckDB
- [ ] Implementar `dataset_profiler.py` - generar data_dictionary desde CSV
- [ ] Implementar endpoint `POST /datasets/upload`
- [ ] Implementar endpoint `GET /datasets` (listar datasets cargados)
- [ ] Implementar modelos Pydantic: `intent.py`, `response.py`, `dataset.py`
- [ ] Tests unitarios del profiler y modelos

### Fase 2: Agente de interpretacion (Semana 2-3)

- [ ] Implementar `gemini_client.py` - cliente con retry, timeout, structured output
- [ ] Implementar `intent_parser.py` - NL -> plan estructurado
- [ ] Disenar system prompt estricto con inyeccion de data_dictionary
- [ ] Implementar validacion del plan contra catalogo
- [ ] Implementar `query_executor.py` - plan validado -> SQL -> resultados
- [ ] Implementar `response_builder.py` - resultados -> KPIs + tabla + chart
- [ ] Implementar `summary_writer.py` - Gemini redacta resumen corto
- [ ] Implementar endpoint `POST /query`
- [ ] Tests de integracion del flujo completo

### Fase 3: Controles y robustez (Semana 3-4)

- [ ] Implementar cache de intenciones (en memoria primero)
- [ ] Implementar rate limiter por usuario
- [ ] Implementar logging de auditoria (SQLite)
- [ ] Implementar manejo de ambiguedad (pedir aclaracion)
- [ ] Implementar escalado Flash -> Pro por confianza
- [ ] Implementar normalizacion de sinonimos
- [ ] Tests de edge cases (preguntas vagas, metricas inventadas, CSV malformado)

### Fase 4: Frontend (Semana 4-5)

- [ ] Inicializar proyecto React + Vite + TypeScript
- [ ] Implementar `QueryInput.tsx` - barra de consulta
- [ ] Implementar `KPICards.tsx` - tarjetas de metricas
- [ ] Implementar `DataTable.tsx` - tabla con TanStack Table
- [ ] Implementar `Chart.tsx` - graficas con ECharts
- [ ] Implementar `ResponsePanel.tsx` - orquestador de componentes
- [ ] Implementar `DatasetSelector.tsx` - selector de dataset
- [ ] Implementar hooks `useQuery.ts` y `useDatasets.ts`
- [ ] Conectar frontend con backend

### Fase 5: Integracion y deploy (Semana 5-6)

- [ ] Dockerizar backend y frontend
- [ ] Configurar docker-compose
- [ ] Configurar CORS y autenticacion basica
- [ ] Pruebas con datos reales del cliente
- [ ] Ajuste de prompts basado en resultados reales
- [ ] Documentar endpoints (OpenAPI ya generado por FastAPI)
- [ ] Deploy a servidor central

### Fase 6: Refinamiento post-deploy (Semana 6-8)

- [ ] Analizar logs de auditoria para detectar patrones de preguntas
- [ ] Agregar metricas/dimensiones segun uso real
- [ ] Optimizar prompts basado en trazabilidad
- [ ] Implementar WebSocket si la latencia HTTP es problema
- [ ] Evaluar si Redis es necesario segun carga real
- [ ] Agregar exportacion (CSV, PDF) si se requiere

---

## Dependencias clave

```
# backend/requirements.txt
fastapi>=0.115.0
uvicorn>=0.34.0
duckdb>=1.2.0
pydantic>=2.10.0
google-genai>=1.0.0
python-multipart>=0.0.18
httpx>=0.28.0
```

```
# frontend/package.json (dependencias principales)
react
@tanstack/react-table
echarts / echarts-for-react
axios
```

---

## Decisiones importantes

1. **DuckDB en lugar de Polars+DuckDB** — Un solo motor reduce complejidad. DuckDB lee CSV nativo y hace SQL analitico. Si se necesita Polars para ETL complejo, se agrega despues.

2. **Sin generacion de codigo** — El modelo nunca genera SQL ni Python. Las funciones son fijas y parametrizadas. Esto elimina la clase mas peligrosa de alucinaciones.

3. **Cache en memoria primero** — Para <50 usuarios un dict con TTL basta. Redis se agrega solo si se necesita.

4. **SQLite para logs, no Postgres** — Para 100 usuarios y auditoria, SQLite es mas que suficiente y no requiere infraestructura adicional.

5. **Servidor central, no local** — Consistencia de reglas de negocio y catalogo de metricas es mas importante que independencia de cada usuario.
