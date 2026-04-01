# Informe de Uso de Tokens — Agente de Analisis CSV

## Resumen Ejecutivo

El sistema realiza **2 llamadas a Gemini por consulta** (intent + summary) y **1 llamada al subir CSV** (labels).
El consumo total por consulta oscila entre **5,500 y 8,000 tokens** (dataset mediano, ~25 columnas).
El **catalogo JSON inyectado en el intent parser consume 70-80%** del total de tokens por consulta.

---

## 1. Flujo de una Consulta y Llamadas a Gemini

```
Usuario hace pregunta
    |
[Cache check] --> HIT --> 0 tokens (respuesta cacheada 5 min)
    |
    v  MISS
[Intent Parser]  --> Gemini Flash (llamada #1)
    |
[Query Executor]  --> DuckDB local (0 tokens)
    |
[Response Builder] --> Python puro (0 tokens)
    |
[Summary Writer]  --> Gemini Flash (llamada #2)
    |
[Cache store] --> respuesta guardada 300s
```

---

## 2. Desglose por Llamada

### 2.1 Intent Parser (`services/intent_parser.py`)

| Componente | Tokens aprox. | % del total | Escala con... |
|---|---|---|---|
| **System instruction** (reglas fijas) | ~500 | 8% | Fijo |
| **Catalogo JSON** (columnas, metricas, dimensiones, aliases) | ~3,500-5,000 | **70-78%** | Num. columnas |
| **Sample rows** (3 filas de ejemplo) | ~200-400 | 5% | Num. columnas |
| **Prompt** (pregunta + historial 6 turnos) | ~100-500 | 5-8% | Long. historial |
| **Output** (AgentDecision JSON) | ~200-500 | — | — |
| **TOTAL INPUT** | **~4,300-6,400** | 100% | — |

**Detalle del catalogo JSON** (el componente dominante):

Para un dataset de 25 columnas tipico:
- `columns` (25 cols x metadata): ~2,000 tokens
- `dimensions_allowed` (~25 dims con granularidades): ~1,200 tokens
- `metrics_allowed` (~40 metricas: sum/avg/min/max): ~1,000 tokens
- `aliases` (mapa de sinonimos): ~300-500 tokens
- `sample_rows` (3 filas): ~200-400 tokens

**Escalamiento por tamano de dataset:**

| Columnas | Tokens catalogo | Tokens totales intent | Costo USD (Flash) |
|---|---|---|---|
| 10 | ~1,500 | ~2,500 | ~$0.008 |
| 25 | ~4,500 | ~5,800 | ~$0.017 |
| 50 | ~8,000 | ~9,500 | ~$0.029 |
| 100+ | ~15,000+ | ~16,500+ | ~$0.050+ |

### 2.2 Summary Writer (`services/summary_writer.py`)

| Componente | Tokens aprox. | % del total | Escala con... |
|---|---|---|---|
| **System instruction** (8 reglas fijas) | ~200 | 25% | Fijo |
| **Prompt**: pregunta del usuario | ~20-50 | 5% | Long. pregunta |
| **Prompt**: tipo de consulta + mappings | ~50-200 | 15% | Num. metricas/dims |
| **Prompt**: filtros aplicados | ~20-100 | 8% | Num. filtros |
| **Prompt**: KPIs calculados | ~50-300 | 20% | Num. KPIs |
| **Prompt**: highlights | ~30-250 | 15% | Num. highlights |
| **Output** (1-2 oraciones) | ~30-80 | — | — |
| **TOTAL INPUT** | **~400-1,100** | 100% | — |

**Costo por llamada:** ~$0.001-0.003 USD (Flash)

### 2.3 Dataset Profiler — Labels (`services/dataset_profiler.py`)

| Componente | Tokens aprox. | Escala con... |
|---|---|---|
| **System instruction** (reglas de etiquetas) | ~150 | Fijo |
| **Payload JSON** (columnas + sample values + sample rows) | ~200-800 | Num. columnas |
| **Output** (mapa nombre→label) | ~100-300 | Num. columnas |
| **TOTAL INPUT** | **~350-950** | — |

**Se ejecuta solo 1 vez** al subir/procesar un CSV. Costo negligible.

---

## 3. Costo Total por Consulta (Gemini 2.5 Flash)

**Precios actuales del modelo:**
- Input: $0.30 / millon tokens
- Output: $2.50 / millon tokens
- Thinking: $2.50 / millon tokens
- Cached: $0.03 / millon tokens (90% descuento)

| Escenario | Input tokens | Output tokens | Costo aprox. |
|---|---|---|---|
| Cache HIT | 0 | 0 | $0.000 |
| Dataset chico (10 cols) | ~2,900 | ~300 | ~$0.002 |
| Dataset mediano (25 cols) | ~6,900 | ~500 | ~$0.003 |
| Dataset grande (50 cols) | ~10,600 | ~500 | ~$0.005 |
| Dataset muy grande (100+ cols) | ~17,500+ | ~600 | ~$0.008+ |

> **Nota:** Los thinking tokens de Gemini 2.5 Flash se cobran como output ($2.50/M) y pueden agregar ~1,000-8,000 tokens adicionales por llamada. El costo real puede ser 2-4x mayor que la tabla anterior.

---

## 4. Distribucion Visual del Consumo

```
POR CONSULTA (dataset 25 cols, cache miss):

Intent Parser [==================================] 85% (~5,800 tokens)
  |- Catalogo JSON  [========================]     70% del intent
  |- System instr.  [===]                           8%
  |- Sample rows    [==]                            5%
  |- Prompt+hist    [==]                            5%

Summary Writer [=====]                             15% (~800 tokens)
  |- KPIs+highlights [==]
  |- System instr.   [=]
  |- Prompt          [=]
```

---

## 5. Que se Puede Optimizar

### 5.1 Context Caching de Gemini API (MAYOR IMPACTO)

**Problema:** Cada consulta envia el mismo system instruction + catalogo (~4,500 tokens para 25 cols). Este bloque es identico mientras el dataset no cambie.

**Solucion:** Usar [Context Caching](https://ai.google.dev/gemini-api/docs/caching) de la API de Gemini. Se cachea el system instruction en el servidor de Google y solo se envia la pregunta del usuario.

**Impacto:**
- Los ~4,500 tokens del catalogo pasan de $0.30/M a $0.03/M (90% descuento)
- Ahorro por consulta: ~$0.0012 → escalable a cientos de consultas/dia
- Cache dura minimo 1 hora en Gemini (configurable)
- **No afecta la capacidad de entendimiento** — el modelo recibe exactamente la misma informacion

**Implementacion:** Modificar `gemini_client.py` para crear un cached content con el system instruction + catalogo, y reutilizarlo en llamadas subsecuentes del mismo dataset.

### 5.2 Comprimir el Catalogo JSON (IMPACTO MEDIO)

**Problema:** El catalogo envia campos que rara vez influyen en la decision del modelo.

**Campos que se pueden reducir sin perder capacidad:**

| Campo | Tokens | Accion | Riesgo |
|---|---|---|---|
| `aliases` (mapa completo) | ~300-500 | Resolver aliases **antes** de enviar a Gemini (en Python) | Ninguno — el modelo no necesita ver sinonimos si ya se resolvieron |
| `sample_rows` (3 filas) | ~200-400 | Reducir a 1 fila o eliminar | Bajo — el modelo ya tiene types y semantic_role |
| `non_null_ratio`, `uniqueness_ratio`, `boolean_like` | ~150 | Eliminar — no afectan la generacion de planes | Ninguno |
| `min`/`max` por columna | ~200 | Enviar solo para columnas de fecha (para rangos temporales) | Bajo |

**Ahorro estimado:** ~600-1,200 tokens por llamada (15-25% del catalogo)

### 5.3 Usar Flash Lite para Summary Writer (IMPACTO BAJO-MEDIO)

**Problema:** El summary writer usa `gemini-2.5-flash` para generar 1-2 oraciones simples a partir de datos ya calculados. Es una tarea trivial para el modelo.

**Solucion:** Cambiar a `gemini-2.5-flash-lite` ($0.10/M input vs $0.30/M).

**Impacto:**
- 66% reduccion en costo de input del summary
- La tarea (resumir KPIs en 2 oraciones) esta dentro de la capacidad de flash-lite
- **No afecta al intent parser** que es donde se necesita mayor capacidad

### 5.4 Reducir Historial de Conversacion (IMPACTO BAJO)

**Estado actual:** Se envian los ultimos 6 turnos (`history[-6:]` en `intent_parser.py:171`).

**Solucion:** Reducir a 3-4 turnos. En analisis de datos, el contexto relevante suele estar en las ultimas 2-3 preguntas.

**Ahorro:** ~100-300 tokens por llamada con historial largo.

**Riesgo:** Minimo — las consultas de datos rara vez dependen de contexto de hace 6 turnos.

### 5.5 Cache TTL mas Agresivo (IMPACTO VARIABLE)

**Estado actual:** `cache_ttl_seconds = 300` (5 minutos).

**Solucion:** Aumentar a 600-900 segundos para datasets que no cambian frecuentemente. Cada cache hit ahorra 2 llamadas completas a Gemini.

**Riesgo:** Ninguno si el dataset no se actualiza entre consultas.

---

## 6. Resumen de Optimizaciones

| Optimizacion | Ahorro por consulta | Dificultad | Riesgo para entendimiento |
|---|---|---|---|
| Context Caching API | 70-80% en intent parser | Media | **Cero** |
| Comprimir catalogo | 15-25% en intent parser | Baja | **Bajo** |
| Flash Lite para summary | 66% en summary writer | Baja | **Bajo** |
| Reducir historial 6→3 | ~100-300 tokens | Trivial | **Minimo** |
| Cache TTL 300→600s | 0 tokens (mas cache hits) | Trivial | **Cero** |

**Recomendacion prioritaria:** Implementar **Context Caching** primero — es el cambio con mayor impacto y cero riesgo para la capacidad del agente.

---

## 7. Archivos Clave

| Archivo | Rol | Llamadas Gemini |
|---|---|---|
| `backend/app/services/intent_parser.py` | Interpreta pregunta → QueryPlan | 1 (el mas costoso) |
| `backend/app/services/summary_writer.py` | Resume resultados en texto | 1 (bajo costo) |
| `backend/app/services/dataset_profiler.py` | Genera labels al subir CSV | 1 (solo al upload) |
| `backend/app/core/gemini_client.py` | Wrapper del SDK de Gemini | Todas pasan por aqui |
| `backend/app/core/cache.py` | Cache TTL en memoria | Evita llamadas repetidas |
| `backend/app/config.py` | Modelos, temperaturas, TTL | Configuracion central |
| `backend/app/core/telemetry.py` | Tracking de tokens y costos | Medicion |
