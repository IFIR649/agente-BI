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

---

## 7. Implementacion

Las optimizaciones se implementan de menor a mayor complejidad. Cada paso es independiente y reversible.

---

### Paso 1 — Ajustes triviales de configuracion

**Archivo:** `backend/app/config.py`

Dos cambios de una linea cada uno:

```python
# Linea 33 — aumentar TTL del cache de respuestas de 5 a 10 minutos
cache_ttl_seconds: int = 600  # era 300

# Linea 39 — modelo mas barato para el summary writer
# (se usara en summary_writer.py al agregar la nueva clave)
gemini_lite_model: str = "gemini-2.5-flash-lite"
```

---

### Paso 2 — Usar Flash Lite en Summary Writer

**Archivo:** `backend/app/services/summary_writer.py`

El `SummaryWriter` genera 1-2 oraciones a partir de KPIs ya calculados. Es la tarea mas simple del sistema. Cambio en la linea 46:

```python
# Antes (linea 46):
model=self.settings.gemini_flash_model,

# Despues:
model=self.settings.gemini_lite_model,
```

Solo se necesita que `Settings` tenga la nueva clave `gemini_lite_model` del Paso 1.

---

### Paso 3 — Comprimir el catalogo en Intent Parser

**Archivo:** `backend/app/services/intent_parser.py`  
**Metodo:** `_build_system_instruction` (lineas 98-141)

Se eliminan campos que el modelo no usa para tomar decisiones de plan, y se limita el historial.

**3a. Reducir campos de columna** — lineas 106-118:

```python
# Antes: envia 8 campos por columna
{
    "name": name,
    "type": column.type,
    "label": column.label,
    "semantic_role": column.semantic_role,
    "non_null_ratio": round(column.non_null_ratio, 3),   # <-- eliminar
    "uniqueness_ratio": round(column.uniqueness_ratio, 3), # <-- eliminar
    "boolean_like": column.boolean_like,                   # <-- eliminar
    "min": column.min_value,
    "max": column.max_value,
}

# Despues: 5 campos por columna (min/max solo si es columna de fecha o numerica)
{
    "name": name,
    "type": column.type,
    "label": column.label,
    "semantic_role": column.semantic_role,
    **({"min": column.min_value, "max": column.max_value}
       if column.semantic_role in ("time", "measure") else {}),
}
```

**3b. Eliminar aliases del catalogo** — linea 139:

Los aliases se usan en Python para normalizar la pregunta *antes* de enviarla a Gemini (ver `core/utils.py`). El modelo no necesita ver el mapa raw.

```python
# Antes (linea 139):
"aliases": catalog.aliases,

# Despues: eliminar esta linea completa
# (catalog.aliases sigue usandose en _finalize_plan para resolver nombres)
```

**3c. Reducir sample_rows de 3 a 1** — linea 140:

```python
# Antes:
"sample_rows": catalog.sample_rows[:3],

# Despues:
"sample_rows": catalog.sample_rows[:1],
```

**3d. Reducir historial de 6 a 3 turnos** — linea 171:

```python
# Antes:
recent = history[-6:]

# Despues:
recent = history[-3:]
```

---

### Paso 4 — Context Caching de la API de Gemini (mayor impacto)

**Archivos a modificar:**
- `backend/app/core/gemini_client.py` — agregar metodo `create_cached_content` y soporte para `cached_content_name` en `generate_structured_result`
- `backend/app/services/intent_parser.py` — cachear el system instruction por `catalog_version`

**Como funciona:** La API de Gemini permite enviar el system instruction una sola vez y obtener un `cache_name`. Las llamadas subsecuentes referencian ese nombre en lugar de reenviar el texto completo. El costo baja de $0.30/M a $0.03/M para los tokens cacheados. Duracion minima: 1 hora.

**4a. Agregar metodo en `gemini_client.py`** (despues de `__init__`):

```python
def create_cached_content(
    self,
    *,
    system_instruction: str,
    model: str,
    ttl_hours: int = 2,
) -> str:
    """Crea un cached content en Gemini y retorna su nombre (cache_name)."""
    genai, types = self._load_sdk()
    client = genai.Client(api_key=self.settings.gemini_api_key)
    cached = client.caches.create(
        model=model,
        config=types.CreateCachedContentConfig(
            system_instruction=system_instruction,
            ttl=f"{ttl_hours * 3600}s",
        ),
    )
    return cached.name  # ej: "cachedContents/abc123"
```

**4b. Modificar `generate_structured_result` en `gemini_client.py`** para aceptar `cached_content_name` opcional:

```python
def generate_structured_result(
    self,
    *,
    system_instruction: str,
    prompt: str,
    response_model: type[T],
    model: str,
    temperature: float,
    cached_content_name: str | None = None,  # <-- nuevo parametro
) -> GeminiCallResult:
    ...
    config_kwargs = {
        "temperature": temperature,
        "response_mime_type": "application/json",
        "response_schema": response_model,
    }
    if cached_content_name:
        config_kwargs["cached_content"] = cached_content_name
        # NO enviar system_instruction cuando hay cached_content
    else:
        config_kwargs["system_instruction"] = system_instruction

    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(**config_kwargs),
    )
```

**4c. Cache de `cache_name` por dataset en `intent_parser.py`:**

El cache name se guarda en un dict en memoria keyed por `(catalog.id, catalog.catalog_version)`. Se crea la primera vez y se reutiliza hasta que expire.

```python
class IntentParser:
    def __init__(self, settings: Settings, gemini_client: GeminiClient) -> None:
        self.settings = settings
        self.gemini_client = gemini_client
        self._gemini_cache: dict[tuple[str, str], str] = {}  # (catalog_id, version) -> cache_name

    def _get_or_create_cache(self, catalog: DatasetCatalog) -> str | None:
        """Retorna un cache_name de Gemini para el system instruction de este catalogo."""
        key = (catalog.id, catalog.catalog_version)
        if key in self._gemini_cache:
            return self._gemini_cache[key]
        try:
            cache_name = self.gemini_client.create_cached_content(
                system_instruction=self._build_system_instruction(catalog),
                model=self.settings.gemini_flash_model,
                ttl_hours=2,
            )
            self._gemini_cache[key] = cache_name
            logger.info("gemini_cache created catalog_id=%s version=%s name=%s",
                        catalog.id, catalog.catalog_version, cache_name)
            return cache_name
        except Exception as exc:
            logger.warning("gemini_cache create failed: %s — fallback to no cache", exc)
            return None

    def parse(self, ...) -> AgentDecision:
        ...
        cache_name = self._get_or_create_cache(catalog)

        result = self.gemini_client.generate_structured_result(
            system_instruction=self._build_system_instruction(catalog),
            prompt=prompt,
            response_model=StructuredAgentDecision,
            model=model_name,
            temperature=self.settings.gemini_temperature_intent,
            cached_content_name=cache_name,  # None si fallo la creacion
        )

```

> **Nota:** El Context Caching de Gemini tiene un minimo de 1,024 tokens para activarse. Con cualquier dataset de mas de ~12 columnas ya supera ese umbral.

---

### Orden de implementacion recomendado

| Paso | Archivo | Esfuerzo | Ahorro tokens |
|---|---|---|---|
| 1 | `config.py` | 5 min | Indirecto |
| 2 | `summary_writer.py` | 2 min | 66% costo summary |
| 3a-3d | `intent_parser.py` | 20 min | 15-25% intent |
| 4 | `gemini_client.py` + `intent_parser.py` | 1-2 h | 70-80% intent |

---

## 8. Archivos Clave

| Archivo | Rol | Llamadas Gemini |
|---|---|---|
| `backend/app/services/intent_parser.py` | Interpreta pregunta → QueryPlan | 1 (el mas costoso) |
| `backend/app/services/summary_writer.py` | Resume resultados en texto | 1 (bajo costo) |
| `backend/app/services/dataset_profiler.py` | Genera labels al subir CSV | 1 (solo al upload) |
| `backend/app/core/gemini_client.py` | Wrapper del SDK de Gemini | Todas pasan por aqui |
| `backend/app/core/cache.py` | Cache TTL en memoria | Evita llamadas repetidas |
| `backend/app/config.py` | Modelos, temperaturas, TTL | Configuracion central |
| `backend/app/core/telemetry.py` | Tracking de tokens y costos | Medicion |
