# Plan de Desarrollo: Agente de Analisis de Datos

## Diagnostico del estado actual

El proyecto tiene una base funcional pero con problemas criticos que impiden usarlo como agente de analisis real.

### Problemas identificados

#### 1. API Key hardcodeada (CRITICO)
- `config.py:37` tiene la API key de Gemini en texto plano
- Debe moverse a variable de entorno o `.env`

#### 2. Prompts de Gemini demasiado debiles
**Intent parser** (`intent_parser.py:127-131`):
```
"Eres un parser estricto pero pragmatico. Devuelve solo un QueryPlan valido.
No inventes columnas ni metricas fuera del catalogo.
Si el usuario es vago, prefiere usar la metrica principal y la fecha principal antes de pedir aclaracion."
```
- No explica que intents existen ni cuando usar cada uno
- No explica como construir filtros de fecha
- No explica como manejar ambiguedad de anos
- No da ejemplos de preguntas y sus planes esperados
- No indica como elegir visualizacion

**Summary writer** (`summary_writer.py:17-19`):
```
"Resume resultados analiticos en espanol en 1 o 2 oraciones.
Sin relleno ni inventar datos."
```
- Demasiado generico, no dice que datos usar ni como formatearlos
- No indica que debe responder directamente la pregunta del usuario

#### 3. Deteccion de fechas en strings falla
- `dataset_profiler.py` solo detecta columnas tipo `date`/`datetime` de DuckDB
- Si el CSV tiene fechas como texto ("15/03/2026", "2026-03-15"), DuckDB las lee como `varchar` y el profiler las ignora
- El dataset real (`ventas-b6c22ab8.json`) tiene 0 columnas de fecha detectadas
- Sin fechas, toda consulta temporal falla

#### 4. Tipos de grafica limitados
- Solo soporta `bar` y `line` (`intent.py:10`: `VisualizationType = Literal["table", "bar", "line"]`)
- Faltan: `pie`, `area`, `scatter`, `pivot_table`
- El frontend solo renderiza SVG basico de barras y lineas

#### 5. Graficas y resultados fuera del chat
- KPIs, graficas y tablas se renderizan en un panel lateral separado (`result-panel`)
- El usuario pregunta en el chat pero la respuesta visual esta en otro lado
- Deberian mostrarse inline en el chat, como lo haria un analista real

#### 6. Logica de heuristics sobredimensionada
- `intent_parser.py` tiene ~550 lineas de heuristics manuales como fallback
- Tokens hardcodeados: `SUMMARY_TOKENS`, `SERIES_TOKENS`, `GENERIC_METRIC_TOKENS`, etc.
- El scoring de confianza es arbitrario (0.45, 0.62, 0.74, 0.78, 0.84)
- Muchos edge cases no cubiertos

#### 7. Sugerencias hardcodeadas en HTML
- `index.html:76-84` tiene 3 sugerencias fijas que asumen un dataset de ventas por sucursal
- Deberian generarse dinamicamente segun el dataset cargado

#### 8. No hay validacion inteligente de ambiguedad temporal
- Si el usuario dice "ventas de abril" y hay datos de 2024 y 2025, el sistema usa el ano mas reciente sin preguntar
- Deberia detectar que hay multiples anos y preguntar cual

#### 9. El resumen no responde la pregunta directamente
- El summary_writer recibe KPIs y highlights pero no sabe que tipo de pregunta fue
- "cuantas ventas hubo en abril?" deberia responder "Hubo X ventas en abril 2026" no un resumen generico

---

## Plan de mejoras

### Fase 1: Fundamentos (seguridad + deteccion de datos)

#### 1.1 Mover API key a .env
**Archivos:** `config.py`, crear `.env`, crear `.env.example`
- Quitar el valor hardcodeado de `gemini_api_key`
- Poner default vacio: `gemini_api_key: str = ""`
- Crear `.env` con la key real (agregar a `.gitignore`)
- Crear `.env.example` como referencia

#### 1.2 Deteccion inteligente de columnas de fecha en strings
**Archivo:** `dataset_profiler.py`

Problema: DuckDB lee muchas fechas como `varchar`. El profiler debe detectar columnas de texto que contengan fechas.

Solucion:
1. Despues de perfilar columnas, para cada columna tipo `string`:
   - Tomar una muestra (10-20 valores no nulos)
   - Intentar parsear como fecha con formatos comunes: `YYYY-MM-DD`, `DD/MM/YYYY`, `MM/DD/YYYY`, `YYYY-MM-DD HH:MM:SS`, etc.
   - Si >80% de la muestra parsea como fecha, marcar la columna como `date` o `datetime`
   - Guardar el formato detectado en el `ColumnProfile` (nuevo campo `detected_date_format`)
2. Registrar la tabla en DuckDB con CAST a DATE para esas columnas
3. Actualizar `_profile_column` para que recalcule min/max como fechas

**Nuevo campo en `ColumnProfile`:**
```python
detected_date_format: str | None = None  # e.g. "%Y-%m-%d", "%d/%m/%Y"
```

#### 1.3 Manejo de ambiguedad temporal multi-ano
**Archivo:** `intent_parser.py`

Cuando el usuario dice "ventas de abril" y el dataset tiene datos de abril 2024 y abril 2025:
1. En `_find_filters`, despues de detectar mes sin ano explicito:
   - Consultar el rango de fechas del dataset
   - Si el rango abarca multiples anos para ese mes, raise `ClarificationNeeded` con:
     - question: "Tengo datos de abril en 2024 y 2025. De cual ano necesitas la informacion?"
     - hints: ["Abril 2024", "Abril 2025", "Ambos"]
2. Si solo hay un ano con datos de ese mes, usar ese ano sin preguntar

---

### Fase 2: Prompt engineering (el cambio mas importante)

#### 2.1 Reescribir system prompt del intent parser
**Archivo:** `intent_parser.py` metodo `_build_system_instruction`

El prompt actual es de 3 lineas. Debe ser un prompt completo que:

```
Eres el interprete de intenciones de un agente de analisis de datos.
Tu trabajo es convertir preguntas en lenguaje natural a un plan de consulta estructurado (QueryPlan).

REGLAS ESTRICTAS:
1. Solo puedes usar columnas, metricas y dimensiones del catalogo proporcionado.
2. Si una metrica o dimension no existe en el catalogo, NO la inventes. Indica en unsupported_metrics.
3. Si la pregunta es ambigua y no puedes resolverla con el contexto del catalogo, baja la confianza y sugiere una aclaracion.

COMO ELEGIR EL INTENT:
- "aggregate_report": cuando el usuario quiere totales, comparaciones, rankings, tops, KPIs
- "time_series_report": cuando el usuario quiere ver evolucion, tendencia, comportamiento a lo largo del tiempo

COMO ELEGIR VISUALIZATION:
- "bar": comparaciones entre categorias (ventas por sucursal, top 5 productos)
- "line": tendencias temporales (ventas por mes, evolucion semanal)
- "pie": proporciones del total (distribucion por categoria, participacion porcentual)
- "area": acumulados o tendencias con volumen (ventas acumuladas por mes)
- "scatter": correlacion entre 2 metricas (ventas vs personas, monto vs cantidad)
- "table": cuando el usuario pide datos tabulares o listados sin grafica
- "pivot_table": cuando quiere cruzar 2 dimensiones (ventas por sucursal y mes)

COMO MANEJAR FECHAS:
- Si el usuario menciona un mes sin ano (ej: "abril"), revisa el rango de fechas en el catalogo.
- Si hay datos de ese mes en un solo ano, usa ese ano.
- Si hay datos en multiples anos, baja confidence a 0.5 y pide aclaracion.
- Usa siempre la columna de fecha principal (default_date_column) a menos que el usuario especifique otra.

COMO MANEJAR PREGUNTAS VAGAS:
- "como van las ventas?" → aggregate_report con la metrica principal, sin filtro de fecha (resumen general)
- "dame un resumen" → aggregate_report con las 2-3 metricas principales, sin dimensiones
- "comparame" sin contexto → baja confidence, pregunta que comparar

CONFIDENCE:
- 0.9-1.0: pregunta clara con metrica, dimension y filtros explicitos
- 0.7-0.89: pregunta razonable donde inferiste algun parametro
- 0.5-0.69: pregunta ambigua pero con una interpretacion plausible
- <0.5: no se puede resolver sin aclaracion

EJEMPLOS:
Pregunta: "cuantas ventas hubo en abril"
Plan: intent=aggregate_report, metrics=[ventas_sum], filters=[fecha between 2026-04-01 and 2026-04-30], visualization=table, confidence=0.92

Pregunta: "grafica de ventas por sucursal"
Plan: intent=aggregate_report, dimensions=[sucursal], metrics=[ventas_sum], visualization=bar, confidence=0.95

Pregunta: "tendencia de ventas mensual en 2026"
Plan: intent=time_series_report, dimensions=[fecha_month], metrics=[ventas_sum], filters=[fecha between 2026-01-01 and 2026-12-31], visualization=line, confidence=0.95

Pregunta: "distribucion porcentual por empresa"
Plan: intent=aggregate_report, dimensions=[empresa], metrics=[ventas_sum], visualization=pie, confidence=0.90

Pregunta: "ventas vs personas por sucursal"
Plan: intent=aggregate_report, dimensions=[sucursal], metrics=[ventas_sum, personas_sum], visualization=scatter, confidence=0.88

CATALOGO DEL DATASET:
{catalog_json}
```

Los ejemplos se deben generar dinamicamente usando las metricas y dimensiones reales del catalogo.

#### 2.2 Reescribir system prompt del summary writer
**Archivo:** `summary_writer.py`

```
Eres un analista de datos que responde en espanol.
Tu trabajo es dar una respuesta DIRECTA a la pregunta del usuario basandote en los datos calculados.

REGLAS:
1. Responde la pregunta primero. Si preguntan "cuantas ventas hubo en abril", responde "Hubo X ventas en abril."
2. Agrega contexto relevante SOLO si aporta valor (cambio porcentual, lider, tendencia).
3. Maximo 2 oraciones. Sin relleno, sin introducciones, sin "Segun los datos...".
4. Usa numeros formateados (125,430 en lugar de 125430.55).
5. Si hay comparacion vs periodo anterior, menciona el cambio.
6. No repitas la pregunta del usuario.
7. No inventes datos que no esten en los KPIs o highlights proporcionados.
```

#### 2.3 Agregar generacion dinamica de ejemplos en el prompt
**Archivo:** `intent_parser.py`

Crear metodo `_build_dynamic_examples(catalog)` que genere 3-5 ejemplos usando:
- Las metricas reales del dataset (no "ventas" hardcodeado)
- Las dimensiones reales
- Las columnas de fecha reales
- Rangos de fecha reales del dataset

---

### Fase 3: Graficas avanzadas + resultados inline en chat

#### 3.1 Agregar tipos de grafica: pie, area, scatter, pivot_table
**Archivos:** `intent.py`, `response.py`, `response_builder.py`, `app.js`

**Modelos:**
```python
# intent.py
VisualizationType = Literal["table", "bar", "line", "pie", "area", "scatter", "pivot_table"]
```

```python
# response.py - ChartData ya soporta series, solo necesita el type correcto
class ChartData(BaseModel):
    type: Literal["bar", "line", "pie", "area", "scatter", "pivot_table", "table"]
    x: list[Any] = Field(default_factory=list)
    series: list[ChartSeries] = Field(default_factory=list)
```

**Response builder** (`response_builder.py`):
- `_build_chart` debe respetar `plan.visualization` en vez de solo mirar `plan.intent`
- Para `pie`: x=dimensiones, series=[una sola serie con datos]
- Para `scatter`: x=metrica1 valores, series=[metrica2 valores]
- Para `pivot_table`: devolver estructura de tabla con subtotales
- Para `area`: igual que line pero el frontend lo renderiza con fill

**Frontend** (`app.js`):
- Agregar `renderPieChart(chart)` — SVG con sectores circulares
- Agregar `renderAreaChart(chart)` — como lineas pero con fill
- Agregar `renderScatterChart(chart)` — puntos x/y
- Agregar `renderPivotTable(chart)` — tabla HTML con subtotales y totales
- Actualizar `renderChart()` para despachar al renderer correcto

#### 3.2 Renderizar resultados inline en el chat
**Archivos:** `app.js`, `styles.css`, `index.html`

Cambio principal: cuando el agente responde con `status: "ok"`, en vez de solo poner el texto del summary en el chat y los resultados en el panel lateral, renderizar TODO inline en el mensaje del chat:

1. Summary text
2. KPI cards (mini version)
3. Grafica (responsive, dentro del bubble del chat)
4. Tabla (colapsable si tiene muchas filas)

**Implementacion:**
- Modificar `handleQuerySubmit` para que al recibir `status: "ok"`:
  - Llame a `pushMessage` con `html: true` y contenido que incluya KPIs + grafica + tabla
  - El panel lateral (`result-panel`) se puede mantener como "ultima respuesta" o eliminarse
- Crear funcion `renderInlineResult(payload)` que genera el HTML completo
- Las graficas inline deben ser mas pequenas (max-width: 100% del bubble)

#### 3.3 Sugerencias dinamicas segun dataset
**Archivos:** `app.js`, posiblemente un nuevo endpoint o usar info del catalog

Cuando se selecciona un dataset:
- Generar 3-4 sugerencias basadas en las metricas y dimensiones del catalogo
- Ejemplo: si el dataset tiene `ventas`, `sucursal`, `fecha`:
  - "total de ventas"
  - "ventas por sucursal"
  - "tendencia de ventas por mes"
  - "top 5 sucursales por ventas"
- Si no tiene fecha: omitir sugerencias temporales
- Renderizar como botones clickeables debajo del input

**Implementacion:**
- En `renderSelectedDataset()` o al cambiar dataset, generar sugerencias
- Usar las propiedades del catalog summary que ya llega al frontend: `metrics_allowed`, `dimensions_allowed`, `default_date_column`

---

### Fase 4: Robustez del agente

#### 4.1 Mejorar respuesta directa segun tipo de pregunta
**Archivo:** `summary_writer.py`

Pasar el `intent` y `plan` al summary writer para que sepa que tipo de respuesta dar:
- Pregunta de conteo ("cuantas ventas") → "Hubo 1,234 ventas en abril 2026."
- Pregunta de total ("total de ventas") → "El total de ventas fue $125,430."
- Pregunta de comparacion ("como van vs anterior") → "Las ventas subieron 12.4% respecto al periodo anterior."
- Pregunta de ranking ("top 5") → "Las 5 sucursales con mas ventas son: Puebla (50k), Cholula (42k)..."
- Pregunta de tendencia ("tendencia mensual") → "Las ventas muestran tendencia alcista, con pico en marzo (45k)."

**Cambio en `_build_prompt`:**
- Incluir el `intent` del plan
- Incluir los filtros aplicados (para contexto temporal)
- Incluir la pregunta original EXACTA del usuario

#### 4.2 Mejorar rewrite de preguntas con historial
**Archivo:** `intent_parser.py` metodo `_rewrite_question_with_history`

Problemas actuales:
- Solo mira la ultima respuesta del agente para decidir como reescribir
- La logica de deteccion de contexto es fragil (busca "fecha" y "columna" en el texto del agente)
- No maneja follow-ups naturales como "y por mes?" o "ahora comparado con el anterior"

Solucion:
- Usar Gemini para reescribir la pregunta con contexto del historial
- Input: historial reciente + pregunta actual
- Output: pregunta autocontenida que no necesita contexto
- Ejemplo: historial="ventas por sucursal en abril" + pregunta="y por mes?" → "ventas por mes en abril"

#### 4.3 Reducir heuristics, confiar mas en Gemini
**Archivo:** `intent_parser.py`

Los heuristics actuales (~400 lineas) son un fallback fragil. Plan:
1. Mantener heuristics SOLO como fallback de emergencia (cuando Gemini no esta disponible)
2. Activar `allow_local_gemini_fallback` = True por default
3. Simplificar los heuristics a lo minimo funcional
4. Mover la logica de deteccion de fecha a un helper compartido
5. Eliminar tokens hardcodeados que Gemini ya puede resolver

#### 4.4 Enviar muestra de datos a Gemini para mejor interpretacion
**Archivos:** `intent_parser.py`, `dataset_profiler.py`

Ya que los datos no son sensibles, podemos mejorar la interpretacion enviando contexto:
- Guardar 5 filas de muestra en el catalogo (nuevo campo `sample_rows`)
- Incluir en el system prompt del intent parser
- Esto ayuda a Gemini a entender el formato real de los datos

**Nuevo campo en `DatasetCatalog`:**
```python
sample_rows: list[dict[str, Any]] = Field(default_factory=list)
```

---

### Fase 5: UX del chat

#### 5.1 Mejorar el diseno del chat para parecer un asistente real
**Archivos:** `styles.css`, `app.js`, `index.html`

- Mensajes del usuario a la derecha, del agente a la izquierda (estilo WhatsApp/ChatGPT)
- Avatar o icono para el agente
- Animacion de "pensando" mas fluida
- Auto-scroll al ultimo mensaje
- Soporte para Enter para enviar (Shift+Enter para nueva linea)

#### 5.2 Indicador de estado del agente
**Archivos:** `app.js`, `styles.css`

Mostrar que esta haciendo el agente:
- "Interpretando tu pregunta..."
- "Ejecutando consulta..."
- "Generando resumen..."

Implementar con actualizaciones de estado en el frontend durante el POST /query (no SSE, solo cambiar el texto del indicador en cada fase).

#### 5.3 Historial persistente por sesion
**Archivo:** `app.js`

- Guardar mensajes en `sessionStorage` para que no se pierdan al recargar
- Limpiar al cerrar pestana
- Boton "Limpiar chat" para reiniciar

---

### Fase 6: Pivot tables y analisis cruzado

#### 6.1 Soporte para pivot tables
**Archivos:** `query_executor.py`, `response_builder.py`, `app.js`

Cuando `visualization = "pivot_table"`:
- El plan tiene 2 dimensiones (ej: sucursal + mes)
- El executor genera la query con ambas dimensiones
- El response_builder construye una tabla pivoteada:
  - Filas = dimension 1 (sucursal)
  - Columnas = dimension 2 (mes)
  - Celdas = metrica (ventas)
  - Fila de totales al final
  - Columna de totales a la derecha

**Nueva estructura de respuesta para pivot:**
```json
{
  "type": "pivot_table",
  "row_dimension": "Sucursal",
  "col_dimension": "Mes",
  "metric": "Total de Ventas",
  "rows": ["Puebla", "Cholula", "Atlixco"],
  "cols": ["Enero", "Febrero", "Marzo"],
  "data": [[10000, 12000, 15000], [8000, 9000, 11000], [6000, 7000, 8000]],
  "row_totals": [37000, 28000, 21000],
  "col_totals": [24000, 28000, 34000],
  "grand_total": 86000
}
```

---

## Orden de ejecucion recomendado

| Prioridad | Fase | Descripcion | Impacto |
|-----------|------|-------------|---------|
| 1 | 1.1 | Mover API key a .env | Seguridad critica |
| 2 | 1.2 | Deteccion de fechas en strings | Sin esto, consultas temporales no funcionan |
| 3 | 2.1 | Reescribir prompt del intent parser | Mejora masiva en calidad de interpretacion |
| 4 | 2.2 | Reescribir prompt del summary writer | Respuestas directas en vez de genericas |
| 5 | 3.2 | Resultados inline en chat | UX fundamental - respuestas donde se esperan |
| 6 | 3.1 | Graficas: pie, area, scatter | Rango de analisis completo |
| 7 | 3.3 | Sugerencias dinamicas | Onboarding para usuarios que no saben que preguntar |
| 8 | 1.3 | Ambiguedad temporal multi-ano | Evita errores silenciosos en datasets multi-ano |
| 9 | 2.3 | Ejemplos dinamicos en prompt | Prompt mas preciso por dataset |
| 10 | 4.1 | Respuesta directa segun tipo de pregunta | Resumen habla como analista real |
| 11 | 4.4 | Muestra de datos al prompt | Mejor interpretacion de datos reales |
| 12 | 4.2 | Rewrite con Gemini | Follow-ups naturales |
| 13 | 5.1 | Diseno del chat | UX profesional |
| 14 | 5.2 | Indicador de estado | Feedback durante procesamiento |
| 15 | 5.3 | Historial persistente | No perder contexto al recargar |
| 16 | 6.1 | Pivot tables | Analisis cruzado avanzado |
| 17 | 4.3 | Reducir heuristics | Mantenibilidad, menos codigo fragil |

---

## Archivos principales a modificar

| Archivo | Cambios |
|---------|---------|
| `backend/app/config.py` | Quitar API key hardcodeada |
| `backend/app/services/intent_parser.py` | Prompt completo, ambiguedad temporal, rewrite con Gemini |
| `backend/app/services/summary_writer.py` | Prompt contextual, respuesta directa |
| `backend/app/services/dataset_profiler.py` | Deteccion de fechas en strings, muestra de datos |
| `backend/app/services/response_builder.py` | Nuevos tipos de grafica, pivot |
| `backend/app/services/query_executor.py` | Soporte para pivot queries |
| `backend/app/models/intent.py` | Nuevos tipos de visualizacion |
| `backend/app/models/response.py` | Nuevos tipos de chart |
| `backend/app/models/dataset.py` | Campos: detected_date_format, sample_rows |
| `backend/app/static/app.js` | Inline results, graficas nuevas, sugerencias dinamicas, UX |
| `backend/app/static/styles.css` | Chat bubbles, inline results, responsividad |
| `backend/app/static/index.html` | Quitar sugerencias hardcodeadas, ajustar layout |
| `.env` | Crear con API key |
| `.env.example` | Crear como referencia |
| `.gitignore` | Agregar .env |

---

## Parte 2: Correcciones criticas post-implementacion

### Diagnostico del error "Gemini no pudo interpretar la consulta"

#### Cadena del error

1. Usuario pregunta "ventas por empresa"
2. `intent_parser.parse()` intenta Flash → falla → intenta Pro → falla
3. `allow_local_gemini_fallback = False` → no hay fallback a heuristics
4. Se lanza `GeminiClientError("Gemini no pudo interpretar la consulta.")` (intent_parser.py:97)
5. query.py lo atrapa como `GeminiClientError` → HTTP 502
6. Frontend muestra "Gemini no pudo interpretar la consulta." como mensaje de sistema

#### Causa raiz: NO es solo Gemini — el dataset esta mal perfilado

El CSV real (`ventas-b6c22ab8.csv`) tiene estas caracteristicas:
- **Separador**: punto y coma (`;`), no coma
- **Decimal**: coma europea (`6335,3448` en vez de `6335.3448`)
- **Fechas como string**: `01/02/2026 0:00` (hora sin cero inicial)

Resultado del profiling actual:
- **0 columnas de fecha detectadas** — `apertura` es string, `date_columns: []`
- **Solo 2 columnas numericas**: `personas` (integer) y `totaldescuentootros` (siempre 0)
- **Todas las columnas financieras son string**: `subtotal`, `total`, `efectivo`, `tarjeta`, etc. tienen valores como `"6335,3448"` que DuckDB lee como varchar

Esto significa:
- La unica metrica util es `personas_sum` — por eso "top 5 apertura por personas" funciona
- "ventas por empresa" falla porque NO EXISTE ninguna metrica de ventas/montos en el catalogo
- Gemini recibe un catalogo con casi nada, no puede mapear "ventas" a nada, y falla
- Incluso si Gemini devolviera un plan, la validacion lo rechazaria

---

### Problemas a resolver (ordenados por impacto)

#### P1. Columnas numericas con formato europeo no se detectan como numericas (CRITICO)
**Archivo**: `dataset_profiler.py`

El CSV usa coma como separador decimal (`6335,3448`). DuckDB con separador `;` lee estos valores como varchar.

**Solucion**:
1. En `_profile_column`, despues de clasificar como "string", agregar deteccion de numeros con formato europeo
2. Tomar muestra de 20 valores no nulos
3. Si >80% matchean patron numerico europeo (`^\d{1,3}(\.\d{3})*(,\d+)?$` o `^\d+(,\d+)?$`), marcar como numeric
4. Guardar `detected_number_format: "european"` en ColumnProfile (nuevo campo)
5. En las expresiones de metricas y dimensiones, usar `REPLACE(REPLACE(col, '.', ''), ',', '.')::DOUBLE` para convertir
6. Reperfilar min/max como numeros reales

**Nuevo campo en ColumnProfile**:
```python
detected_number_format: str | None = None  # "european" (1.234,56) | "standard" (1,234.56) | None
```

#### P2. Columna de fecha "apertura" no se detecta (CRITICO)
**Archivo**: `dataset_profiler.py`

El valor es `01/02/2026 0:00` — la hora tiene formato `H:MM` (sin cero inicial), que no matchea `%H:%M:%S`.

**Solucion**:
1. Agregar formatos de fecha con horas sin segundos a `DATE_FORMATS`:
   - `"%d/%m/%Y %H:%M"` (sin segundos)
2. Antes de intentar parseo, normalizar el valor: strip, y si la hora es `X:MM`, padear a `0X:MM`
3. Verificar que `STRPTIME` de DuckDB soporte el mismo formato; si no, generar la expresion SQL correcta

#### P3. Gemini falla silenciosamente sin fallback (ALTO)
**Archivos**: `intent_parser.py`, `config.py`

- `allow_local_gemini_fallback = False` por defecto — los heuristics existen pero estan desactivados
- `_try_model` atrapa TODAS las excepciones y retorna None sin logging
- Si ambos modelos fallan, el error es generico sin sugerencias

**Solucion**:
1. Cambiar `allow_local_gemini_fallback` default a `True` en config.py
2. Agregar logging en `_try_model` para registrar por que fallo cada modelo
3. En `parse()`, cuando todo falla, devolver `ClarificationNeeded` con sugerencias utiles en vez de error 502

#### P4. No hay manejo gracioso cuando la metrica no existe en el dataset (ALTO)
**Archivo**: `intent_parser.py`

Cuando `unsupported_metrics` tiene items, `_finalize_plan` lanza `PlanValidationError` → HTTP 422 con mensaje tecnico.

**Solucion**:
1. Cambiar `PlanValidationError` por `ClarificationNeeded` con nombres legibles de metricas disponibles
2. En el prompt de Gemini, agregar: "Si la metrica no existe, pon confidence=0.6 y sugiere la metrica mas cercana"

#### P5. Errores se muestran como "Sistema" generico en el frontend (MEDIO)
**Archivo**: `app.js`

Todos los errores HTTP se muestran igual como mensaje de sistema rojo.

**Solucion**:
1. 422 → mostrar como mensaje de agente con sugerencias clickeables
2. 502 → "El servicio de IA no esta disponible. Intenta de nuevo."
3. 500 → "Ocurrio un error. Intenta reformular la pregunta."

#### P6. Verificar manejo de CSV con separador punto y coma (MEDIO)
**Archivo**: `database.py`

Confirmar que `register_csv_view` no fuerza `delimiter=','`. Si DuckDB auto-detecta, no hay problema.

#### P7. Falta logging en toda la cadena del agente (BAJO para usuario, ALTO para debug)
**Archivos**: `intent_parser.py`, `query_executor.py`, `gemini_client.py`

No hay ningun `logging` en el codigo. Agregar logs en puntos criticos:
- `_try_model`: modelo, exito/fallo, razon
- `parse`: pregunta, plan, confianza
- `generate_structured`: si parseo fallo y por que

---

### Orden de ejecucion Parte 2

| # | Problema | Archivo principal | Impacto |
|---|----------|-------------------|---------|
| 1 | P1: Numeros europeos como string | dataset_profiler.py | CRITICO |
| 2 | P2: Fecha con hora sin cero | dataset_profiler.py | CRITICO |
| 3 | P3: Fallback desactivado | config.py, intent_parser.py | ALTO |
| 4 | P4: Metrica inexistente → error | intent_parser.py | ALTO |
| 5 | P6: Verificar delimiter CSV | database.py | MEDIO |
| 6 | P5: Frontend error handling | app.js | MEDIO |
| 7 | P7: Logging | varios | BAJO |

### Nota importante

Despues de implementar P1 y P2, hay que **re-perfilar el dataset** (eliminar el catalogo viejo y re-subir el CSV) para que las columnas financieras se detecten como numericas y `apertura` como fecha. P1 es el cambio mas impactante: convierte ~15 columnas de string a numeric, generando ~60+ metricas nuevas.

---

## Parte 3: Labels legibles + dimensiones temporales avanzadas

### Diagnostico

#### Problema 1: Los nombres de columnas se muestran tal cual del CSV
`humanize_identifier("TOTALSINDESCUENTO")` → `"Totalsindescuento"` — solo capitaliza, no separa palabras.

Esto afecta **todo**: KPIs ("Total de Totalsindescuento"), graficas, tablas, ejes de charts y respuestas del summary writer. El usuario ve nombres tecnicos en lugar de etiquetas naturales como "Total sin Descuento".

La funcion `humanize_identifier()` en `core/utils.py:46-48` solo reemplaza `_` por espacios y aplica `.title()`. No puede separar palabras pegadas en camelCase o ALLCAPS como `TOTALSINDESCUENTO`, `DESCUENTOIMPORTE`, `SUBTOTAL`.

#### Problema 2: No existe dimension "dia de la semana"
El profiler genera granularidades `day`, `week`, `month`, `year` via `DATE_TRUNC()` (profiler linea 380). No hay `day_of_week` — si el usuario pide "ventas por dia de la semana" o "dame lunes vs viernes", el agente no puede resolverlo porque no existe esa dimension en el catalogo.

DuckDB soporta `DAYOFWEEK()` y `DAYNAME()` nativamente, solo falta generarla.

---

### P3.1 Labels inteligentes generados por Gemini (ALTO)

**Archivos:** `models/dataset.py`, `services/dataset_profiler.py`, `core/gemini_client.py`, `routers/datasets.py`

**Objetivo:** Que Gemini asigne labels legibles a cada columna durante el profiling, adaptandose a cualquier CSV sin diccionarios hardcodeados. Que el usuario pueda corregir labels despues.

#### Por que Gemini y no heuristics

Cada CSV tiene nombres distintos — `TOTALSINDESCUENTO`, `qty_sold`, `FechaAperturaTienda`, `mntVtaBruta`. Un diccionario de palabras no escala. Gemini ya sabe interpretar abreviaciones, siglas, camelCase, y contexto de dominio (si ve `col1=EFECTIVO` junto a `col2=TARJETA`, infiere que son metodos de pago). Una sola llamada a Flash durante el profiling resuelve todos los casos.

#### Paso 1: Agregar campo `label` a `ColumnProfile`
**Archivo:** `models/dataset.py`

```python
class ColumnProfile(BaseModel):
    ...
    label: str | None = None  # etiqueta legible asignada por Gemini
```

#### Paso 2: Nuevo metodo `_generate_column_labels()` en el profiler
**Archivo:** `services/dataset_profiler.py`

Crear metodo que haga UNA sola llamada a Gemini Flash para etiquetar todas las columnas de golpe:

```python
def _generate_column_labels(
    self,
    columns: dict[str, ColumnProfile],
    sample_rows: list[dict[str, Any]],
) -> dict[str, str]:
    """Llama a Gemini Flash para generar labels legibles para cada columna."""
```

**System instruction:**
```
Eres un asistente que genera etiquetas legibles en espanol para columnas de un dataset CSV.
Para cada nombre de columna, genera una etiqueta corta y natural que un usuario no tecnico entienda.

REGLAS:
- Separa palabras pegadas: TOTALSINDESCUENTO → "Total sin Descuento"
- Traduce abreviaciones comunes: qty → cantidad, amt → monto, desc → descuento
- Respeta el idioma original si es claro; si el nombre esta en ingles, traduce al espanol
- Usa las filas de muestra como contexto para entender que representa cada columna
- Maximo 4 palabras por label
- No uses ALL CAPS ni snake_case en el label — usa formato titulo natural
- Si el nombre ya es legible, dejalo como esta con formato titulo
```

**Prompt:** JSON con los nombres de columna, sus tipos, y 3 sample rows.

**Response model (Pydantic para `generate_structured`):**
```python
class ColumnLabelsResponse(BaseModel):
    labels: dict[str, str]  # {"TOTALSINDESCUENTO": "Total sin Descuento", ...}
```

**Llamada:** Usar `gemini_client.generate_structured()` con Flash, temperature 0.1. Es una sola llamada ligera — no impacta el tiempo de upload significativamente.

**Fallback:** Si Gemini no esta configurado o falla, usar `humanize_identifier()` actual como fallback (solo .title() y separar underscores). El profiling nunca debe fallar por no poder generar labels.

#### Paso 3: Integrar en `_build_catalog()`
**Archivo:** `services/dataset_profiler.py` — en `_build_catalog()` despues de `_assign_semantic_roles()` (linea 164)

```python
# Generar labels con Gemini (o fallback a humanize_identifier)
column_labels = self._generate_column_labels(columns, sample_rows)
for name, profile in columns.items():
    profile.label = column_labels.get(name, humanize_identifier(name))
```

El profiler necesita acceso a `GeminiClient`. Agregarlo al constructor:
```python
class DatasetProfiler:
    def __init__(self, settings: Settings, db_manager: DuckDBManager, gemini_client: GeminiClient):
        ...
        self.gemini_client = gemini_client
```

Y en `main.py` donde se instancia el profiler, pasarle el gemini_client existente.

#### Paso 4: Propagar labels a dimensiones y metricas
**Archivo:** `services/dataset_profiler.py`

Donde hoy se usa `humanize_identifier(name)`, usar `columns[name].label`:

- `_build_dimensions()` linea 359: `label=columns[name].label`
- `_build_dimensions()` linea 374: `label=columns[date_column].label`
- `_build_dimensions()` linea 384: `label=f"{columns[date_column].label} por {_GRANULARITY_LABELS[granularity]}"`
- `_build_metrics()` lineas 414,422,429,439: `label=f"Total de {columns[name].label}"` etc.

Para evitar redundancia ("Total de Total sin Descuento"), si el label ya contiene la palabra del agregador, no prefijar:
```python
col_label = columns[name].label
if any(w in col_label.lower() for w in ("total", "subtotal", "suma")):
    sum_label = col_label
else:
    sum_label = f"Total de {col_label}"
```

#### Paso 5: Endpoint para que el usuario corrija labels
**Archivo:** `routers/datasets.py`

Nuevo endpoint PATCH para que el usuario pueda renombrar labels despues del profiling:

```python
class LabelUpdate(BaseModel):
    column_labels: dict[str, str]  # {"TOTALSINDESCUENTO": "Venta Bruta"}

@router.patch("/{dataset_id}/labels", response_model=DatasetSummary)
async def update_labels(request: Request, dataset_id: str, body: LabelUpdate):
    ...
```

Logica:
1. Cargar el catalogo existente (`_load_catalog`)
2. Actualizar `profile.label` para cada columna en `body.column_labels`
3. Regenerar `dimension_definitions` y `metrics_allowed` con los nuevos labels (reusar `_build_dimensions` y `_build_metrics`)
4. Regenerar `aliases` (reusar `_build_aliases`)
5. Guardar catalogo actualizado (`_save_catalog`)

Esto permite que el usuario ajuste sin re-subir el CSV.

#### Paso 6 (opcional): UI para editar labels
**Archivo:** `app.js` / `index.html`

En la vista de dataset seleccionado, mostrar una lista editable de columnas con sus labels. Al guardar, hacer PATCH al endpoint. Esto es opcional para la primera iteracion — el endpoint basta para integraciones futuras.

#### Flujo completo

```
CSV upload → profiling → _generate_column_labels(Gemini Flash) → labels en ColumnProfile
    ↓                          ↓ (fallback si Gemini falla)
    ↓                     humanize_identifier()
    ↓
_build_dimensions() usa profile.label → DimensionDefinition.label
_build_metrics() usa profile.label → MetricDefinition.label
    ↓
response_builder usa MetricDefinition.label → KPI.label, ChartSeries.name, TableData.columns
    ↓
Frontend muestra "Total sin Descuento" en vez de "TOTALSINDESCUENTO"
    ↓
(opcional) Usuario corrige via PATCH /datasets/{id}/labels → regenera todo
```

---

### P3.2 Granularidades en espanol (MEDIO)
**Archivo:** `services/dataset_profiler.py` linea 384

Actualmente genera labels como `"Fecha Day"`, `"Fecha Month"`. Cambiar a espanol:

```python
_GRANULARITY_LABELS = {
    "day": "Dia",
    "week": "Semana",
    "month": "Mes",
    "year": "Año",
    "day_of_week": "Dia de la Semana",
}
```

Usar en linea 384: `label=f"{col_label} por {_GRANULARITY_LABELS[granularity]}"`

Resultado: `"Fecha por Mes"`, `"Apertura por Semana"`, etc.

---

### P3.3 Dimension "dia de la semana" (ALTO)
**Archivos:** `models/intent.py`, `services/dataset_profiler.py`, `services/query_executor.py`

**Objetivo:** Que el usuario pueda pedir "ventas por dia de la semana" y obtener agrupaciones por Lunes, Martes, etc.

#### Paso 1: Agregar granularidad `day_of_week`
**Archivo:** `models/intent.py:12`

```python
Granularity = Literal["day", "week", "month", "year", "day_of_week"]
```

#### Paso 2: Generar dimension en profiler
**Archivo:** `services/dataset_profiler.py` en `_build_dimensions()`, despues del loop de granularidades (linea 389)

Agregar para cada columna de fecha:
```python
dow_name = f"{date_column}_day_of_week"
dimensions[dow_name] = DimensionDefinition(
    name=dow_name,
    label=f"{columns[date_column].label} Dia de la Semana",
    expression=f"DAYNAME({date_expr})",
    kind="time_granularity",
    source_column=date_column,
    granularity="day_of_week",
)
```

`DAYNAME()` en DuckDB retorna strings en ingles ("Monday", "Tuesday"...).

#### Paso 3: Traducir dias al espanol
**Archivo:** `services/response_builder.py` o `services/query_executor.py`

Opcion A (SQL — preferida): usar `CASE WHEN` en la expression directamente:
```sql
CASE DAYOFWEEK({date_expr})
    WHEN 0 THEN 'Domingo'
    WHEN 1 THEN 'Lunes'
    WHEN 2 THEN 'Martes'
    WHEN 3 THEN 'Miercoles'
    WHEN 4 THEN 'Jueves'
    WHEN 5 THEN 'Viernes'
    WHEN 6 THEN 'Sabado'
END
```

Esto va directamente en la `expression` de la dimension, asi que no necesita post-procesamiento.

#### Paso 4: Agregar alias para dia de semana
**Archivo:** `services/dataset_profiler.py` en `_build_aliases()`

Agregar aliases para que Gemini y los heuristics reconozcan la intencion:
```python
"dia de la semana" → "{date_column}_day_of_week"
"dia semana" → "{date_column}_day_of_week"
"lunes martes" → "{date_column}_day_of_week"
```

#### Paso 5: Orden logico de dias
El `CASE WHEN` con `DAYOFWEEK()` retorna strings, que se ordenan alfabeticamente (Domingo, Jueves, Lunes...) en vez de cronologicamente.

Solucion: Agregar columna auxiliar de orden en la query. En `query_executor.py`, cuando una dimension tiene `granularity="day_of_week"`, agregar `ORDER BY DAYOFWEEK({date_expr})` en vez del orden por defecto.

---

### P3.4 Propagacion de labels al summary writer (MEDIO)
**Archivo:** `services/summary_writer.py`

Actualmente el summary writer recibe KPIs que ya tienen `label`, pero la respuesta de Gemini puede seguir usando nombres tecnicos del plan.

En `_build_prompt()` (linea 47), incluir un mapeo de nombres internos a labels:
```python
if plan:
    lines.append("Nombres legibles de metricas y dimensiones:")
    for m in plan.metrics:
        metric = catalog.metrics_index.get(m)
        if metric:
            lines.append(f"  {m} → {metric.label}")
    for d in plan.dimensions:
        dim = catalog.dimension_definitions.get(d)
        if dim:
            lines.append(f"  {d} → {dim.label}")
```

Esto requiere pasar `catalog` al summary writer (actualmente no lo recibe).

---

### Orden de ejecucion Parte 3

| # | Tarea | Archivo principal | Impacto |
|---|-------|-------------------|---------|
| 1 | P3.1 | utils.py, dataset_profiler.py, dataset.py | ALTO — afecta todas las respuestas visibles |
| 2 | P3.3 | intent.py, dataset_profiler.py | ALTO — habilita analisis por dia de semana |
| 3 | P3.2 | dataset_profiler.py | MEDIO — granularidades en espanol |
| 4 | P3.4 | summary_writer.py | MEDIO — summaries usan labels legibles |

### Nota
Despues de implementar P3.1, hay que **re-perfilar los datasets existentes** para que los nuevos labels se generen. Los catalogos guardados en JSON tendran los labels viejos hasta que se re-suban los CSVs.
