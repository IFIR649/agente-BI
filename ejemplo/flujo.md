# Flujo simple de la web con API protegida

Este documento explica, sin meterse de más en lo técnico, cómo debe trabajar una aplicación web con el backend actual.

El ejemplo práctico está en [index.html](./index.html).

## Qué necesita la web para funcionar

La web necesita mandar siempre estos datos en las llamadas al backend:

- `X-API-Key`: la clave global que permite entrar a la API.
- `X-User-Id`: el usuario real que está usando la aplicación.

El backend soporta otros headers opcionales para trazabilidad, pero la web normal no necesita pedirlos ni mostrarlos.

## Qué rutas quedan abiertas

Estas rutas se pueden abrir sin API key:

- `/`
- `/test`
- `/analytics`
- `/docs`
- `/openapi.json`
- `/api-info`
- `/static/*`
- `/health`

Ojo: que la página cargue no significa que ya pueda consultar datos. Para usar la API real, la web sí debe mandar `X-API-Key` y `X-User-Id`.

## Flujo recomendado

### 1. Guardar el acceso

Primero la web debe pedir o recuperar:

- API base
- API key
- User ID

Lo normal es guardar eso en `localStorage` para no pedirlo en cada recarga.

### 2. Crear una sesión temporal

La web llama:

- `POST /sessions`

Con eso el backend devuelve un `token`.

Ese `token` representa una sesión temporal del chat. Esa sesión queda amarrada a:

- la misma API key
- el mismo `X-User-Id`

Si luego otro usuario intenta usar ese token, el backend lo rechaza.

### 3. Subir el CSV a esa sesión

Luego la web llama:

- `POST /sessions/{token}/upload`

En esa llamada sube el archivo CSV.

Cuando termina bien:

- la sesión ya tiene su dataset activo
- el chat queda listo
- ya no hace falta selector de datasets

## 4. Consultar el chat

Después la web llama:

- `POST /sessions/{token}/query`

Mandando:

- la pregunta
- un historial corto reciente

El backend responde según el caso:

- resultado normal
- mensaje asistivo
- petición de aclaración

La web solo debe mostrar la respuesta y seguir guardando el historial corto local.

## 5. Mantener viva la sesión

Mientras la pantalla siga abierta, la web debe mandar:

- `POST /sessions/{token}/heartbeat`

Lo recomendable es hacerlo cada 30 segundos.

Eso le dice al backend que la sesión sigue viva.

## 6. Cerrar la sesión

Cuando el usuario cierre la pantalla o termine de usarla, la web debe intentar llamar:

- `POST /sessions/{token}/logout`

Como la API ahora está protegida por headers, no conviene usar `sendBeacon`, porque no puede mandar `X-API-Key` ni `X-User-Id`.

La forma práctica es:

- usar `fetch(..., { keepalive: true })`
- y dejar el `heartbeat timeout` como respaldo por si el navegador no alcanza a cerrar la sesión a tiempo

## Qué debe guardar la web localmente

La web debería guardar:

- API base
- API key
- User ID

No debería guardar:

- el token de sesión durante días
- un dataset “activo global”

El token de sesión es temporal.

## Qué debe pasar si algo falla

### Si falta API key o User ID

La web no debe intentar consultar.

Debe mostrar un mensaje claro tipo:

- “Captura la API key y el user id para continuar.”

### Si el token expiró

La web debe:

- avisar que la sesión ya no sirve
- crear una nueva sesión
- pedir que se vuelva a subir el CSV

### Si el CSV falla

La web debe mostrar el error y no habilitar el chat.

### Si el heartbeat falla una vez

Puede avisar, pero no tiene que romper todo de inmediato.

Si el backend responde que el token ya no existe o no pertenece al acceso actual, entonces sí debe cerrar la sesión local.

## Cómo pensar la seguridad

La lógica correcta es esta:

- `X-API-Key` dice si la app puede entrar al backend.
- `X-User-Id` dice qué usuario humano está operando.
- `token` dice qué sesión temporal del chat se está usando.

No conviene usar solo el `User ID` como si fuera seguridad.
Tampoco conviene usar el token de sesión como si reemplazara la API key.

Cada pieza cumple un papel distinto.

## Qué ya trae el backend para analytics

El backend ya puede registrar:

- usuario humano
- sesión temporal
- preguntas
- respuestas
- consumo de tokens
- costos
- tiempos

Entonces, si la web manda bien sus headers, luego se podrá revisar mejor quién usó qué, desde dónde y en qué sesión.
