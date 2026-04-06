# agente-BI

## Backend

- Desarrollo: `.\scripts\backend-dev.ps1`
- Produccion Windows Server: `.\scripts\backend-prod.ps1 --host 0.0.0.0 --port 8000`
- Pruebas: `.\scripts\backend-test.ps1`

Los scripts requieren el entorno local del repo en `.\env\Scripts\python.exe`. En despliegue final, esa ruta corresponde a `C:\Aplicaciones\agente_web\env\Scripts\python.exe`.

`backend-dev.ps1` es solo para desarrollo local. Inicia Uvicorn con `--reload`, lo que no debe usarse como modo normal de servidor en Windows Server.

`backend-prod.ps1` inicia Uvicorn sin `--reload` y con `--no-access-log` para evitar flujo ruidoso por request en consola. Se conservan los logs de arranque y error.

Si la API parece "congelada" en una consola interactiva de Windows, revisa antes si la ventana entro en modo seleccion o QuickEdit. Ese estado puede pausar el proceso y simular un cuelgue hasta salir de la seleccion.
