# agente-BI

## Backend

- Ubuntu / Linux desarrollo: `bash scripts/backend-dev.sh --host 0.0.0.0 --port 8000`
- Ubuntu / Linux produccion: `bash scripts/backend-prod.sh --host 0.0.0.0 --port 8000`
- Ubuntu / Linux pruebas: `bash scripts/backend-test.sh`
- Windows desarrollo: `.\scripts\backend-dev.ps1`
- Windows produccion: `.\scripts\backend-prod.ps1 --host 0.0.0.0 --port 8000`
- Windows pruebas: `.\scripts\backend-test.ps1`

### Preparacion en Ubuntu

```bash
python3 -m venv env
source env/bin/activate
pip install -r backend/requirements.txt
cp .env.example .env
```

Edita `.env` y define como minimo:

```bash
AGENT_API_KEY=<secreto>
AGENT_GEMINI_API_KEY=<gemini_key>
```

Los scripts Linux detectan Python en este orden: entorno virtual activo, `./env/bin/python`, `./.venv/bin/python`, `./venv/bin/python`, `python3`.

### Arranque por SSH en Ubuntu

Foreground:

```bash
bash scripts/backend-prod.sh --host 0.0.0.0 --port 8000
```

Background:

```bash
nohup bash scripts/backend-prod.sh --host 0.0.0.0 --port 8000 > backend.log 2>&1 &
```

`backend-dev.sh` es solo para desarrollo local. Inicia Uvicorn con `--reload`, lo que no debe usarse como modo normal de servidor.

`backend-prod.sh` inicia Uvicorn sin `--reload` y con `--no-access-log` para evitar flujo ruidoso por request en consola. Se conservan los logs de arranque y error.

### Windows

Los scripts PowerShell siguen requiriendo el entorno local del repo en `.\env\Scripts\python.exe`. En despliegue final, esa ruta corresponde a `C:\Aplicaciones\agente_web\env\Scripts\python.exe`.

`backend-dev.ps1` es solo para desarrollo local. Inicia Uvicorn con `--reload`, lo que no debe usarse como modo normal de servidor en Windows Server.

`backend-prod.ps1` inicia Uvicorn sin `--reload` y con `--no-access-log` para evitar flujo ruidoso por request en consola. Se conservan los logs de arranque y error.

Si la API parece "congelada" en una consola interactiva de Windows, revisa antes si la ventana entro en modo seleccion o QuickEdit. Ese estado puede pausar el proceso y simular un cuelgue hasta salir de la seleccion.
