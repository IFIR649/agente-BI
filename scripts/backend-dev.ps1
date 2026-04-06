Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "Resolve-RepoPython.ps1")

$resolved = Get-RepoPython -ScriptsRoot $PSScriptRoot

Push-Location $resolved.RepoRoot
try {
    Write-Warning "backend-dev.ps1 es solo para desarrollo local. Usa --reload y no debe emplearse en Windows Server."
    Write-Warning "Para servidor usa .\\scripts\\backend-prod.ps1 --host 0.0.0.0 --port 8000"
    & $resolved.PythonPath -m uvicorn backend.app.main:app --reload @args
    if ($LASTEXITCODE) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}
