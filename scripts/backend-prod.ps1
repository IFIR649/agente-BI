Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "Resolve-RepoPython.ps1")

$resolved = Get-RepoPython -ScriptsRoot $PSScriptRoot

Push-Location $resolved.RepoRoot
try {
    & $resolved.PythonPath -m uvicorn backend.app.main:app --no-access-log @args
    if ($LASTEXITCODE) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}
