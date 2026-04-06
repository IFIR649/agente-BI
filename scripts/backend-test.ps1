Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "Resolve-RepoPython.ps1")

$resolved = Get-RepoPython -ScriptsRoot $PSScriptRoot

Push-Location $resolved.RepoRoot
try {
    & $resolved.PythonPath -m pytest backend\tests -q @args
    if ($LASTEXITCODE) {
        exit $LASTEXITCODE
    }
} finally {
    Pop-Location
}
