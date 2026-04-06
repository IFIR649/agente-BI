Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-RepoPython {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ScriptsRoot
    )

    $repoRoot = (Resolve-Path (Join-Path $ScriptsRoot "..")).ProviderPath
    $pythonPath = Join-Path $repoRoot "env\Scripts\python.exe"

    if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
        $message = @(
            "No se encontro el interprete esperado del proyecto."
            "Ruta esperada: $pythonPath"
            "Crea .\\env con Python 3.14 y las dependencias del proyecto antes de ejecutar este script."
        ) -join [Environment]::NewLine
        throw $message
    }

    return [pscustomobject]@{
        RepoRoot   = $repoRoot
        PythonPath = $pythonPath
    }
}
