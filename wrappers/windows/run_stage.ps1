param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $root

$venvPython = Join-Path $root ".venv\\Scripts\\python.exe"
if (Test-Path $venvPython) {
    & $venvPython .\run_pipeline.py @Args
    exit $LASTEXITCODE
}

python .\run_pipeline.py @Args
