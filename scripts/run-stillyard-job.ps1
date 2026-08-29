[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('fmt', 'fmt-write', 'check', 'test', 'clippy', 'schema-update', 'build-release')]
    [string] $Job
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
$stillyardExecutable = 'C:\Users\User\AppData\Local\stillyard\Stillyard\bin\stillyard.exe'
$jobSpec = Join-Path $repositoryRoot ".stillyard\jobs\$Job.json"

if (-not (Test-Path -LiteralPath $stillyardExecutable -PathType Leaf)) {
    throw "The canonical system Stillyard executable is missing: $stillyardExecutable"
}
if (-not (Test-Path -LiteralPath $jobSpec -PathType Leaf)) {
    throw "Unknown or missing Stillyard JobSpec: $jobSpec"
}

& $stillyardExecutable submit --spec $jobSpec --wait --passthrough --deadline-seconds 86400
exit $LASTEXITCODE
