[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('fmt', 'fmt-write', 'check', 'test', 'msrv-check', 'msrv-test', 'clippy', 'schema-update', 'build-release')]
    [string] $Job
)

$ErrorActionPreference = 'Stop'

$repositoryRoot = Split-Path -Parent $PSScriptRoot
if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    throw 'LOCALAPPDATA is required to locate the system Stillyard installation'
}
$stillyardExecutable = Join-Path $env:LOCALAPPDATA 'stillyard\Stillyard\bin\stillyard.exe'
$isMsrvJob = $Job -in @('msrv-check', 'msrv-test')
$jobSpecName = if ($isMsrvJob) { "$Job.json.in" } else { "$Job.json" }
$jobSpec = Join-Path $repositoryRoot ".stillyard\jobs\$jobSpecName"
$generatedJobSpec = $null

if (-not (Test-Path -LiteralPath $stillyardExecutable -PathType Leaf)) {
    throw "The canonical system Stillyard executable is missing: $stillyardExecutable"
}
if (-not (Test-Path -LiteralPath $jobSpec -PathType Leaf)) {
    throw "Unknown or missing Stillyard JobSpec: $jobSpec"
}

try {
    if ($isMsrvJob) {
        $generatedJobSpec = New-TemporaryFile
        & (Join-Path $PSScriptRoot 'New-StillyardMsrvJobSpec.ps1') `
            -TemplatePath $jobSpec `
            -OutputPath $generatedJobSpec.FullName `
            -RepositoryRoot $repositoryRoot
        $jobSpec = $generatedJobSpec.FullName
    }

    & $stillyardExecutable submit --spec $jobSpec --wait --passthrough --deadline-seconds 86400
    $jobExitCode = $LASTEXITCODE
} finally {
    if ($null -ne $generatedJobSpec) {
        Remove-Item -LiteralPath $generatedJobSpec.FullName -Force -ErrorAction SilentlyContinue
    }
}

exit $jobExitCode
