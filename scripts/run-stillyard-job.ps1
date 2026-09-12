[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet('fmt', 'fmt-write', 'check', 'test', 'test-wsl-bootstrap', 'msrv-check', 'msrv-test', 'clippy', 'schema-update', 'build-release')]
    [string] $Job,

    # A separate source snapshot is useful when the Windows checkout has unrelated edits.
    [string] $RepositoryRoot,

    # Retain the exact submitted document for the status/evidence ledger.
    [string] $EvidenceDirectory,

    [string] $WslDistribution,
    [string] $WslUser
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($RepositoryRoot)) {
    $RepositoryRoot = Split-Path -Parent $PSScriptRoot
}
$repositoryRoot = (Resolve-Path -LiteralPath $RepositoryRoot).ProviderPath
if ($env:STILLYARD_JOB_ID -or $env:STILLYARD_ATTEMPT) {
    throw 'This launcher submits unmanaged root Jobs; a managed consumer must use the authenticated child API.'
}
if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    throw 'LOCALAPPDATA is required to locate the system Stillyard installation'
}
$stillyardExecutable = Join-Path $env:LOCALAPPDATA 'stillyard\Stillyard\bin\stillyard.exe'
$isMsrvJob = $Job -in @('msrv-check', 'msrv-test')
if ($Job -eq 'test-wsl-bootstrap' -and
    ([string]::IsNullOrWhiteSpace($WslDistribution) -or [string]::IsNullOrWhiteSpace($WslUser))) {
    throw 'test-wsl-bootstrap requires explicit -WslDistribution and -WslUser'
}
$jobSpec = Join-Path $PSScriptRoot "..\.stillyard\jobs\$Job.json.in"
$generatedJobSpec = $null
$receiptPath = $null
$acceptanceUnknown = $false

if (-not (Test-Path -LiteralPath $stillyardExecutable -PathType Leaf)) {
    throw "The canonical system Stillyard executable is missing: $stillyardExecutable"
}
if (-not (Test-Path -LiteralPath $jobSpec -PathType Leaf)) {
    throw "Unknown or missing Stillyard JobSpec: $jobSpec"
}

try {
    $generatedJobSpec = New-TemporaryFile
    & (Join-Path $PSScriptRoot 'New-StillyardJobSpec.ps1') `
        -TemplatePath $jobSpec `
        -OutputPath $generatedJobSpec.FullName `
        -RepositoryRoot $repositoryRoot `
        -RequireMsrv:$isMsrvJob -WslDistribution $WslDistribution -WslUser $WslUser
    $jobSpec = $generatedJobSpec.FullName
    $receiptPath = Join-Path $env:TEMP "stillyard-$Job-$([Guid]::NewGuid().ToString('N')).receipt.json"
    if (-not [string]::IsNullOrWhiteSpace($EvidenceDirectory)) {
        $evidenceRoot = (New-Item -ItemType Directory -Path $EvidenceDirectory -Force).FullName
        $runName = "$Job-$([Guid]::NewGuid().ToString('N'))"
        $retainedSpec = Join-Path $evidenceRoot "$runName.spec.json"
        Copy-Item -LiteralPath $jobSpec -Destination $retainedSpec
        $receiptPath = Join-Path $evidenceRoot "$runName.receipt.json"
        Write-Host "Stillyard Job $Job; submitted spec: $retainedSpec"
    } else {
        Write-Host "Stillyard Job $Job; durable receipt: $receiptPath"
    }

    & $stillyardExecutable submit --spec $jobSpec --result-file $receiptPath --wait --passthrough --deadline-seconds 86400
    $jobExitCode = $LASTEXITCODE
    # A result-file publication can fail after acceptance (observed native OS
    # error 5). Recover the same operation; never submit a new logical Job.
    for ($recoveryAttempt = 0; $recoveryAttempt -lt 3 -and $jobExitCode -ne 0; $recoveryAttempt++) {
        if (-not (Test-Path -LiteralPath $receiptPath -PathType Leaf)) { break }
        $intent = Get-Content -LiteralPath $receiptPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $acceptanceUnknown = $null -eq $intent.receipt
        if (-not $acceptanceUnknown -or -not $intent.idempotency_key -or -not $intent.endpoint) { break }
        Write-Host "Stillyard Job $Job; recovering original operation $($intent.idempotency_key)"
        # Publication to the original receipt can remain denied even though
        # acceptance succeeded. Preserve that intent, then recover the SAME
        # operation into a fresh durable receipt on each attempt.
        $receiptDirectory = Split-Path -Parent $receiptPath
        $receiptPath = Join-Path $receiptDirectory "$Job-recovered-$([Guid]::NewGuid().ToString('N')).receipt.json"
        Write-Host "Stillyard Job $Job; recovery receipt: $receiptPath"
        Start-Sleep -Milliseconds 200
        & $stillyardExecutable --endpoint $intent.endpoint ensure --spec $jobSpec `
            --idempotency-key $intent.idempotency_key --result-file $receiptPath `
            --wait --passthrough --deadline-seconds 86400
        $jobExitCode = $LASTEXITCODE
    }
    if (Test-Path -LiteralPath $receiptPath -PathType Leaf) {
        $intent = Get-Content -LiteralPath $receiptPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $acceptanceUnknown = $null -eq $intent.receipt
    }
} finally {
    if ($null -ne $generatedJobSpec -and -not $acceptanceUnknown) {
        Remove-Item -LiteralPath $generatedJobSpec.FullName -Force -ErrorAction SilentlyContinue
    } elseif ($null -ne $generatedJobSpec) {
        Write-Host "Acceptance remains unknown; retained JobSpec: $($generatedJobSpec.FullName); receipt: $receiptPath"
    }
}

exit $jobExitCode
