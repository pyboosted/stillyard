$ErrorActionPreference = 'Stop'
$root = 'C:\Development\stillyard-mr0-handoff-20260909'
foreach ($file in Get-ChildItem (Join-Path $root 'scripts') -Filter '*.ps1') {
    $tokens = $null
    $parseErrors = $null
    $null = [System.Management.Automation.Language.Parser]::ParseFile($file.FullName, [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count -ne 0) { throw ($parseErrors | Out-String) }
}
$count = 0
foreach ($template in Get-ChildItem (Join-Path $root '.stillyard\jobs') -Filter '*.json.in') {
    $outputPath = Join-Path 'C:\Development\stillyard-mr0-evidence-20260909' ('validated-' + $template.BaseName)
    $generator = if ($template.Name.StartsWith('msrv-')) { 'New-StillyardMsrvJobSpec.ps1' } else { 'New-StillyardJobSpec.ps1' }
    & (Join-Path $root ('scripts\' + $generator)) -TemplatePath $template.FullName -OutputPath $outputPath -RepositoryRoot $root
    $document = Get-Content -Raw -LiteralPath $outputPath | ConvertFrom-Json
    if ($document.working_directory -ne $root -or $document.resources.cargo_slots -ne 1 -or $document.environment.set.CARGO_TARGET_DIR -ne (Join-Path $root 'target\scheduled')) { throw 'Unexpected generated Job coordinates or claims' }
    Write-Output ('Generated valid JobSpec: ' + $template.Name)
    $count++
}
if ($count -ne 9) { throw 'Expected all nine canonical Job templates' }
Write-Output 'All PowerShell files parse; all nine native templates expand, including both MSRV wrappers.'
