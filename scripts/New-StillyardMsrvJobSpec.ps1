[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $TemplatePath,
    [Parameter(Mandatory = $true)] [string] $OutputPath,
    [Parameter(Mandatory = $true)] [string] $RepositoryRoot
)

$ErrorActionPreference = 'Stop'
& (Join-Path $PSScriptRoot 'New-StillyardJobSpec.ps1') `
    -TemplatePath $TemplatePath -OutputPath $OutputPath `
    -RepositoryRoot $RepositoryRoot -RequireMsrv
