[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $TemplatePath,

    [Parameter(Mandatory = $true)]
    [string] $OutputPath,

    [Parameter(Mandatory = $true)]
    [string] $RepositoryRoot
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Resolve-Executable {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,

        [string[]] $Fallbacks = @()
    )

    $command = Get-Command $Name -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $command) {
        return $command.Source
    }
    foreach ($fallback in $Fallbacks) {
        if (-not [string]::IsNullOrWhiteSpace($fallback) -and
            (Test-Path -LiteralPath $fallback -PathType Leaf)) {
            return (Resolve-Path -LiteralPath $fallback).ProviderPath
        }
    }
    throw "Required executable was not found: $Name"
}

function Get-VisualStudioEnvironment {
    $programFilesX86 = [Environment]::GetFolderPath('ProgramFilesX86')
    $standardVsWhere = Join-Path $programFilesX86 'Microsoft Visual Studio\Installer\vswhere.exe'
    $vsWhere = Resolve-Executable -Name 'vswhere.exe' -Fallbacks @($standardVsWhere)
    $installationPath = (& $vsWhere @(
            '-latest',
            '-products', '*',
            '-requires', 'Microsoft.VisualStudio.Component.VC.Tools.x86.x64',
            '-property', 'installationPath'
        ) | Select-Object -First 1)
    if ([string]::IsNullOrWhiteSpace($installationPath)) {
        throw 'No Visual Studio installation with the x64 C++ toolchain was found'
    }

    $vsDevCmd = Join-Path $installationPath.Trim() 'Common7\Tools\VsDevCmd.bat'
    if (-not (Test-Path -LiteralPath $vsDevCmd -PathType Leaf)) {
        throw "Visual Studio environment launcher is missing: $vsDevCmd"
    }

    $command = @(
        'set "PATH=%SystemRoot%\System32;%SystemRoot%;%SystemRoot%\System32\Wbem"',
        'set "INCLUDE="',
        'set "LIB="',
        'set "LIBPATH="',
        ('call "{0}" -no_logo -arch=x64 -host_arch=x64 >nul' -f $vsDevCmd),
        'set'
    ) -join ' && '
    $lines = & $env:ComSpec /d /s /c $command
    if ($LASTEXITCODE -ne 0) {
        throw "VsDevCmd failed with exit code $LASTEXITCODE"
    }

    $environment = [Collections.Generic.Dictionary[string, string]]::new(
        [StringComparer]::OrdinalIgnoreCase
    )
    foreach ($line in $lines) {
        if ($line -match '^(?<name>[^=]+)=(?<value>.*)$') {
            $environment[$Matches.name] = $Matches.value
        }
    }
    foreach ($required in @('PATH', 'INCLUDE', 'LIB')) {
        if (-not $environment.ContainsKey($required) -or
            [string]::IsNullOrWhiteSpace($environment[$required])) {
            throw "VsDevCmd did not provide $required"
        }
    }
    return $environment
}

function Add-PathDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [Collections.Generic.List[string]] $Directories,

        [string] $Executable
    )

    if ([string]::IsNullOrWhiteSpace($Executable)) {
        return
    }
    $directory = Split-Path -Parent $Executable
    if (-not $Directories.Contains($directory)) {
        $Directories.Add($directory)
    }
}

$templatePathResolved = (Resolve-Path -LiteralPath $TemplatePath).ProviderPath
$repositoryRootResolved = (Resolve-Path -LiteralPath $RepositoryRoot).ProviderPath
if (-not (Test-Path -LiteralPath (Join-Path $repositoryRootResolved 'Cargo.toml') -PathType Leaf)) {
    throw "Repository root has no Cargo.toml: $repositoryRootResolved"
}
if ([string]::IsNullOrWhiteSpace($env:USERPROFILE)) {
    throw 'USERPROFILE is required to generate the MSRV JobSpec'
}

$cargoHome = if ([string]::IsNullOrWhiteSpace($env:CARGO_HOME)) {
    Join-Path $env:USERPROFILE '.cargo'
} else {
    $env:CARGO_HOME
}
$rustupHome = if ([string]::IsNullOrWhiteSpace($env:RUSTUP_HOME)) {
    Join-Path $env:USERPROFILE '.rustup'
} else {
    $env:RUSTUP_HOME
}
$cargoExe = Resolve-Executable -Name 'cargo.exe' -Fallbacks @(
    (Join-Path $cargoHome 'bin\cargo.exe')
)
$rustupExe = Resolve-Executable -Name 'rustup.exe' -Fallbacks @(
    (Join-Path $cargoHome 'bin\rustup.exe')
)
$installedToolchains = & $rustupExe toolchain list
if ($LASTEXITCODE -ne 0 -or -not ($installedToolchains -match '^1\.85\.0-x86_64-pc-windows-msvc')) {
    throw 'Required Rust toolchain is not installed: 1.85.0-x86_64-pc-windows-msvc'
}

$vsEnvironment = Get-VisualStudioEnvironment
$pathDirectories = [Collections.Generic.List[string]]::new()
foreach ($directory in ($vsEnvironment['PATH'] -split ';')) {
    if (-not [string]::IsNullOrWhiteSpace($directory) -and
        -not $pathDirectories.Contains($directory)) {
        $pathDirectories.Add($directory)
    }
}
Add-PathDirectory -Directories $pathDirectories -Executable $cargoExe
Add-PathDirectory -Directories $pathDirectories -Executable $rustupExe
foreach ($optional in @('sccache.exe', 'git.exe', 'cmake.exe')) {
    $command = Get-Command $optional -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -ne $command) {
        Add-PathDirectory -Directories $pathDirectories -Executable $command.Source
    }
}

$sccacheDirectory = if ([string]::IsNullOrWhiteSpace($env:SCCACHE_DIR)) {
    Join-Path $repositoryRootResolved 'target\sccache'
} else {
    $env:SCCACHE_DIR
}
$bindings = [ordered]@{
    '${CARGO_EXE}' = $cargoExe
    '${REPOSITORY_ROOT}' = $repositoryRootResolved
    '${CARGO_HOME}' = $cargoHome
    '${CARGO_TARGET_DIR}' = (Join-Path $repositoryRootResolved 'target\scheduled')
    '${INCLUDE}' = $vsEnvironment['INCLUDE']
    '${LIB}' = $vsEnvironment['LIB']
    '${PATH}' = ($pathDirectories -join ';')
    '${RUSTUP_HOME}' = $rustupHome
    '${SCCACHE_DIR}' = $sccacheDirectory
    '${USERPROFILE}' = $env:USERPROFILE
    '${SYSTEMDRIVE}' = $env:SYSTEMDRIVE
}

$jobSpec = Get-Content -LiteralPath $templatePathResolved -Raw
foreach ($binding in $bindings.GetEnumerator()) {
    $jsonString = ConvertTo-Json -InputObject ([string]$binding.Value) -Compress
    $escapedValue = $jsonString.Substring(1, $jsonString.Length - 2)
    $jobSpec = $jobSpec.Replace($binding.Key, $escapedValue)
}
if ($jobSpec -match '\$\{[A-Z_]+\}') {
    throw "Generated JobSpec contains an unresolved placeholder: $($Matches[0])"
}
$null = $jobSpec | ConvertFrom-Json

$outputDirectory = Split-Path -Parent $OutputPath
if (-not [string]::IsNullOrWhiteSpace($outputDirectory) -and
    -not (Test-Path -LiteralPath $outputDirectory -PathType Container)) {
    throw "Output directory does not exist: $outputDirectory"
}
$utf8WithoutBom = [Text.UTF8Encoding]::new($false)
[IO.File]::WriteAllText($OutputPath, $jobSpec.TrimEnd() + [Environment]::NewLine, $utf8WithoutBom)
