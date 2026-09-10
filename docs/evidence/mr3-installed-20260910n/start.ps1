param([string] $DaemonCommand, [string] $DaemonDirectory)
$ErrorActionPreference = 'Stop'
Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine=$DaemonCommand; CurrentDirectory=$DaemonDirectory} | ConvertTo-Json
