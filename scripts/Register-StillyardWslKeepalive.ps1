param(
    [Parameter(Mandatory=$true)][string]$Distribution,
    [Parameter(Mandatory=$true)][string]$LinuxUser,
    [Parameter(Mandatory=$true)][string]$LinuxHelper,
    [Parameter(Mandatory=$true)][string]$LinuxStore,
    [Parameter(Mandatory=$true)][string]$EvidenceDirectory,
    [switch]$Apply
)
$ErrorActionPreference = 'Stop'
# This task owns only the external WSL lifetime session. It never clears a
# containment or touches scheduler history. No password is stored with S4U.
foreach ($value in @($Distribution,$LinuxUser,$LinuxHelper,$LinuxStore)) {
    if ($value -notmatch '^[A-Za-z0-9/_.:@ -]+$') {
        throw 'This initial installer requires ordinary ASCII runtime coordinates.'
    }
}
$taskName = 'Stillyard WSL ' + $Distribution
$principalName = [Security.Principal.WindowsIdentity]::GetCurrent().Name
$principalSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$arguments = '-d "' + $Distribution + '" -u "' + $LinuxUser + '" --exec /usr/bin/python3 "' + $LinuxHelper + '" keepalive --root "' + $LinuxStore + '"'
$action = New-ScheduledTaskAction -Execute ($env:SystemRoot + '\System32\wsl.exe') -Argument $arguments
$principal = New-ScheduledTaskPrincipal -UserId $principalName -LogonType S4U -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
$triggers = @((New-ScheduledTaskTrigger -AtStartup), (New-ScheduledTaskTrigger -AtLogOn -User $principalName))
$task = New-ScheduledTask -Action $action -Principal $principal -Settings $settings -Trigger $triggers -Description 'Keeps the explicitly paired WSL runtime alive; no scheduler or cleanup authority.'
New-Item -ItemType Directory -Path $EvidenceDirectory -ErrorAction Stop | Out-Null
$plan = [ordered]@{TaskName=$taskName; Principal=$principalName; Sid=$principalSid; LogonType='S4U'; Executable=$action.Execute; Arguments=$arguments; Apply=[bool]$Apply}
$plan | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8 -LiteralPath (Join-Path $EvidenceDirectory 'plan.json')
if ($Apply) {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        throw 'Initial keepalive registration refuses an existing task.'
    }
    Register-ScheduledTask -TaskName $taskName -InputObject $task | Out-Null
    Export-ScheduledTask -TaskName $taskName | Set-Content -Encoding UTF8 -LiteralPath (Join-Path $EvidenceDirectory 'registered-task.xml')
    Start-ScheduledTask -TaskName $taskName
    Get-ScheduledTaskInfo -TaskName $taskName | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8 -LiteralPath (Join-Path $EvidenceDirectory 'started.json')
}
Write-Output $EvidenceDirectory
