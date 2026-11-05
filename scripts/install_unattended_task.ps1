$ErrorActionPreference = 'Stop'
$TaskName = 'CoinbaseCryptoMomentum'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$Python = Join-Path $Root '.venv\Scripts\python.exe'
$EnvFile = Join-Path $Root '.env'

if (-not (Test-Path $Python)) { throw "Missing $Python. Run setup_windows.cmd first." }
if (-not (Test-Path $EnvFile)) { throw "Missing $EnvFile. Configure the bot before installing unattended mode." }

$UserId = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$Action = New-ScheduledTaskAction -Execute $Python -Argument '-m crypto_momentum.watchdog' -WorkingDirectory $Root
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserId
$Principal = New-ScheduledTaskPrincipal -UserId $UserId -LogonType S4U -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries

$Task = New-ScheduledTask -Action $Action -Trigger $Trigger -Principal $Principal -Settings $Settings `
    -Description 'Supervises Coinbase Crypto Momentum live engine plus observation-only 15m shadow engine independently of the terminal window. Starts at user logon and Task Scheduler restarts the watchdog if it exits.'
Register-ScheduledTask -TaskName $TaskName -InputObject $Task -Force | Out-Null
Enable-ScheduledTask -TaskName $TaskName | Out-Null
Start-ScheduledTask -TaskName $TaskName

Write-Host ''
Write-Host "Installed and started Scheduled Task: $TaskName"
Write-Host "Project: $Root"
Write-Host 'Run status_unattended_task.cmd to verify State=Running.'
Write-Host 'Do NOT also run run_bot.cmd while the scheduled task is active.'
Write-Host 'Use stop_unattended_task.cmd before maintenance or manual interactive runs.'
