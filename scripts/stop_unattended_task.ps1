$ErrorActionPreference = 'Stop'
$TaskName = 'CoinbaseCryptoMomentum'
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
# Disable first so Task Scheduler cannot relaunch the watchdog while maintenance is underway.
Disable-ScheduledTask -TaskName $TaskName | Out-Null
try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction Stop } catch { }
Start-Sleep -Seconds 2
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
Write-Host 'Unattended task disabled/stopped. Exchange-side stop orders are not cancelled by this script.'
