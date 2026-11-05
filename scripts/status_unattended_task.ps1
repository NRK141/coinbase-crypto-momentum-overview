$ErrorActionPreference = 'Stop'
$TaskName = 'CoinbaseCryptoMomentum'
$Root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$WatchdogHeartbeat = Join-Path $Root 'data\watchdog_heartbeat.json'
$RuntimeHeartbeat = Join-Path $Root 'data\runtime_heartbeat.json'
$ShadowLog = Join-Path $Root 'logs\shadow_15m.log'
$UniverseAuditLog = Join-Path $Root 'logs\universe_audit.log'
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$info = Get-ScheduledTaskInfo -TaskName $TaskName
$task | Select-Object TaskName, State
$info | Select-Object LastRunTime, LastTaskResult, NextRunTime, NumberOfMissedRuns

if (Test-Path $WatchdogHeartbeat) {
    Write-Host ''
    Write-Host 'Watchdog heartbeat:'
    Get-Content $WatchdogHeartbeat
} else {
    Write-Host ''
    Write-Host 'No watchdog heartbeat file exists yet.'
}

if (Test-Path $RuntimeHeartbeat) {
    Write-Host ''
    Write-Host 'Trading engine / shadow heartbeat:'
    Get-Content $RuntimeHeartbeat
} else {
    Write-Host ''
    Write-Host 'No runtime heartbeat file exists yet.'
}

if (Test-Path $ShadowLog) {
    $item = Get-Item $ShadowLog
    Write-Host ''
    Write-Host ("Shadow 15m log: {0} | last write: {1}" -f $ShadowLog, $item.LastWriteTime)
} else {
    Write-Host ''
    Write-Host 'Shadow 15m log has not been created yet; it appears after the first successful V1.4.12 signal cycle.'
}


if (Test-Path $UniverseAuditLog) {
    $item = Get-Item $UniverseAuditLog
    Write-Host ''
    Write-Host ("Universe audit log: {0} | last write: {1}" -f $UniverseAuditLog, $item.LastWriteTime)
} else {
    Write-Host ''
    Write-Host 'Universe audit log has not been created yet; it appears after the first V1.4.12 trading-universe refresh.'
}


Write-Host ''
Write-Host 'Local remote-health dashboard:'
try {
    $dash = Invoke-RestMethod -Uri 'http://127.0.0.1:8765/api/status' -TimeoutSec 2
    $dash | Select-Object health, runtime_age_text, watchdog_state, last_scan_age_text, position
} catch {
    Write-Host 'Dashboard not responding on http://127.0.0.1:8765'
}
