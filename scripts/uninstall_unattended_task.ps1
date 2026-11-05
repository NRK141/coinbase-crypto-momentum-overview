$ErrorActionPreference = 'Stop'
$TaskName = 'CoinbaseCryptoMomentum'
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Disable-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Out-Null
    try { Stop-ScheduledTask -TaskName $TaskName -ErrorAction Stop } catch { }
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed Scheduled Task: $TaskName"
} else {
    Write-Host "Scheduled Task not installed: $TaskName"
}
