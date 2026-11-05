$ErrorActionPreference = 'Stop'
$TaskName = 'CoinbaseCryptoMomentum'
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
Enable-ScheduledTask -TaskName $TaskName | Out-Null
Start-ScheduledTask -TaskName $TaskName
Start-Sleep -Seconds 2
Get-ScheduledTask -TaskName $TaskName | Select-Object TaskName, State
Write-Host 'Unattended task enabled and started. Do not also run run_bot.cmd.'
