@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo Run setup_windows.cmd first.
  pause
  exit /b 1
)
echo Starting Crypto Momentum V1.4.12 with crash watchdog...
echo Press Ctrl+C to stop cleanly. Exchange-side stop orders remain on Coinbase.
echo Unexpected Python-process exits are restarted automatically, up to the restart-storm limit.
echo.
.venv\Scripts\python.exe -m crypto_momentum.watchdog
set EXITCODE=%ERRORLEVEL%
echo.
echo Watchdog exited with code %EXITCODE%.
pause
exit /b %EXITCODE%
