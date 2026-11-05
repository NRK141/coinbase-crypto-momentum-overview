@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 (
  echo Python launcher ^(py^) was not found. Install Python 3.11+ and select "Add Python to PATH".
  pause
  exit /b 1
)

if not exist .venv (
  echo Creating virtual environment...
  py -3 -m venv .venv
  if errorlevel 1 goto :fail
)

call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
if errorlevel 1 goto :fail
pip install -r requirements.txt
if errorlevel 1 goto :fail
pip install -e .
if errorlevel 1 goto :fail

if not exist .env (
  copy .env.example .env >nul
  echo.
  echo Created .env from .env.example.
  echo Edit .env and paste your Coinbase API key and private key before running diagnostics.
)

echo.
echo Setup complete.
echo Next: edit .env, then run check_connection.cmd
pause
exit /b 0

:fail
echo.
echo Setup failed. Review the message above.
pause
exit /b 1
