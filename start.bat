@echo off
setlocal
cd /d "%~dp0"

set "HOST=127.0.0.1"
set "PORT=8000"
set "VENV_PY=.venv\Scripts\python.exe"

where python >nul 2>&1
if errorlevel 1 (
    echo [error] python is not on PATH. Install Python 3.11+ and re-run.
    pause
    exit /b 1
)

if not exist "%VENV_PY%" (
    echo [setup] creating virtual environment in .venv ...
    python -m venv .venv || goto :fail
)

echo [setup] installing / verifying dependencies ...
"%VENV_PY%" -m pip install --quiet --disable-pip-version-check -r requirements.txt || goto :fail

echo.
echo ==============================================
echo  AWS Bandwidth Monitor
echo  URL: http://%HOST%:%PORT%
echo  Ctrl+C in this window stops the server.
echo ==============================================
echo.

REM Open the default browser ~2s after launch (non-blocking, hidden window).
start "" /b powershell -NoProfile -WindowStyle Hidden -Command ^
    "Start-Sleep -Seconds 2; Start-Process 'http://%HOST%:%PORT%/'"

"%VENV_PY%" -m uvicorn backend.main:app --host %HOST% --port %PORT%
goto :eof

:fail
echo.
echo [error] setup failed. See messages above.
pause
exit /b 1
