@echo off
REM ─────────────────────────────────────────────────────────────────────────────
REM Dremio Excel Importer — Windows launcher
REM Double-click this file to start the web UI.
REM ─────────────────────────────────────────────────────────────────────────────

cd /d "%~dp0"

set PORT=8766
set JAR=jars\dremio-excel-importer.jar
set UI=importer-ui.py

REM ── Check Java ──────────────────────────────────────────────────────────────
where java >nul 2>&1
if errorlevel 1 (
    echo.
    echo ══════════════════════════════════════════════════════
    echo   Java not found.
    echo.
    echo   Please install Java 11 or later, then run this
    echo   file again.
    echo.
    echo   Download from: https://adoptium.net
    echo ══════════════════════════════════════════════════════
    echo.
    start https://adoptium.net
    pause
    exit /b 1
)

for /f "tokens=*" %%v in ('java -version 2^>^&1 ^| findstr /i "version"') do (
    echo Java : %%v
)

REM ── Check Python ────────────────────────────────────────────────────────────
where python >nul 2>&1
if errorlevel 1 (
    where python3 >nul 2>&1
    if errorlevel 1 (
        echo.
        echo Python not found. Install from https://python.org
        start https://python.org/downloads
        pause
        exit /b 1
    )
    set PYTHON=python3
) else (
    set PYTHON=python
)

for /f "tokens=*" %%v in ('%PYTHON% --version 2^>^&1') do (
    echo Python: %%v
)

REM ── Check JAR ───────────────────────────────────────────────────────────────
if not exist "%JAR%" (
    echo.
    echo JAR not found at %JAR%
    echo Run install.sh to build it first.
    pause
    exit /b 1
)

REM ── Kill any existing instance ───────────────────────────────────────────────
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%PORT%"') do (
    taskkill /f /pid %%a >nul 2>&1
)

REM ── Launch ───────────────────────────────────────────────────────────────────
echo.
echo Starting Dremio Excel Importer UI on port %PORT%...
start "" /b %PYTHON% %UI% --port %PORT%

REM Wait a moment then open browser
timeout /t 2 /nobreak >nul
start http://localhost:%PORT%

echo.
echo Dremio Excel Importer is running at http://localhost:%PORT%
echo Close this window to stop the server.
echo.
pause
