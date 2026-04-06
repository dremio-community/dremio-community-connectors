@echo off
REM Windows launcher — double-click this file in File Explorer to open the Rebuild UI.
REM Requires: Python 3 (from python.org; make sure "Add to PATH" is checked during install)

cd /d "%~dp0"

REM ── Locate Python ──────────────────────────────────────────────────────────
set PY=
where python >nul 2>&1 && set PY=python
if "%PY%"=="" (
  where python3 >nul 2>&1 && set PY=python3
)
if "%PY%"=="" (
  if exist "%LOCALAPPDATA%\Programs\Python\Python313\python.exe" (
    set PY=%LOCALAPPDATA%\Programs\Python\Python313\python.exe
  ) else if exist "%LOCALAPPDATA%\Programs\Python\Python312\python.exe" (
    set PY=%LOCALAPPDATA%\Programs\Python\Python312\python.exe
  ) else if exist "%LOCALAPPDATA%\Programs\Python\Python311\python.exe" (
    set PY=%LOCALAPPDATA%\Programs\Python\Python311\python.exe
  ) else if exist "%LOCALAPPDATA%\Programs\Python\Python310\python.exe" (
    set PY=%LOCALAPPDATA%\Programs\Python\Python310\python.exe
  )
)

if "%PY%"=="" (
  echo.
  echo  ERROR: Python 3 not found.
  echo.
  echo  Please install Python 3 from https://www.python.org/downloads/
  echo  Make sure to check "Add Python to PATH" during installation.
  echo.
  pause
  exit /b 1
)

echo.
echo  Starting Rebuild UI...
echo  The browser will open automatically at http://localhost:8765
echo  Press Ctrl+C in this window to stop the server.
echo.

"%PY%" rebuild-ui.py
if errorlevel 1 (
  echo.
  echo  Something went wrong. See error above.
  pause
)
