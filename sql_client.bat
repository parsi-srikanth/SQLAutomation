@echo off
setlocal

rem Set the path to your virtual environment's activate script
set PROJ_DIR=C:\Users\C00541311\Desktop\SQLAutomation
set VIRTUAL_ENV=C:\Users\C00541311\Desktop\SQLAutomation\sqlAuto-venv\Scripts\python.exe

rem Check if Python is installed
python --version >nul 2>nul
if errorlevel 1 (
    echo Python is not installed or not found in PATH.
    pause
    rem exit /b 1
)

rem Check if the virtual environment python executable exists
if not exist "%VIRTUAL_ENV%" (
    echo Virtual environment not found. Please provide the correct path to your virtual environment's python.exe file.
    pause
    rem exit /b 1
)

rem Check if a SQL file is dropped on the script
if "%~1"=="" (
    echo No SQL file specified. Drag and drop a .sql file onto this script.
    pause
    rem exit /b 1
)

cd "%PROJ_DIR%"
"%VIRTUAL_ENV%" "%PROJ_DIR%/client.py" "%~f1"
pause

endlocal