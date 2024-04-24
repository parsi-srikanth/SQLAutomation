@echo off
setlocal

rem Set the path to your virtual environment's activate script
set VENV_PATH=C:\Users\C00541311\Desktop\SQLAutomation\sqlAuto-venv\Scripts\activate.bat

rem Check if the virtual environment activate script exists
if not exist "%VENV_PATH%" (
    echo Virtual environment not found. Please provide the correct path to your virtual environment's activate script.
    pause
    rem exit /b 1
)

rem Activate the virtual environment
call "%VENV_PATH%"

rem Check if a SQL file is dropped on the script
if "%~1"=="" (
    echo No SQL file specified. Drag and drop a .sql file onto this script.
    pause
    rem exit /b 1
)

rem Check if Python is installed
python --version >nul 2>nul
if errorlevel 1 (
    echo Python is not installed or not found in PATH.
    pause
    rem exit /b 1
)

rem Call the Python script with the full path of the dropped SQL file
cd ..
cd ..
python client.py "%~f1"
pause
endlocal
