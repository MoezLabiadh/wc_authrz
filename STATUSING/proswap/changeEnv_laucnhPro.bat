@echo off

rem This bat file changes the python environment of ArcGIS Pro, then launches the app.
rem proswap.bat is located in E:\sw_nt\ArcGIS\Pro\bin\Python\Scripts
rem The new python environment is P:\corp\python_ast

rem Call proswap.bat with the new python environment as an argument
echo Changing Python environment to P:\corp\python_ast.
cd /d E:\sw_nt\ArcGIS\Pro\bin\Python\Scripts

rem Run proswap.bat and capture its output
for /f "delims=" %%i in ('call proswap.bat P:\corp\python_ast 2^>^&1') do (
    set "output=%%i"
)

rem Check the error level of the last command
set "errorLevel=%errorlevel%"
echo Error level: %errorLevel%

if %errorLevel% neq 0 (
    echo Warning: proswap.bat returned exit code %errorLevel%. Message: !output!
)

rem Launch ArcGIS Pro using the full path to the executable
echo Launching ArcGIS Pro
start "" "E:\sw_nt\ArcGIS\Pro\bin\ArcGISPro.exe"

rem Pause the execution and wait for user input
pause
