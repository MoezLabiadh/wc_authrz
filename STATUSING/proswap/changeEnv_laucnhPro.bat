@echo off
rem This bat file changes the python environment of ArcGIS Pro, then launches the app.
rem proswap.bat is located in E:\XXX\ArcGIS\Pro\bin\Python\Scripts
rem The new python environment is P:\XXX\python3916

rem Check the current python environment using Python itself
for /f "delims=" %%a in ('python -c "import sys; print(sys.prefix)"') do set current_env=%%a

if "%current_env%"=="P:\XXX\python3916" (
    echo Python environment is already set to P:\corp\python3916.
) else (
    rem Call proswap.bat with the new python environment as an argument
    echo Changing Python environment to P:\XXX\python3916.

    cd /d E:\sw_nt\ArcGIS\Pro\bin\Python\Scripts
    call proswap.bat P:\XXX\python3916

    rem Check the status of the operation
    if %errorlevel%==0 (
        echo Python environment changed successfully.
    ) else (
        echo An error occurred while changing the python environment.
    )
)

rem Launch ArcGIS Pro using the full path to the executable
echo Launching ArcGIS Pro
start "" "E:\XXX\ArcGIS\Pro\bin\ArcGISPro.exe"

rem Pause the execution and wait for user input
pause
