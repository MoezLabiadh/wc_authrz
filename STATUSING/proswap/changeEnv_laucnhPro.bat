@echo off
rem This bat file uses proswap.bat to change the python environment of arcgis pro
rem proswap.bat is located in E:\XXX\ArcGIS\Pro\bin\Python\Scripts
rem The new python environment is P:\XXX\python3916

rem Change the current directory to the location of proswap.bat
cd /d E:\XXX\ArcGIS\Pro\bin\Python\Scripts

rem Check the current python environment and compare it with the new one
rem call python -m sys -c "print(sys.prefix)"
call python -c "import sys; print(sys.prefix)"
set current_env=%errorlevel%
if %current_env% == P:\corp\python3916 (
    echo Python environment is already set to P:\corp\python3916.
) else (
    rem Call proswap.bat with the new python environment as an argument
    echo Changing Python environment to P:\XXX\python3916.
    call proswap.bat P:\XXX\python3916

    rem Check the status of the operation
    if %errorlevel% == 0 (
        echo Python environment changed successfully.
    ) else (
        echo An error occurred while changing the python environment.
    )
)

rem Launch arcgis pro using the full path to the executable
echo Launching arcgis pro
start "" "E:\XXX\ArcGIS\Pro\bin\ArcGISPro.exe"

rem Pause the execution and wait for user input
pause
