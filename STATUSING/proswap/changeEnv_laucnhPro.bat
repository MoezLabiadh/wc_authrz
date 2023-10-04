@echo off
rem This script changes the Python environment of ArcGIS Pro
rem and then launches ArcGIS Pro application.

rem Set the path to the new Python environment
set PYTHONPATH=P:\XXXXXXXX\python3916

rem Check if the new Python environment exists
if not exist %PYTHONPATH% (
  echo The Python environment does not exist. Please check the path and try again.
  rem Pause the screen for 10 seconds
  timeout /t 10 /nobreak >nul
  exit /b 1
)

rem Set the path to the ArcGIS Pro executable
set ARCPROPATH="E:\XXXXXXXX\ArcGIS\Pro\bin\ArcGISPro.exe"

rem Check if the ArcGIS Pro executable exists
if not exist %ARCPROPATH% (
  echo The ArcGIS Pro executable does not exist. Please check the path and try again.
  rem Pause the screen for 10 seconds
  timeout /t 10 /nobreak >nul
  exit /b 2
)

rem Launch ArcGIS Pro with the new Python environment
start "" %ARCPROPATH%
