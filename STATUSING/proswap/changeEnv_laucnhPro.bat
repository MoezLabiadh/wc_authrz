@echo off
rem This script changes the Python environment of ArcGIS Pro to P:/whse_np/corp/python3916
rem and then launches ArcGIS Pro application using the proswap command.

rem Set the path to the new Python environment
set PYTHONPATH=P:\XXXXXXXXXXXXXX\python3916

rem Check if the new Python environment exists
if not exist %PYTHONPATH% (
  echo The new Python environment does not exist. Please check the path and try again.
  rem Pause the screen for 10 seconds
  timeout /t 10 /nobreak >nul
  exit /b 1
)

rem Set the path to the ArcGIS Pro executable
set ARCPROPATH="E:\XXXXXXXXXXXXXXXXXX\ArcGIS\Pro\bin\ArcGISPro.exe"

rem Check if the ArcGIS Pro executable exists
if not exist %ARCPROPATH% (
  echo The ArcGIS Pro executable does not exist. Please check the path and try again.
  rem Pause the screen for 10 seconds
  timeout /t 10 /nobreak >nul
  exit /b 2
)

rem Activate the new Python environment for the current and all future sessions of ArcGIS Pro using the proswap command[^1^][5]
proswap %PYTHONPATH%

rem Launch ArcGIS Pro with the new Python environment
start "" %ARCPROPATH%

