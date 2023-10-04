@echo off
rem This script changes the Python environment of ArcGIS Pro
rem and then launches ArcGIS Pro application.

rem Set the path to the new Python environment
set PYTHONPATH=P:/XXXXXXX/python3916

rem Set the path to the ArcGIS Pro executable
set ARCPROPATH="E:\XXXX\ArcGIS\Pro\bin\ArcGISPro.exe"

rem Launch ArcGIS Pro with the new Python environment
start "" %ARCPROPATH%
