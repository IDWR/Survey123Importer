echo off

set version=0.0.1
IF %1%.==. GOTO SkipVersion
set version=%1%

:SkipVersion

cd Packages\MeasurementDatabaseClient
rd /s /q build
rd /s /q MeasurementDatabaseClient.egg-info
py -3 setup.py bdist_wheel --dist-dir=..\..\Wheels --build-number=%version%
cd ..\Survey123Client
rd /s /q build
rd /s /q Survey123Client.egg-info
py -3 setup.py bdist_wheel --dist-dir=..\..\Wheels --build-number=%version%
cd ..\..\
