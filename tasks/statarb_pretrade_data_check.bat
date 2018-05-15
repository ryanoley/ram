@echo off

set SCRIPTDIR=%GITHUB%\ram\ram\strategy\statarb\implementation

echo ------------------------------
echo --  Data check for StatArb  --
echo ------------------------------

python %SCRIPTDIR%\prep_data.py

pause
