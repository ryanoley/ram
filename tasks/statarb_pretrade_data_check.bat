@echo off

set SCRIPTDIR=%GITHUB%\ram\ram\strategy\statarb\implementation\execution

echo ------------------------------
echo --  Data check for StatArb  --
echo ------------------------------

python %SCRIPTDIR%\check_data.py

START %DATA%\ram\implementation\StatArbStrategy\pretrade_data_check.csv

pause
