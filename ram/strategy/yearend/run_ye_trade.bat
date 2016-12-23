@echo off

SETLOCAL ENABLEEXTENSIONS

set RAMSCRIPTDIR=%GITHUB%\ram\ram\data\qad
set YESCRIPTDIR=%GITHUB%\ram\ram\strategy\yearend

:: Update tables
echo Updating database tables
set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\research_tables.sql"


echo Running Trade
:: Iterate models and get allocations
python "%YESCRIPTDIR%\main.py"


