@echo off

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

set SCRIPTS=%GITHUB%\ram\data\ram_sql_tables\starmine\mapping\

set OUTPUT=%DATA%\ram\data\starmine\

set dateprefix=%date:~10,4%%date:~4,2%%date:~7,2%

%SQLCMD% -i %SCRIPTS%\ram_starmine_map.sql > %OUTPUT%\%dateprefix%_problem_seccodes.txt

:: python %SCRIPTS%\handle_mappings.py

pause