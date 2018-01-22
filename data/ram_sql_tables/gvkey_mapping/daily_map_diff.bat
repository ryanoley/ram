@echo off

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

set SCRIPTS=%GITHUB%\ram\data\ram_sql_tables\gvkey_mapping\

set OUTPUT=%DATA%\ram\data\gvkey_mapping2\

set LOGFILE=%LOGDIR%\csvsecurity_diffs.log

set dateprefix=%date:~10,4%%date:~4,2%%date:~7,2%

>> %LOGFILE% 2>&1 (

echo ------------------------------
echo %date%_!time! - Start

%SQLCMD% -i %SCRIPTS%\daily_id_diff.sql > %OUTPUT%\%dateprefix%_csvsecurity_diffs.txt

echo %date%_!time! - End
echo ------------------------------
)
