:: Used when gvkey mapping tables have changed.

set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison
set LOGFILE=%LOGDIR%\refresh_ram_tables.log
set SQLDIR=%GITHUB%\ram\data\ram_sql_tables

:: Compustat Tables
echo. & echo ~~ get_map_data batch !time!~~
call %SQLDIR%\gvkey_mapping\daily_mapping.bat

echo. & echo ~~ ram_compustat_accounting_derived !time!~~
%SQLCMD% -i %SQLDIR%\ram_compustat_accounting_derived.sql

echo. & echo ~~ ram_equity_report_dates !time!~~
%SQLCMD% -i %SQLDIR%\ram_equity_report_dates.sql
