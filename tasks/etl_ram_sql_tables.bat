
set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

set SQLDIR=%GITHUB%\ram\data\ram_sql_tables
set TASKDIR=%GITHUB%\ram\tasks

:: GVKEY Mapping
call %SQLDIR%\gvkey_idc_mapping\get_map_data.bat

:: RAM Tables
%SQLCMD% -i %SQLDIR%\ram_master_ids.sql
%SQLCMD% -i %SQLDIR%\ram_compustat_sector.sql
%SQLCMD% -i %SQLDIR%\ram_equity_report_dates.sql
%SQLCMD% -i %SQLDIR%\ram_equity_pricing.sql

:: Check status of QAD and RAM tables and send alerts if necessary
python %TASKDIR%\db_monitor.py -update_db_stats -check_qad
