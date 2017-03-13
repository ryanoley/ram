
set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

set SQLDIR=%GITHUB%\ram\data\ram_sql_tables

:: Master ID Tables
%SQLCMD% -i %SQLDIR%\ram_master_ids.sql
%SQLCMD% -i %SQLDIR%\ram_master_ids_etf.sql

:: GVKEY Mapping
call %SQLDIR%\gvkey_idc_mapping\get_map_data.bat

:: RAM Tables
%SQLCMD% -i %SQLDIR%\ram_compustat_sector.sql
%SQLCMD% -i %SQLDIR%\ram_equity_report_dates.sql
%SQLCMD% -v tabletype=1 -i %SQLDIR%\ram_equity_pricing.sql
%SQLCMD% -v tabletype=2 -i %SQLDIR%\ram_equity_pricing.sql

