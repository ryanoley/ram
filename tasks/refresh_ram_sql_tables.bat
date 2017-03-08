set RAMSCRIPTDIR=%GITHUB%\ram\data\ram_sql_tables

set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

call %RAMSCRIPTDIR%\gvkey_idc_mapping\get_map_data.bat

%SQLCMDAUTH% -i %RAMSCRIPTDIR%\ram_master_ids.sql
%SQLCMDAUTH% -i %RAMSCRIPTDIR%\ram_compustat_sector.sql
%SQLCMDAUTH% -i %RAMSCRIPTDIR%\ram_equity_report_dates.sql
%SQLCMDAUTH% -i %RAMSCRIPTDIR%\ram_equity_pricing.sql

pause
