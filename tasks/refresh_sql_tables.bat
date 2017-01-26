
set RAMSCRIPTDIR=%GITHUB%\ram\ram\data\qad
set PYSTATUSDIR=%GITHUB%\ram\tasks

set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\gvkey_map.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\master_equities.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\master_etf.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\data_compustat_sector.sql"


:: Check status of QAD and RAM tables and send alerts if necessary
python %PYSTATUSDIR%\db_monitor.py -update_db_stats -check_qad
