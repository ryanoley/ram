
set RAMSCRIPTDIR=%GITHUB%\ram\ram\data\qad
set PYSCRIPTDIR=%GITHUB%\ram\tasks

set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\master_equities.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\master_etf.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\data_compustat_sector.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\table_monitor.sql"


:: Check status of QAD and RAM tables and send alerts if necessary
python %PYSCRIPTDIR%\db_monitor.py -update_qad_status -write_log

