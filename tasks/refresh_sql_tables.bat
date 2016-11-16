
set RAMSCRIPTDIR=%GITHUB%\ram\ram\data\qad

set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\master_equities.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\master_etfs.sql"
%SQLCMDAUTH% -i "%RAMSCRIPTDIR%\table_monitor.sql"
