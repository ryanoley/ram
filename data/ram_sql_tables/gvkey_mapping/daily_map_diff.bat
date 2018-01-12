set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

set SCRIPTS=%GITHUB%\ram\data\ram_sql_tables\gvkey_mapping\

set OUTPUT=%DATA%\ram\data\gvkey_mapping2\

set dateprefix=%date:~10,4%%date:~4,2%%date:~7,2%

%SQLCMD% -i %SCRIPTS%\daily_id_diff.sql > %OUTPUT%\%dateprefix%_csvsecurity_diffs.txt

pause
