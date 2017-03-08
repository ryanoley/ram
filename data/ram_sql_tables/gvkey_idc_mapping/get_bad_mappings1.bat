
set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s","

%SQLCMDAUTH% -v tablenum=1 -i "get_bad_mappings.sql" > %DATA%\ram\data\gvkey_mapping\idcdata.csv
%SQLCMDAUTH% -v tablenum=2 -i "get_bad_mappings.sql" > %DATA%\ram\data\gvkey_mapping\gvkeydata.csv

pause