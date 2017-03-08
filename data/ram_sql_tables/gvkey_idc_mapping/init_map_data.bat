
set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s","

%SQLCMDAUTH% -v tablenum=1 -i "get_map_data.sql" > %DATA%\ram\data\gvkey_mapping\bad_idcdata.csv
%SQLCMDAUTH% -v tablenum=2 -i "get_map_data.sql" > %DATA%\ram\data\gvkey_mapping\bad_gvkeydata.csv
%SQLCMDAUTH% -v tablenum=3 -i "get_map_data.sql" > %DATA%\ram\data\gvkey_mapping\good_idcgvkeydata.csv

pause