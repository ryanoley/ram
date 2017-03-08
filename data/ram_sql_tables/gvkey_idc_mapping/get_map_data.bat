set SCRIPTDIR=%GITHUB%\ram\data\ram_sql_tables\gvkey_idc_mapping
set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s","

%SQLCMDAUTH% -v tablenum=1 -i %SCRIPTDIR%\get_map_data.sql > %DATA%\ram\data\gvkey_mapping\bad_idcdata.csv
%SQLCMDAUTH% -v tablenum=2 -i %SCRIPTDIR%\get_map_data.sql > %DATA%\ram\data\gvkey_mapping\bad_gvkeydata.csv
%SQLCMDAUTH% -v tablenum=3 -i %SCRIPTDIR%\get_map_data.sql > %DATA%\ram\data\gvkey_mapping\good_idcgvkeydata.csv

python %SCRIPTDIR%\mapping_verification.py --check_mappings
python %SCRIPTDIR%\make_mapping_table.py
