set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

set SCRIPTS=%GITHUB%\ram\data\ram_sql_tables\gvkey_idc_mapping
set OUTPUTS=%DATA%\ram\data\gvkey_mapping

%SQLCMDAUTH% -v tablenum=1 -i %SCRIPTS%\get_map_data_alternative.sql > %OUTPUTS%\bad_gvkey_mapping.txt
%SQLCMDAUTH% -v tablenum=2 -i %SCRIPTS%\get_map_data_alternative.sql > %OUTPUTS%\good_gvkey_mapping.txt

pause