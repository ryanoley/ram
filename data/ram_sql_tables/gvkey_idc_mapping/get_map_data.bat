
@echo off

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SQLCMDAUTH="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

set SCRIPTS=%GITHUB%\ram\data\ram_sql_tables\gvkey_idc_mapping
set OUTPUTS=%DATA%\ram\data\gvkey_mapping

%SQLCMDAUTH% -v tablenum=1 -i %SCRIPTS%\get_map_data.sql > %OUTPUTS%\bad_idcdata.txt
%SQLCMDAUTH% -v tablenum=2 -i %SCRIPTS%\get_map_data.sql > %OUTPUTS%\bad_gvkeydata.txt
%SQLCMDAUTH% -v tablenum=3 -i %SCRIPTS%\get_map_data.sql > %OUTPUTS%\good_idcgvkeydata.txt

python %SCRIPTS%\mapping_verification.py --check_mappings
python %SCRIPTS%\make_mapping_table.py
