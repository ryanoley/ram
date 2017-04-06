set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s ","

:: Report Dates for earnings and post earnings put into table
%SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\universe_filter_report_dates.sql
%SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\universe_filter_report_dates.sql

pause