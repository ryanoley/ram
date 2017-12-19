set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

set SCRIPTS=%GITHUB%\ram\data\ern_pead\research\

set OUTPUTS_ERN=%DATA%\ram\data\temp_ern_pead\earnings
set OUTPUTS_PEAD=%DATA%\ram\data\temp_ern_pead\pead

:: Report Dates for earnings and post earnings put into table
%SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\universe_filter_report_dates.sql
%SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\universe_filter_report_dates.sql

:: Report Dates for earnings and post earnings to file
%SQLCMD% -v trade=1 -i %SCRIPTS%\report_dates_returns.sql > %OUTPUTS_ERN%\report_dates_returns.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\report_dates_returns.sql > %OUTPUTS_PEAD%\report_dates_returns.txt

%SQLCMD% -v trade=1 -i %SCRIPTS%\technical_vars_1.sql > %OUTPUTS_ERN%\technical_vars_1.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\technical_vars_1.sql > %OUTPUTS_PEAD%\technical_vars_1.txt

%SQLCMD% -v trade=1 -i %SCRIPTS%\technical_vars_2.sql > %OUTPUTS_ERN%\technical_vars_2.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\technical_vars_2.sql > %OUTPUTS_PEAD%\technical_vars_2.txt

%SQLCMD% -v trade=1 -i %SCRIPTS%\technical_vars_3.sql > %OUTPUTS_ERN%\technical_vars_3.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\technical_vars_3.sql > %OUTPUTS_PEAD%\technical_vars_3.txt

%SQLCMD% -v trade=1 -i %SCRIPTS%\technical_vars_4.sql > %OUTPUTS_ERN%\technical_vars_4.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\technical_vars_4.sql > %OUTPUTS_PEAD%\technical_vars_4.txt

%SQLCMD% -v trade=1 -i %SCRIPTS%\dividend_yield.sql > %OUTPUTS_ERN%\dividend_yield.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\dividend_yield.sql > %OUTPUTS_PEAD%\dividend_yield.txt

%SQLCMD% -v trade=1 -i %SCRIPTS%\starmine_arm.sql > %OUTPUTS_ERN%\starmine_arm.txt
%SQLCMD% -v trade=2 -i %SCRIPTS%\starmine_arm.sql > %OUTPUTS_PEAD%\starmine_arm.txt

%SQLCMD% -i %SCRIPTS%\accounting_ern.sql > %OUTPUTS_ERN%\accounting.txt
%SQLCMD% -i %SCRIPTS%\accounting_pead.sql > %OUTPUTS_PEAD%\accounting.txt

pause
