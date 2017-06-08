set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s ","

:: Report Dates for earnings and post earnings put into table
:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\universe_filter_report_dates.sql
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\universe_filter_report_dates.sql

:: Report Dates for earnings and post earnings to file
:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\report_dates_returns.sql > %DATA%\ram\data\temp_ern_pead\earnings\report_dates_returns.csv
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\report_dates_returns.sql > %DATA%\ram\data\temp_ern_pead\pead\report_dates_returns.csv

:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_1.sql > %DATA%\ram\data\temp_ern_pead\earnings\technical_vars_1.csv
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_1.sql > %DATA%\ram\data\temp_ern_pead\pead\technical_vars_1.csv

%SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_2.sql > %DATA%\ram\data\temp_ern_pead\earnings\technical_vars_2.csv
%SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_2.sql > %DATA%\ram\data\temp_ern_pead\pead\technical_vars_2.csv

:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_3.sql > %DATA%\ram\data\temp_ern_pead\earnings\technical_vars_3.csv
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_3.sql > %DATA%\ram\data\temp_ern_pead\pead\technical_vars_3.csv

:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_4.sql > %DATA%\ram\data\temp_ern_pead\earnings\technical_vars_4.csv
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\technical_vars_4.sql > %DATA%\ram\data\temp_ern_pead\pead\technical_vars_4.csv

:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\dividend_yield.sql > %DATA%\ram\data\temp_ern_pead\earnings\dividend_yield.csv
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\dividend_yield.sql > %DATA%\ram\data\temp_ern_pead\pead\dividend_yield.csv

:: %SQLCMD% -v trade=1 -i %GITHUB%\ram\data\ern_pead\research\starmine_arm.sql > %DATA%\ram\data\temp_ern_pead\earnings\starmine_arm.csv
:: %SQLCMD% -v trade=2 -i %GITHUB%\ram\data\ern_pead\research\starmine_arm.sql > %DATA%\ram\data\temp_ern_pead\pead\starmine_arm.csv

:: %SQLCMD% -i %GITHUB%\ram\data\ern_pead\research\accounting_ern.sql > %DATA%\ram\data\temp_ern_pead\earnings\accounting.csv
:: %SQLCMD% -i %GITHUB%\ram\data\ern_pead\research\accounting_pead.sql > %DATA%\ram\data\temp_ern_pead\pead\accounting.csv

pause
