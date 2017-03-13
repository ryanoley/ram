set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s","

:: Report Dates for earnings and post earnings
%SQLCMD% -v quarter=1 year=2017 trade=1 -i %GITHUB%\ram\data\ern_pead\reconciliation\report_dates_prices.sql > %DATA%\earnings\implementation\reconcile\reconcile_data.csv
%SQLCMD% -v quarter=1 year=2017 trade=2 -i %GITHUB%\ram\data\ern_pead\reconciliation\report_dates_prices.sql > %DATA%\pead\implementation\reconcile\reconcile_data.csv

%SQLCMD% -i %GITHUB%\ram\data\ern_pead\reconciliation\hedge_prices.sql > %DATA%\earnings\implementation\reconcile\hedge_prices.csv
%SQLCMD% -i %GITHUB%\ram\data\ern_pead\reconciliation\hedge_prices.sql > %DATA%\pead\implementation\reconcile\hedge_prices.csv

::  Copy Fund Manager files
set FMDIR="\\192.168.2.8\roundaboutam\Common Folders\Roundabout\Operations\Roundabout Accounting\Fund Manager 2016\Fund Manager Export 2016.csv"
copy %FMDIR% "%DATA%\Fund Manager Export.csv" /y

set FMDIR="\\192.168.2.8\roundaboutam\Common Folders\Roundabout\Operations\Roundabout Accounting\Fund Manager 2016\Fund Manager Export Transaction Data.csv"
copy %FMDIR% "%DATA%\Fund Manager Export Transaction Data.csv" /y

pause
