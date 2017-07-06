set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison -s "|"

::  Should be converted to command line arguments?
set QTR=2
set YEAR=2017

set SCRIPTS=%GITHUB%\ram\data\ern_pead\reconciliation

set OUTPUTS_ERN=%DATA%\earnings\implementation\reconcile
set OUTPUTS_PEAD=%DATA%\pead\implementation\reconcile

::  DATA
%SQLCMD% -v quarter=%QTR% year=%YEAR% trade=1 -i %SCRIPTS%\report_dates_prices.sql > %OUTPUTS_ERN%\model_report_dates_prices.txt
%SQLCMD% -v quarter=%QTR% year=%YEAR% trade=2 -i %SCRIPTS%\report_dates_prices.sql > %OUTPUTS_PEAD%\model_report_dates_prices.txt

%SQLCMD% -i %SCRIPTS%\hedge_prices.sql > %OUTPUTS_ERN%\hedge_prices.txt
%SQLCMD% -i %SCRIPTS%\hedge_prices.sql > %OUTPUTS_PEAD%\hedge_prices.txt

%SQLCMD% -i %SCRIPTS%\seccode_cusip_map.sql > %OUTPUTS_ERN%\seccode_cusip_map.txt
%SQLCMD% -i %SCRIPTS%\seccode_cusip_map.sql > %OUTPUTS_PEAD%\seccode_cusip_map.txt

::  Copy Fund Manager files - DOES NOT WORK
:: set FMDIR="\\192.168.2.8\roundaboutam\Common Folders\Roundabout\Operations\Roundabout Accounting\Fund Manager 2016\Fund Manager Export 2016.csv"
:: copy %FMDIR% "%DATA%\Fund Manager Export.csv" /y

:: set FMDIR="\\192.168.2.8\roundaboutam\Common Folders\Roundabout\Operations\Roundabout Accounting\Fund Manager 2016\Fund Manager Export Transaction Data.csv"
:: copy %FMDIR% "%DATA%\Fund Manager Export Transaction Data.csv" /y
