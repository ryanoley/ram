
@echo off

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison
set LOGFILE=%LOGDIR%\refresh_ram_tables.log
set SQLDIR=%GITHUB%\ram\data\ram_sql_tables

>> %LOGFILE% 2>&1(
echo. & echo --------------------------------
echo RAM Table Update - %date%_!time! - Start

:: Master ID Tables
echo. & echo ~~ ram_master_ids !time!~~
%SQLCMD% -i %SQLDIR%\ram_master_ids.sql

echo. & echo  ~~ ram_master_ids_etf !time!~~
%SQLCMD% -i %SQLDIR%\ram_master_ids_etf.sql

:: Pricing
echo. & echo ~~ ram_annualized_cash_dividends !time!~~
%SQLCMD% -i %SQLDIR%\ram_annualized_cash_dividends.sql

echo. & echo ~~ ram_equity_pricing_etf !time!~~
%SQLCMD% -v tabletype=1 -i %SQLDIR%\ram_equity_pricing.sql

echo. & echo ~~ ram_equity_pricing !time!~~
%SQLCMD% -v tabletype=2 -i %SQLDIR%\ram_equity_pricing.sql

echo. & echo ~~ ram_equity_pricing_research !time!~~
%SQLCMD% -i %SQLDIR%\ram_equity_pricing_research.sql

echo. & echo ~~ ram_dividend_yield !time!~~
%SQLCMD% -i %SQLDIR%\ram_dividend_yield.sql

echo. & echo ~~ ram_index_pricing !time!~~
%SQLCMD% -i %SQLDIR%\ram_index_pricing.sql

:: Compustat Tables
echo. & echo ~~ get_map_data batch !time!~~
call %SQLDIR%\gvkey_mapping\daily_mapping.bat

echo. & echo ~~ ram_compustat_accounting !time!~~
%SQLCMD% -i %SQLDIR%\ram_compustat_accounting.sql

echo. & echo ~~ ram_compustat_accounting_derived !time!~~
%SQLCMD% -i %SQLDIR%\ram_compustat_accounting_derived.sql

echo. & echo ~~ ram_compustat_sector !time!~~
%SQLCMD% -i %SQLDIR%\ram_compustat_sector.sql

echo. & echo ~~ ram_equity_report_dates !time!~~
%SQLCMD% -i %SQLDIR%\ram_equity_report_dates.sql

:: StarMine
echo. & echo ~~ ram_starmine_map !time!~~
call %SQLDIR%\starmine\mapping\daily_mapping.bat

:: Call GVKEY Mapping Batch File
echo. & echo ~~ get_map_data batch !time!~~
call %SQLDIR%\gvkey_mapping\daily_mapping.bat

echo. & echo RAM Table Update - %date%_!time! - End
)

exit
