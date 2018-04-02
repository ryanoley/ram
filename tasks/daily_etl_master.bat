
@echo off

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison
set LOGFILE=%LOGDIR%\daily_etl_master.log


>> %LOGFILE% 2>&1 (
echo ------------------------------
echo Daily ETL Master - %date%_!time! - Start

:: RAM TABLES
echo. & echo ~~ ram_table_update batch !time!~~
start /b /WAIT %GITHUB%\ram\data\ram_sql_tables\ram_table_update.bat

:: Position sheet scraper
echo. & echo ~~ position_sheet_scraper !time!~~
python %GITHUB%\ram\tasks\position_sheet_scraper.py

:: Table Monitor
echo. & echo ~~ table_monitor !time!~~
%SQLCMD% -i %GITHUB%\ram\data\ram_sql_tables\table_monitor.sql

echo. & echo Daily ETL Master - %date%_!time! - End

)

