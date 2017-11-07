
@echo off

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

:: RAM TABLES
call %GITHUB%\ram\data\ram_sql_tables\daily_update.bat

:: Position sheet scraper
python %GITHUB%\ram\tasks\position_sheet_scraper.py
