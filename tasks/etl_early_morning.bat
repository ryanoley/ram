
set SQLCMD="%SQLCMDPATH%\sqlcmd" -S 192.168.2.8 -d ram -U ramuser -P 183madison

:: RAM TABLES
call %GITHUB%\ram\data\ram_sql_tables\daily_update.bat

:: Check status of QAD and RAM tables and send alerts if necessary
python %GITHUB%\ram\tasks\db_monitor.py -update_db_stats -check_qad
