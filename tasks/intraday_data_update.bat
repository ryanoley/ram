@echo off
SETLOCAL ENABLEEXTENSIONS enabledelayedexpansion


set SCRIPTDIR=%GITHUB%\ram\ram\data
set LOGFILE=%LOGDIR%\intraday_update.log


>> %LOGFILE% 2>&1(
echo ---------------------------------
echo Intraday Data Update - %date%_!time! - Start

:: Iterate models and get allocations
python %SCRIPTDIR%\intraday_data_manager.py -u


echo Intraday Data Update - %date%_!time! - End

echo:
echo:
)


