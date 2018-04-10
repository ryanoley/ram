@echo off
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SCRIPTDIR=%GITHUB%\ram\ram\strategy\statarb\implementation\
set LOGFILE=%LOGDIR%\statarb_daily_data_pull.log

echo -----------------------------
echo --  Data pull for StatArb  --
echo -----------------------------

>> %LOGFILE% 2>&1(
echo ---------------------------------
echo Daily data pull - %date%_!time! - Start

python %SCRIPTDIR%\get_daily_data.py

echo Daily data pull - %date%_!time! - End
echo:
)
