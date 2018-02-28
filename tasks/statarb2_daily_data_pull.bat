@echo off
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SCRIPTDIR=%GITHUB%\ram\ram\strategy\statarb2\implementation\
set LOGFILE=%LOGDIR%\statarb2_daily_data_pull.log

echo ------------------------------
echo --  Data pull for StatArb2  --
echo ------------------------------

>> %LOGFILE% 2>&1(
echo ---------------------------------
echo Daily data pull - %date%_!time! - Start

python %SCRIPTDIR%\get_daily_raw_data.py

echo Daily data pull - %date%_!time! - End
echo:
)
