@echo off
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SCRIPTDIR=%GITHUB%\ram\ram\strategy\statarb\implementation\
set LOGFILE=%LOGDIR%\statarb_data_check_log.log

echo ------------------------------
echo --  Data check for StatArb  --
echo ------------------------------

>> %LOGFILE% 2>&1(
echo ---------------------------------
echo Daily data pull - %date%_!time! - Start

python %SCRIPTDIR%\check_data.py

echo Daily data pull - %date%_!time! - End
echo:
)

START %DATA%\ram\implementation\StatArbStrategy\pretrade_data_check.csv
