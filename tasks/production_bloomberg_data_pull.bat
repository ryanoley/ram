@echo off
SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SCRIPTDIR=%GITHUB%\ram\tasks
set LOGFILE=%LOGDIR%\production_bloomberg_data_pull.log

echo ---------------------------
echo --  Bloomberg Data Pull  --
echo ---------------------------

>> %LOGFILE% 2>&1(
echo ---------------------------------
echo Daily data pull - %date%_!time! - Start

python %SCRIPTDIR%\production_bloomberg_data_pull.py

echo Daily data pull - %date%_!time! - End
echo:
)
