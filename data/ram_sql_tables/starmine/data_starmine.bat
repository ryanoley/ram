
@echo off

::	This code is used to download files from Thomson Rueters ftp and insert records
::	into the ram database.
::	Environment variables should be set on client machine to the DATA directory
::	on the MarkertQA server and the local Git repo.

SETLOCAL ENABLEEXTENSIONS ENABLEDELAYEDEXPANSION

set SCRIPTDIR=%GITHUB%\ram\data\ram_sql_tables\starmine
set LOGFILE=%LOGDIR%\starmine.log


>> %LOGFILE% 2>&1(
echo -------------------------
echo DATA - StarMine - %date%_!time! - Start

python "%SCRIPTDIR%\starmine_ftp.py"

echo DATA - StarMine - %date%_!time! - Complete
echo:
echo:
echo:
)

exit

