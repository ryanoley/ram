
@echo off

SETLOCAL ENABLEEXTENSIONS

set RAMEX_SCRIPT=%GITHUB%\ramex\ramex\scripts
set RAMEX_APP=%GITHUB%\ramex\ramex\application
set STATARB_IMP=%GITHUB%\ram\ram\strategy\statarb\implementation

CLS

:MENU
ECHO.
ECHO.
ECHO ---------------------------------------------------------------
ECHO --------------- Stat Arb Implementation Manager ---------------
ECHO ---------------------------------------------------------------
ECHO Instructions: Select an item from the menu.  Each task should
ECHO    be run daily and in numeric order.  You can exit and restart
ECHO    this application multiple times.
ECHO.
ECHO Definitions:
ECHO     TRADER ENGINE - Java trading application
ECHO     LIVE PRICES SHEET - Excel  with macros for getting eze prices
ECHO ---------------------------------------------------------------
ECHO 1.  Prep Data: 10:15 am or later. Must be run first every day
ECHO         and the output must be verified before running #2.
ECHO.
ECHO 2.  Run Trade: 3:40 pm or before.  Click Start ZMQ Button in
ECHO         TRADER ENGINE and export live prices in LIVE PRICES SHEET.
ECHO         When prompted, press ENTER once these are complete.
ECHO.
ECHO 3.  Process Executed Trades. 4:00 pm after all trading. Click
ECHO         Export Trades Button in TRADER ENGINE first.
ECHO.
ECHO 4.  Reconcile Trades. 4:30 pm after Fund Manager is updated
ECHO.
ECHO 5.  Exit this application
ECHO ----------------------------------------------------------------
ECHO.
ECHO.

set /p M="Type 1-5 and press ENTER to Run that process: "
IF %M%==1 GOTO PREPDATA
IF %M%==2 GOTO RUNLIVE
IF %M%==3 GOTO RECTRADES
IF %M%==4 GOTO RECPORT
IF %M%==5 GOTO EOF
GOTO EOF


:PREPDATA
ECHO.
ECHO.
ECHO --PREPARING DATA--
:: Start RAMEX Server
python %STATARB_IMP%\prep_data.py
ECHO --PREP DATA COMPLETE--
GOTO MENU


:RUNLIVE
ECHO.
ECHO.
ECHO --GETTING LIVE ALLOCATIONS--
:: Start RAMEX Server
start python %RAMEX_APP%\server.py -p
:: Run get live allocations script
python %STATARB_IMP%\get_live_allocations.py
ECHO --LIVE ALLOCATIONS COMPLETE--
GOTO MENU


:RECTRADES
ECHO.
ECHO.
ECHO --RECONCILING TRADES--
:: Match Stat Arb Trades with Trader Engine Executions
python %RAMEX_SCRIPT%\trade_reconciliation.py
ECHO --TRADE RECONCILIATION COMPLETE--
GOTO MENU


:RECPORT
ECHO.
ECHO.
ECHO --RECONCILING WITH FUND MANAGER--
:: Reconcile Stat Arb Trades with Trader Engine Executions
python %RAMEX_SCRIPT%\portfolio_reconciliation.py -u -w -v
ECHO --PORTFOLIO RECONCILIATION COMPLETE--
GOTO MENU


:EOF
CLS
