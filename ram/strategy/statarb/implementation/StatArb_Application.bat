
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
ECHO.
ECHO ------------------------ INSTRUCTIONS -------------------------
ECHO Select an item from the menu.  Tasks 1-4 should be run daily
ECHO    and in numeric order. You can exit and restart this app anytime
ECHO.
ECHO Definitions:
ECHO     TRADER ENGINE - Java trading application
ECHO     LIVE PRICES SHEET - Excel  with macros for getting eze prices
ECHO.
ECHO -------------------TRADE IMPLEMENTAION TASKS---------------------
ECHO.
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
ECHO 4.  Process Fund Manager Export. 4:30 pm after Fund Manager
ECHO.         is updated
ECHO.
ECHO ------------------------- OTHER TASKS ---------------------------
ECHO.
ECHO 5.  Run pricing and trade reconciliation.  Run this once daily to
ECHO.        generate reconciliation outputs for the prior day.
ECHO.        NOT NEEDED for daily trading
ECHO.
ECHO 6.  Exit this application
ECHO ----------------------------------------------------------------
ECHO.
ECHO.

set /p M="Type 1-5 and press ENTER to Run that process: "
IF %M%==1 GOTO PREPDATA
IF %M%==2 GOTO RUNLIVE
IF %M%==3 GOTO PROCTRADES
IF %M%==4 GOTO PROCFM
IF %M%==5 GOTO RECON
IF %M%==6 GOTO EOF
GOTO EOF


:PREPDATA
ECHO.
ECHO.
ECHO --PREPARING DATA--
:: Start RAMEX Server
ECHO Output must be validated and any [ERROR] Messages Resolved
python %STATARB_IMP%\prep_data.py
ECHO --PREP DATA COMPLETE--
GOTO SPACING


:RUNLIVE
ECHO.
ECHO.
ECHO --GETTING LIVE ALLOCATIONS--
ECHO Click Start ZMQ Button in TRADER ENGINE
ECHO Export Live Prices from LIVE PRICES SHEET at 3:40 pm or later
:: Start RAMEX Server
start python %RAMEX_APP%\server.py -p
:: Run get live allocations script
python %STATARB_IMP%\get_live_allocations.py
ECHO --LIVE ALLOCATIONS COMPLETE--
GOTO SPACING


:PROCTRADES
ECHO.
ECHO.
ECHO --PROCESSING TRADE EXECUTIONS--
:: Match StatArb Trades with Trader Engine Executions
python %RAMEX_SCRIPT%\trade_reconciliation.py
ECHO --TRADE PROCESSING COMPLETE--
GOTO SPACING


:PROCFM
ECHO.
ECHO.
ECHO --PROCESSING FUND MANAGER EXPORT--
:: Import and verify with Fund Manager Transactions
python %RAMEX_SCRIPT%\portfolio_reconciliation.py -u -w -v
ECHO --FUND MANAGER PROCESSING COMPLETE--
GOTO SPACING


:RECON
ECHO.
ECHO.
ECHO --RUNNING RECONCILIATION--
:: Reconcile Pricing and Trades for StatArb
python %STATARB_IMP%\reconciliation_raw.py -p -o
ECHO --RECONCILIATION COMPLETE--
GOTO SPACING


:SPACING
ECHO.
ECHO.
ECHO.
ECHO ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
ECHO.
ECHO.
ECHO.
ECHO.
GOTO MENU

:EOF
CLS
