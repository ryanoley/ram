
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
ECHO ---------------------------------------------------------------
ECHO 1.  Prep Data: 10:15 am or later. Must be run first every day
ECHO         and the output must be verified before running #2.
ECHO 2.  Run Trade: 3:40 pm or before.  Click Start ZMQ in Trader
ECHO         Engine and then export live prices in live_pricing
ECHO         sheet.  When prompted, press Enter if these are complete.
ECHO 3.  Reconcile Executed Trades. 4:00 pm after all trading. You
ECHO         must click Export Trades in Trader Engine first.                                                  --
ECHO 4.  Reconcile Portfolio. 4:30 pm after Fund Manager is updated
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


:PREPDATA
:: Start RAMEX Server
python %STATARB_IMP%\prep_data.py
GOTO MENU


:RUNLIVE
:: Start RAMEX Server
start python %RAMEX_APP%\server.py
:: Run get live allocations script
python %STATARB_IMP%\get_live_allocations.py
GOTO MENU


:RECTRADES
:: Reconcile Stat Arb Trades with Trader Engine Executions
python %RAMEX_SCRIPT%\trade_reconciliation.py
GOTO MENU


:RECPORT
:: Reconcile Stat Arb Trades with Trader Engine Executions
python %RAMEX_SCRIPT%\trade_reconciliation.py
GOTO MENU


:RECPORT
python %RAMEX_SCRIPT%\portfolio_reconciliation.py -u -w -v
GOTO MENU


:EOF
CLS
