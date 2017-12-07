# Tasks Directory

## Database

#### `daily_etl_master.bat`

* Updates ram tables via: `ram_sql_tables\ram_table_update.bat`
* Scrapes positions sheet via: `tasks\position_sheet_scraper.py`
* Calls table monitor script: `ram_sql_tables\table_monitor.sql`

This script is called in the morning.

#### `intraday_data_update.bat`:


#### `weekend_tasks.bat`

Calls `table_coverage_analysis.py` on the weekends.


## Trading-Related

#### Bloomberg

There is some live data that needs to be pulled from Bloomberg for end-of-day execution of trades, including Splits, Dividends and Spin-Offs. This code should live on the Bloomberg machine, and require a local scheduled job to run at some point mid-day.

`production_bloomberg_data_pull.py`

#### StatArb

Data needs to be pulled daily for the StatArb trade, and a job must be created to call the following batch file:

`statarb_daily_data_pull.bat`
