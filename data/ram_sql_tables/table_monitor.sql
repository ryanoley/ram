
use ram;

-- ######  Final Accounting Table table   #########################################

/*
if object_id('ram.dbo.ram_table_monitor', 'U') is not null 
	drop table ram.dbo.ram_table_monitor


create table ram.dbo.ram_table_monitor (
		StatusDateTime smalldatetime,
		TableName varchar(40),
		MinTableDate smalldatetime,
		MaxTableDate smalldatetime,
		RowCount_ int
)
*/


-- ######  Table Monitors - ALPHABETICAL ORDER  ############################################

declare @StatusDateTime smalldatetime = getdate();


; with table_ram_annualized_cash_dividends as (
select			@StatusDateTime as MonitorDateTime,
				'ram_annualized_cash_dividends' as TableName,
				min(ExDate) as MinTableDate,
				max(ExDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_annualized_cash_dividends
)


, table_ram_compustat_accounting as (
select			@StatusDateTime as MonitorDateTime,
				'ram_compustat_accounting' as TableName,
				min(ReportDate) as MinTableDate,
				max(ReportDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_compustat_accounting
)


, table_ram_compustat_accounting_derived as (
select			@StatusDateTime as MonitorDateTime,
				'ram_compustat_accounting_derived' as TableName,
				min(AsOfDate) as MinTableDate,
				max(AsOfDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_compustat_accounting_derived
)


, table_ram_compustat_sector as (
select			@StatusDateTime as MonitorDateTime,
				'ram_compustat_sector' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_compustat_sector
)


, table_ram_dividend_yield as (
select			@StatusDateTime as MonitorDateTime,
				'ram_dividend_yield' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_dividend_yield
)


, table_ram_equity_pricing as (
select			@StatusDateTime as MonitorDateTime,
				'ram_equity_pricing' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_equity_pricing
)


, table_ram_equity_pricing_research as (
select			@StatusDateTime as MonitorDateTime,
				'ram_equity_pricing_research' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_equity_pricing_research
)


, table_ram_equity_report_dates as (
select			@StatusDateTime as MonitorDateTime,
				'ram_equity_report_dates' as TableName,
				min(ReportDate) as MinTableDate,
				max(ReportDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_equity_report_dates
)


, table_ram_index_pricing as (
select			@StatusDateTime as MonitorDateTime,
				'ram_index_pricing' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_index_pricing
)


, table_ram_master_ids as (
select			@StatusDateTime as MonitorDateTime,
				'ram_master_ids' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_master_ids
)


, table_ram_master_ids_etf as (
select			@StatusDateTime as MonitorDateTime,
				'ram_master_ids_etf' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_master_ids_etf
)


, table_ram_starmine_map as (
select			@StatusDateTime as MonitorDateTime,
				'ram_starmine_map' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_starmine_map
)


, table_ram_trading_dates as (
select			@StatusDateTime as MonitorDateTime,
				'ram_trading_dates' as TableName,
				min(CalendarDate) as MinTableDate,
				max(CalendarDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_trading_dates
)


--  Created in separate directory
, table_ram_idccode_to_gvkey_map as (
select			@StatusDateTime as MonitorDateTime,
				'ram_idccode_to_gvkey_map' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_idccode_to_gvkey_map
)


insert into ram.dbo.ram_table_monitor
select * from table_ram_annualized_cash_dividends
union
select * from table_ram_compustat_accounting
union
select * from table_ram_compustat_accounting_derived
union
select * from table_ram_compustat_sector
union
select * from table_ram_dividend_yield
union
select * from table_ram_equity_pricing
union
select * from table_ram_equity_pricing_research
union
select * from table_ram_equity_report_dates
union
select * from table_ram_index_pricing
union
select * from table_ram_master_ids
union
select * from table_ram_master_ids_etf
union
select * from table_ram_starmine_map
union
select * from table_ram_trading_dates
union
select * from table_ram_idccode_to_gvkey_map
