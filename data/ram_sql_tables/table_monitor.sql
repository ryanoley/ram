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
select			@StatusDateTime as StatusDateTime,
				'ram_annualized_cash_dividends' as TableName,
				min(ExDate) as MinTableDate,
				max(ExDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_annualized_cash_dividends
)


, table_ram_compustat_accounting as (
select			@StatusDateTime as StatusDateTime,
				'ram_compustat_accounting' as TableName,
				min(ReportDate) as MinTableDate,
				max(ReportDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_compustat_accounting
)


, table_ram_compustat_accounting_derived as (
select			@StatusDateTime as StatusDateTime,
				'ram_compustat_accounting_derived' as TableName,
				min(AsOfDate) as MinTableDate,
				max(AsOfDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_compustat_accounting_derived
)


, table_ram_compustat_sector as (
select			@StatusDateTime as StatusDateTime,
				'ram_compustat_sector' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_compustat_sector
)


, table_ram_dividend_yield as (
select			@StatusDateTime as StatusDateTime,
				'ram_dividend_yield' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_dividend_yield
)


, table_ram_equity_pricing as (
select			@StatusDateTime as StatusDateTime,
				'ram_equity_pricing' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_equity_pricing
)


, table_ram_equity_pricing_research as (
select			@StatusDateTime as StatusDateTime,
				'ram_equity_pricing_research' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_equity_pricing_research
)


, table_ram_equity_report_dates as (
select			@StatusDateTime as StatusDateTime,
				'ram_equity_report_dates' as TableName,
				min(ReportDate) as MinTableDate,
				max(ReportDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_equity_report_dates
)


, table_ram_index_pricing as (
select			@StatusDateTime as StatusDateTime,
				'ram_index_pricing' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_index_pricing
)


, table_ram_master_ids as (
select			@StatusDateTime as StatusDateTime,
				'ram_master_ids' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_master_ids
)


, table_ram_master_ids_etf as (
select			@StatusDateTime as StatusDateTime,
				'ram_master_ids_etf' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_master_ids_etf
)

, table_ram_etf_pricing as (
select			@StatusDateTime as StatusDateTime,
				'ram_etf_pricing' as TableName,
				min(Date_) as MinTableDate,
				max(Date_) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_etf_pricing
)

, table_ram_starmine_map as (
select			@StatusDateTime as StatusDateTime,
				'ram_starmine_map' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_starmine_map
)


, table_ram_trading_dates as (
select			@StatusDateTime as StatusDateTime,
				'ram_trading_dates' as TableName,
				min(CalendarDate) as MinTableDate,
				max(CalendarDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_trading_dates
)


--  Created in separate directory
, table_ram_idccode_to_gvkey_map as (
select			@StatusDateTime as StatusDateTime,
				'ram_idccode_to_gvkey_map' as TableName,
				min(StartDate) as MinTableDate,
				max(EndDate) as MaxTableDate,
				count(*) as RowCount_
from			ram.dbo.ram_idccode_to_gvkey_map
)


-- Legacy Tables
-- univ_filter_data
, univ_filter as (
select 
    @StatusDateTime as StatusDateTime,
    'univ_filter_data' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.univ_filter_data
)

-- univ_filter_data_etf
, univ_filter_etf as (
select 
    @StatusDateTime as StatusDateTime,
    'univ_filter_data_etf' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.univ_filter_data_etf
)

-- ern_live
, ern_live as (
select 
    @StatusDateTime as StatusDateTime,
    'ern_event_dates_live' as TableName,
    min(EvalDate) as MinTableDate,
    max(EvalDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ern_event_dates_live
)

-- pead_live
, pead_live as (
select 
    @StatusDateTime as StatusDateTime,
    'pead_event_dates_live' as TableName,
    min(EvalDate) as MinTableDate,
    max(EvalDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.pead_event_dates_live
)

-- report_dates
, report_dates as (
select 
    @StatusDateTime as StatusDateTime,
    'report_dates' as TableName,
    min(ReportDate) as MinTableDate,
    max(ReportDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.report_dates
)

-- short_interest
, short_int as (
select 
    @StatusDateTime as StatusDateTime,
    'ShortInterest' as TableName,
    min(Date_) as MinTableDate,
    max(Date_) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ShortInterest
)

-- Starmine ARM
, starmine_arm as (
select 
    @StatusDateTime as StatusDateTime,
    'ram_starmine_arm' as TableName,
    min(AsOfDate) as MinTableDate,
    max(AsOfDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_starmine_arm
)

-- Starmine ShortInterest
, starmine_si as (
select 
    @StatusDateTime as StatusDateTime,
    'ram_starmine_short_interest' as TableName,
    min(AsOfDate) as MinTableDate,
    max(AsOfDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_starmine_short_interest
)


-- Starmine SmartEstimate
, starmine_smart_estimate as (
select 
    @StatusDateTime as StatusDateTime,
    'ram_starmine_smart_estimate' as TableName,
    min(AsOfDate) as MinTableDate,
    max(AsOfDate) as MaxTableDate,
    count(*) as Count_
from ram.dbo.ram_starmine_smart_estimate
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
select * from table_ram_etf_pricing
union
select * from table_ram_starmine_map
union
select * from table_ram_trading_dates
union
select * from table_ram_idccode_to_gvkey_map
union
select * from univ_filter
union
select * from univ_filter_etf
union
select * from ern_live
union
select * from pead_live
union
select * from report_dates
union
select * from short_int
union
select * from starmine_arm
union
select * from starmine_si
union
select * from starmine_smart_estimate
