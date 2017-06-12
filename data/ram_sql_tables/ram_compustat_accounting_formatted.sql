/*
Simplifying logic: Don't use the CSPITDHstFnd point dates. Not enough
time to figure this out at this point

select * from qai.dbo.CSPITItem

ITEMS
-----
SALEQ								:	2
EPSFXQ (EPS excl extra (Diluted)	:	9
NIQ (Net Income (Loss))				:	69

*/

use ram;

-- Put item numbers in here
declare @ITEMTABLE table (ItemNum int)
insert into @ITEMTABLE (ItemNum) values (2), (9), (69)


-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_compustat_accounting_formatted', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting_formatted


create table	ram.dbo.ram_compustat_accounting_formatted (
		GVKey int,
		QuarterDate smalldatetime,
		ReportDate smalldatetime,
		Item varchar(10),
		Value_ float,
		ValueGrowth float,
		ValueGrowthTTM float
		primary key (GVKey, QuarterDate, Item)
)


; with prepped_data as (

select				C.GVKey,
					R.QuarterDate,
					R.ReportDate,
					R.FiscalQuarter,
					Lag(FiscalQuarter, 4) over (
						partition by C.GVKey, Item
						order by Datadate) as LagFiscalQuarter,
					I.Mnemonic as Item,
					Value_,
					Sum(Value_) over (
						partition by C.GVKey, Item
						order by Datadate
						rows between 3 preceding and current row) as ValueSumFourQuarters,
					ROW_NUMBER() over (
						partition by C.GVKey, Item
						order by Datadate) as row_count

from				qai.dbo.CSPITDFnd C
	join			qai.dbo.Cspititem I
		on			C.Item = I.Number
	join			(select distinct GVKey, QuarterDate, ReportDate, FiscalQuarter from ram.dbo.ram_equity_report_dates) R
		on			R.GVKey = C.GvKey
		and			R.QuarterDate = C.Datadate
where				Valuetype = 0
	and				Item in (select * from @ITEMTABLE)

)


, prepped_data2 as (
select				GVKey,
					QuarterDate,
					ReportDate,
					Item,
					Value_,

					case
						when FiscalQuarter = LagFiscalQuarter
						then Value_ / nullif(Lag(Value_, 4) over (
								partition by GVKey, Item
								order by QuarterDate), 0) - 1
						else Null
					end as ValueGrowth,

					case
						when row_count >= 8 and FiscalQuarter = LagFiscalQuarter 
						then ValueSumFourQuarters / nullif(Lag(ValueSumFourQuarters, 4) over (
							partition by GVKey, Item
							order by QuarterDate), 0) - 1
						else Null
					end as ValueGrowthTTM

from				prepped_data

)


insert into ram.dbo.ram_compustat_accounting_formatted
select * from prepped_data2
