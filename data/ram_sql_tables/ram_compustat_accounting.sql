/*
Simplifying logic: Don't use the CSPITDHstFnd point dates. Not enough
time to figure this out at this point

select * from qai.dbo.CSPITItem

ITEMS
-----
SALEQ :			2
*/

use ram;

-- Put item numbers in here
declare @ITEMTABLE table (ItemNum int)
insert into @ITEMTABLE (ItemNum) select (2)


-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_compustat_accounting', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting


create table	ram.dbo.ram_compustat_accounting (
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
	join			ram.dbo.ram_equity_report_dates R
		on			R.GVKey = C.GvKey
		and			R.QuarterDate = C.Datadate
where				Valuetype = 0
	and				Item in (select * from @ITEMTABLE)
	and				R.FiscalQuarter = Lag(FiscalQuarter, 4)
										  over (
											partition by C.GVKey, Item
											order by Datadate)
)


, prepped_data2 as (
select				GVKey,
					QuarterDate,
					ReportDate,
					Item,
					Value_,
					Value_ / nullif(Lag(Value_, 4) over (
						partition by GVKey, Item
						order by QuarterDate), 0) - 1 as ValueGrowth,

					Case
						when row_count < 8 then null
						else ValueSumFourQuarters / nullif(Lag(ValueSumFourQuarters, 4) over (
							partition by GVKey, Item
							order by QuarterDate), 0) - 1
					end as ValueGrowthTTM

from				prepped_data

)


insert into ram.dbo.ram_compustat_accounting
select * from prepped_data2
