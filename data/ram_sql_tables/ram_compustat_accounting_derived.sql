/*
This is generally the table that should be called by an API
to get formatted values, which include the following:

1. EPSGROWTHQTR
2. EPSGROWTHTTM
3. SALESGROWTHQTR
4. SALESGROWTHTTM

*/

--select * from ram.dbo.ram_compustat_accounting_items

use ram;

-- ######  Final Accounting Table table   #########################################

if object_id('ram.dbo.ram_compustat_accounting_derived', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting_derived


create table	ram.dbo.ram_compustat_accounting_derived (
		GVKey int,
		AsOfDate smalldatetime,
		ItemName varchar(20),
		Value_ float
		primary key (GVKey, ItemName, AsOfDate)
)



-- ######  DATES  #########################################################

; with unique_gvkeys_dates as (
select		*,
			case
				when lag(FiscalQuarter, 4) over (
					partition by GVKey
					order by QuarterEndDate) = FiscalQuarter then 1
				else 0
			end	as LagQuarterFlag
from		(select	distinct GVKey, QuarterEndDate, ReportDate, FiscalQuarter 
			 from ram.dbo.ram_compustat_accounting) A
where		GVKey = 1690
)


-- ######  OPERATING INCOME BEFORE DEPRECIATION  ################################

, ebitda_data1 as (

select				T.*,
					D1.Value_ as OIBDPQ,
					D2.Value_ as OIBDP
from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 196

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 205
		and			D2.Item = 51

)

	
-- ######  EPS  ###############################################################

, eps_data1 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4, D3.Value_, D4.Value_ / 4) as EPSQ,
					coalesce(D2.Value_, D4.Value_) as EPSTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 101		-- EPS Excluding Extraordinary (Diluted)

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 218
		and			D2.Item = 99		-- EPS Excluding Extraordinary Trailing 12 (Diluted)

	left join		ram.dbo.ram_compustat_accounting D3
		on			T.GVKey = D3.GVKey
		and			T.QuarterEndDate = D3.QuarterEndDate
		and			D3.Group_ = 218
		and			D3.Item = 103		-- EPS Including Extraordinary (Basic)

	left join		ram.dbo.ram_compustat_accounting D4
		on			T.GVKey = D4.GVKey
		and			T.QuarterEndDate = D4.QuarterEndDate
		and			D4.Group_ = 218
		and			D4.Item = 104		-- EPS Excluding Extraordinary Trailing 12 (Basic)

)

, eps_data_final1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'EPSGROWTHQTR' as ItemName,
					case when LagQuarterFlag = 1 then
						EPSQ / lag(EPSQ, 4) over (
							partition by GVKey
							order by QuarterEndDate) - 1
					else Null end as Value_

from				eps_data1
)

, eps_data_final2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'EPSGROWTHTTM' as ItemName,
					case when LagQuarterFlag = 1 then
						EPSTTM / lag(EPSTTM, 4) over (
							partition by GVKey
							order by QuarterEndDate) - 1
					else Null end as Value_

from				eps_data1
)

-- ######  SALES  ###############################################################

, sales_data1 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as SALESQ,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as SALESTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 288

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 205
		and			D2.Item = 219

)

, sales_data_final1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESGROWTHQTR' as ItemName,
					case when LagQuarterFlag = 1 then
						SALESQ / lag(SALESQ, 4) over (
							partition by GVKey
							order by QuarterEndDate) - 1
					else Null end as Value_

from				sales_data1
)

, sales_data_final2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESGROWTHTTM' as ItemName,
					case when LagQuarterFlag = 1 then
						SALESTTM / lag(SALESTTM, 4) over (
							partition by GVKey
							order by QuarterEndDate) - 1
					else Null end as Value_

from				sales_data1
)




-- ######  STACK AND WRITE  #####################################################

, stacked_data as (
select * from eps_data_final1
union
select * from eps_data_final2
union
select * from sales_data_final1
union
select * from sales_data_final2
)


insert into ram.dbo.ram_compustat_accounting_derived
select * from stacked_data




select * from ram.dbo.ram_compustat_accounting_derived
order by AsOfDate

