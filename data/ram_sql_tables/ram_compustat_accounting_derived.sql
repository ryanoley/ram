/*
This is generally the table that should be called by an API
to get formatted values, which include the following:

[Net Income]
* NETINCOMEQ
* NETINCOMETTM
* NETINCOMEGROWTHQ
* NETINCOMEGROWTHTTM

[Operating Income]
* OPERATINGINCOMEQ
* OPERATINGINCOMETTM
* OPERATINGINCOMEGROWTHQ
* OPERATINGINCOMEGROWTHTTM

[Sales]
* SALESQ
* SALESTTM
* SALESGROWTHQ
* SALESGROWTHTTM

[Cash]
* FREECASHFLOW
* FREECASHFLOWGROWTHQ
* FREECASHFLOWGROWTHTTM
* X_CASHANDSECURITIES


[Debt]




[Operating Income]
* Net Debt / Operating Income


[OTHER]
* GROSSMARGIN: (Sales - Cogs) / Sales



net debt/EBITDA
Ebitda margin %
EBIT margin %
Sales growth
EBITDA growth

Cash as a % of the EV



Enterprise Value: market cap - cash + debt + preferred equity
Net Debt: total debt + preferred equity - cash & marketable securities


*/

--select * from ram.dbo.ram_compustat_accounting_items

use ram;

-- ######  Final Accounting Table table   #########################################

if object_id('ram.dbo.ram_compustat_accounting_derived', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting_derived


create table	ram.dbo.ram_compustat_accounting_derived (
		GVKey int,
		AsOfDate smalldatetime,
		ItemName varchar(30),
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
where		GVKey in (1690, 3243, 6066)
)


-- ######  OPERATING INCOME BEFORE DEPRECIATION  ################################

, operating_income_data as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as operating_income,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as operating_income_ttm

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


, operating_income_1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMEQ' as ItemName,
					operating_income
from				operating_income_data
)


, operating_income_2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMETTM' as ItemName,
					operating_income_ttm
from				operating_income_data
)


, operating_income_3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMEGROWTHQ' as ItemName,
					operating_income / lag(operating_income, 4) over (
						partition by GVKey
						order by QuarterEndDate) - 1 as Value_

from				operating_income_data
)


, operating_income_4 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMEGROWTHTTM' as ItemName,
					operating_income_ttm / lag(operating_income_ttm, 4) over (
						partition by GVKey
						order by QuarterEndDate) - 1 as Value_

from				operating_income_data
)

-- ######  CASH AND MARKETABLE SECURITIES  ###########################################
--- NOT EVEN CLOSE TO CORRECT FOR `C` (Perhaps financials generally)

, cash_and_marketable_securities_data1 as (

select				T.GVKey,
					T.ReportDate as AsOfDate,
					'X_CASHANDSECURITIES' as ItemName,
					(D1.Value_ + D2.Value_) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 54		-- Cash and Short Term Securities

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 218
		and			D2.Item = 162		-- Long Term Investments

)


-- ######  NET INCOME  ###############################################################

, net_income_data1 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as net_income,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as net_income_ttm

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 184		-- Net Income Quarterly

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 205
		and			D2.Item = 26		-- Net Income

)

, net_income_data_final0 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMEQ' as ItemName,
					net_income as Value_

from				net_income_data1
)


, net_income_data_final1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMETTM' as ItemName,
					net_income_ttm as Value_

from				net_income_data1
)


, net_income_data_final2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMEGROWTHQ' as ItemName,
					net_income / lag(net_income, 4) over (
						partition by GVKey
						order by QuarterEndDate) - 1 as Value_

from				net_income_data1
)


, net_income_data_final3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMEGROWTHTTM' as ItemName,
					net_income_ttm / lag(net_income_ttm, 4) over (
							partition by GVKey
							order by QuarterEndDate) - 1 as Value_

from				net_income_data1
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
					'SALESGROWTHQ' as ItemName,
					SALESQ / lag(SALESQ, 4) over (
						partition by GVKey
						order by QuarterEndDate) - 1 as Value_
from				sales_data1
)


, sales_data_final2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESGROWTHTTM' as ItemName,
					SALESTTM / lag(SALESTTM, 4) over (
							partition by GVKey
							order by QuarterEndDate) - 1 as Value_
from				sales_data1
)


, sales_data_final3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESQ' as ItemName,
					SALESQ as Value_
from				sales_data1
)


, sales_data_final4 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESTTM' as ItemName,
					SALESTTM as Value_
from				sales_data1
)

-- ######  PROFIT TO ASSETS  #####################################################

, prof_assets_1 as (
select				T.*,

					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as SALESTTM,

					sum(coalesce(D3.Value_, D4.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as COGSTTM,

					coalesce(D5.Value_, D6.Value_) as ASSETS

from				unique_gvkeys_dates T

	-- SALES
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

	-- COGS
	left join		ram.dbo.ram_compustat_accounting D3
		on			T.GVKey = D3.GVKey
		and			T.QuarterEndDate = D3.QuarterEndDate
		and			D3.Group_ = 218
		and			D3.Item = 65

	left join		ram.dbo.ram_compustat_accounting D4
		on			T.GVKey = D4.GVKey
		and			T.QuarterEndDate = D4.QuarterEndDate
		and			D4.Group_ = 204
		and			D4.Item = 128

	-- ASSETS
	left join		ram.dbo.ram_compustat_accounting D5
		on			T.GVKey = D5.GVKey
		and			T.QuarterEndDate = D5.QuarterEndDate
		and			D5.Group_ = 218
		and			D5.Item = 37

	left join		ram.dbo.ram_compustat_accounting D6
		on			T.GVKey = D6.GVKey
		and			T.QuarterEndDate = D6.QuarterEndDate
		and			D6.Group_ = 204
		and			D6.Item = 58

)

, prof_assets_final as (
select			GVKey,
				ReportDate as AsOfDate,
				'X_GROSSPROFASSET' as ItemName,
				(SALESTTM - COGSTTM) / nullif(ASSETS, 0) as Value_
from			prof_assets_1
)


-- ######  FREE CASH FLOW  #####################################################

, free_cash_flow as (
select				T.GVKey,
					T.ReportDate as AsOfDate,
					'FREECASHFLOW' as ItemName,
					(D1.Value_ - D2.Value_) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = -999
		and			D1.Item = 108

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = -999
		and			D2.Item = 90

)


, free_cash_flow0 as (
select				*,
					sum(Value_) over (
						partition by GVKey
						order by AsOfDate
						rows between 3 preceding and current row) as ValueTTM

from				free_cash_flow
)


, free_cash_flow1 as (
select				GVKey,
					AsOfDate,
					'FREECASHFLOWGROWTHQ' as ItemName,
					Value_ / lag(Value_, 4) over (
							partition by GVKey
							order by AsOfDate) - 1 as Value_
from				free_cash_flow0
)


, free_cash_flow2 as (
select				GVKey,
					AsOfDate,
					'FREECASHFLOWGROWTHTTM' as ItemName,
					ValueTTM / lag(ValueTTM, 4) over (
							partition by GVKey
							order by AsOfDate) - 1 as Value_
from				free_cash_flow0
)

-- ######  STACK AND WRITE  #####################################################

, stacked_data as (
select * from net_income_data_final0
union
select * from net_income_data_final1
union
select * from net_income_data_final2
union
select * from net_income_data_final3
union
select * from operating_income_1
union
select * from operating_income_2
union
select * from operating_income_3
union
select * from operating_income_4
union
select * from cash_and_marketable_securities_data1
union
select * from sales_data_final1
union
select * from sales_data_final2
union
select * from sales_data_final3
union
select * from sales_data_final4
union
select * from prof_assets_final
union
select * from free_cash_flow
union
select * from free_cash_flow1
union
select * from free_cash_flow2

)


insert into ram.dbo.ram_compustat_accounting_derived
select * from stacked_data
