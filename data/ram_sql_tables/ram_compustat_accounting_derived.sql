/*
This is generally the table that should be called by an API
to get formatted values, which include the following:

[Net Income]
* NETINCOMEQ
* NETINCOMETTM
* NETINCOMEGROWTHQ
* NETINCOMEGROWTHTTM

[Operating Income - Almost EBITDA]
* OPERATINGINCOMEQ
* OPERATINGINCOMETTM
* OPERATINGINCOMEGROWTHQ
* OPERATINGINCOMEGROWTHTTM

[EBIT] - NOTE: This is SALES - COGS - SGnA - Dep/Amortization
* EBITQ
* EBITTTM
* EBITGROWTHQ
* EBITGROWTHTTM

[Sales]
* SALESQ
* SALESTTM
* SALESGROWTHQ
* SALESGROWTHTTM

[EPS]
* ADJEPSQ
* ADJEPSTTM
* ADJEPSGROWTHQ
* ADJEPSGROWTHTTM

[Cash]
* FREECASHFLOWQ
* FREECASHFLOWTTM
* FREECASHFLOWGROWTHQ
* FREECASHFLOWGROWTHTTM
* X_CASHANDSECURITIES

[OTHER]
* X_GROSSMARGINQ: Cannot confirm COGS with Bloomberg
* X_GROSSMARGINTTM: Cannot confirm COGS with Bloomberg
* X_GROSSPROFASSET
* ASSETS
* BOOKVALUE

[Debt]
* SHORTLONGDEBT

*/

use ram;

-- ######  Final Accounting Table table  #######################################

if object_id('ram.dbo.ram_compustat_accounting_derived', 'U') is not null 
	drop table ram.dbo.ram_compustat_accounting_derived


create table	ram.dbo.ram_compustat_accounting_derived (
		GVKey int,
		AsOfDate smalldatetime,
		ItemName varchar(30),
		Value_ float
		primary key (GVKey, ItemName, AsOfDate)
)


-- ######  DATES  ##############################################################

; with unique_gvkeys_dates as (
select distinct		GVKey, QuarterEndDate, ReportDate, FiscalQuarter 
from				ram.dbo.ram_compustat_accounting
)


-- ######  EPS DIVISOR  ########################################################

, eps_divisor as (
select distinct GVKey, DATADATE, AJEXQ as EpsDivisor from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
union
select distinct GVKey, DATADATE, AJEXQ as EpsDivisor from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
)


-- ######  COMMON FIELDS   ######################################################
--   Combine Quarterly and Annual tables, handle missing quarterly data

--   BALANCE SHEET   ---
, assets_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 37		-- ASSETS / QUARTERLY

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 204
		and			D2.Item = 58		-- ASSETS / ANNUAL
)


, liabilities_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 176		-- TOTAL LIABILTIIES / QUARTERLY

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 204
		and			D2.Item = 460		-- TOTAL LIABILTIIES / ANNUAL
)


, intangibles_data_0 as (
select				T.*,
					coalesce(D1.Value_, 0) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 204
		and			D1.Item = 343		-- Intangibles / ANNUAL
)


, cash_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_, 0) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 54		-- Cash and Short Term Securities

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 204
		and			D2.Item = 104		-- Cash and Short Term Securities / ANNUAL
)


, debt_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_, 0) as Value_

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 81		-- DLTTQ / QUARTERLY TABLE

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 204
		and			D2.Item = 183		-- DLTT / ANNUAL TABLE
)


--   INCOME STATEMENT   ----
--   Included are Trailing Twelve Month values (TTM)

, sales_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as ValueQ,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as ValueTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 288		-- Sales / QUARTERLY TABLE

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 205
		and			D2.Item = 219		-- Sales / ANNUAL TABLE
)


, cogs_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as ValueQ,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as ValueTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 65		-- COGS / QUARTERLY TABLE

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 204
		and			D2.Item = 128		-- COGS / ANNUAL TABLE
)


, sgna_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as ValueQ,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as ValueTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 403		-- SGnA / QUARTERLY TABLE

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 205
		and			D2.Item = 423		-- SGnA / ANNUAL TABLE
)


, depamort_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as ValueQ,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as ValueTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 85		-- DPAMOR / QUARTERLY TABLE

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 204
		and			D2.Item = 189		-- SGnA / ANNUAL TABLE

)


, operating_income_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_ / 4) as ValueQ,
					sum(coalesce(D1.Value_, D2.Value_ / 4)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row) as ValueTTM

from				unique_gvkeys_dates T

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 196		-- OIBDPQ / Quarterly Table

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 205
		and			D2.Item = 51		-- OIBDP / Annual Table
)

-- ######  EPS  #################################################################

, eps_data_0 as (
select				T.*,
					coalesce(D1.Value_, D2.Value_) / isnull(E.EpsDivisor, 1) as ValueQ,
					coalesce(sum(coalesce(D1.Value_, D2.Value_) / isnull(E.EpsDivisor, 1)) over (
						partition by T.GVKey 
						order by T.QuarterEndDate
						rows between 3 preceding and current row), D3.Value_, D4.Value_) as ValueTTM

from				unique_gvkeys_dates T

	left join		eps_divisor E
		on			T.GVKey = E.GVKEY
		and			T.QuarterEndDate = E.DATADATE

	left join		ram.dbo.ram_compustat_accounting D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate
		and			D1.Group_ = 218
		and			D1.Item = 101		-- EPSFXQ / Quarterly Table
				
	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 218
		and			D2.Item = 100		-- EPSFIQ / Quarterly Table

	-- ANNUAL DATA
	left join		ram.dbo.ram_compustat_accounting D3
		on			T.GVKey = D3.GVKey
		and			T.QuarterEndDate = D3.QuarterEndDate
		and			D3.Group_ = 204
		and			D3.Item = 241		-- EPSFX / Annual Table
				
	left join		ram.dbo.ram_compustat_accounting D4
		on			T.GVKey = D4.GVKey
		and			T.QuarterEndDate = D4.QuarterEndDate
		and			D4.Group_ = 204
		and			D4.Item = 240		-- EPSFI / Annual Table
)


, eps_final_1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'ADJEPSQ' as ItemName,
					ValueQ as Value_
from				eps_data_0
)


, eps_final_2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'ADJEPSTTM' as ItemName,
					ValueTTM as Value_
from				eps_data_0
)


, eps_final_3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'ADJEPSGROWTHQ' as ItemName,
					ValueQ / nullif(lag(ValueQ, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				eps_data_0
)


, eps_final_4 as (
select				GVKey,
					ReportDate as AsOfDate,
					'ADJEPSGROWTHTTM' as ItemName,
					ValueTTM / nullif(lag(ValueTTM, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				eps_data_0
)


-- ######  OPERATING INCOME BEFORE DEPRECIATION  ################################
--		Use in place of EBITDA

, operating_income_1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMEQ' as ItemName,
					ValueQ as Value_
from				operating_income_data_0
)


, operating_income_2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMETTM' as ItemName,
					ValueTTM as Value_
from				operating_income_data_0
)


, operating_income_3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMEGROWTHQ' as ItemName,
					ValueQ / nullif(lag(ValueQ, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				operating_income_data_0
)


, operating_income_4 as (
select				GVKey,
					ReportDate as AsOfDate,
					'OPERATINGINCOMEGROWTHTTM' as ItemName,
					ValueTTM / nullif(lag(ValueTTM, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				operating_income_data_0
)


-- ######  CASH AND MARKETABLE SECURITIES  ###########################################
--- NOT EVEN CLOSE TO CORRECT FOR `C` (Perhaps financials generally)

, cash_and_marketable_securities_data1 as (
select				T.GVKey,
					T.ReportDate as AsOfDate,
					'X_CASHANDSECURITIES' as ItemName,
					(D1.Value_ + coalesce(D2.Value_, 0)) as Value_

from				unique_gvkeys_dates T

	left join		cash_data_0 D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate

	left join		ram.dbo.ram_compustat_accounting D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
		and			D2.Group_ = 218
		and			D2.Item = 162		-- Long Term Investments (NOTHING IN ANNUAL TABLES)
)


-- ######  EBIT  ########################################################################

, ebit_data_0 as (
select				D1.GVKey,
					D1.ReportDate as AsOfDate,
					D1.ValueQ - D2.ValueQ - D3.ValueQ - D4.ValueQ as ebit,
					D1.ValueTTM - D2.ValueTTM - D3.ValueTTM - D4.ValueTTM as ebit_ttm

from				sales_data_0 D1

	join			cogs_data_0 D2
		on			D1.GVKey = D2.GVKey
		and			D1.QuarterEndDate = D2.QuarterEndDate

	join			sgna_data_0 D3
		on			D1.GVKey = D3.GVKey
		and			D1.QuarterEndDate = D3.QuarterEndDate

	join			depamort_data_0 D4
		on			D1.GVKey = D4.GVKey
		and			D1.QuarterEndDate = D4.QuarterEndDate
)


, ebit_final_1 as (
select				GVKey,
					AsOfDate,
					'EBITQ' as ItemName,
					ebit as Value_
from				ebit_data_0
)


, ebit_final_2 as (
select				GVKey,
					AsOfDate,
					'EBITTTM' as ItemName,
					ebit_ttm as Value_
from				ebit_data_0
)


, ebit_final_3 as (
select				GVKey,
					AsOfDate,
					'EBITGROWTHQ' as ItemName,
					ebit / nullif(lag(ebit, 4) over (
						partition by GVKey
						order by AsOfDate), 0) - 1 as Value_
from				ebit_data_0
)


, ebit_final_4 as (
select				GVKey,
					AsOfDate,
					'EBITGROWTHTTM' as ItemName,
					ebit_ttm / nullif(lag(ebit_ttm, 4) over (
						partition by GVKey
						order by AsOfDate), 0) - 1 as Value_
from				ebit_data_0
)


-- ######  NET INCOME  ###############################################################

, net_income_data_0 as (
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


, net_income_data_final_1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMEQ' as ItemName,
					net_income as Value_
from				net_income_data_0
)


, net_income_data_final_2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMETTM' as ItemName,
					net_income_ttm as Value_
from				net_income_data_0
)


, net_income_data_final_3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMEGROWTHQ' as ItemName,
					net_income /  nullif(lag(net_income, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				net_income_data_0
)


, net_income_data_final_4 as (
select				GVKey,
					ReportDate as AsOfDate,
					'NETINCOMEGROWTHTTM' as ItemName,
					net_income_ttm /  nullif(lag(net_income_ttm, 4) over (
							partition by GVKey
							order by QuarterEndDate), 0) - 1 as Value_
from				net_income_data_0
)


-- ######  ASSETS  ###############################################################

, assets_data_final_1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'ASSETS' as ItemName,
					Value_
from				assets_data_0
)

-- ######  SALES  ###############################################################

, sales_data_final_1 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESQ' as ItemName,
					ValueQ as Value_
from				sales_data_0
)


, sales_data_final_2 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESTTM' as ItemName,
					ValueTTM as Value_
from				sales_data_0
)


, sales_data_final_3 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESGROWTHQ' as ItemName,
					ValueQ / nullif(lag(ValueQ, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				sales_data_0
)


, sales_data_final_4 as (
select				GVKey,
					ReportDate as AsOfDate,
					'SALESGROWTHTTM' as ItemName,
					ValueTTM / nullif(lag(ValueTTM, 4) over (
						partition by GVKey
						order by QuarterEndDate), 0) - 1 as Value_
from				sales_data_0
)


-- ######  SHORT AND LONG DEBT  #####################################################

, short_long_debt as (
select				T.GVKey,
					T.ReportDate as AsOfDate,
					'SHORTLONGDEBT' as ItemName,
					D1.Value_ + D2.Value_ as Value_

from				unique_gvkeys_dates T

	left join		liabilities_data_0 D1
		on			T.GVKey = D1.GVKey
		and			T.QuarterEndDate = D1.QuarterEndDate

	left join		debt_data_0 D2
		on			T.GVKey = D2.GVKey
		and			T.QuarterEndDate = D2.QuarterEndDate
)


-- ######  PROFIT TO ASSETS  #####################################################

, prof_assets_final as (
select				D1.GVKey,
					D1.ReportDate as AsOfDate,
					'X_GROSSPROFASSET' as ItemName,
					(D1.ValueTTM - D2.ValueTTM) / nullif(D3.Value_, 0) as Value_

from				sales_data_0 D1

	join			cogs_data_0 D2
		on			D1.GVKey = D2.GVKey
		and			D1.QuarterEndDate = D2.QuarterEndDate

	join			assets_data_0 D3
		on			D1.GVKey = D3.GVKey
		and			D1.QuarterEndDate = D3.QuarterEndDate
)


-- ######  BOOK VALUE  #####################################################

, bookvalue_final as (
select				D1.GVKey,
					D1.ReportDate as AsOfDate,
					'BOOKVALUE' as ItemName,
					D1.Value_ - D2.Value_ - D3.Value_ as Value_
					
from				assets_data_0 D1

	join			liabilities_data_0 D2
		on			D1.GVKey = D2.GVKey
		and			D1.QuarterEndDate = D2.QuarterEndDate

	join			intangibles_data_0 D3
		on			D1.GVKey = D3.GVKey
		and			D1.QuarterEndDate = D3.QuarterEndDate
)


-- ######  FREE CASH FLOW  #####################################################

, free_cash_flow as (
select				T.GVKey,
					T.ReportDate as AsOfDate,
					'FREECASHFLOWQ' as ItemName,
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
select				GVKey,
					AsOfDate,
					'FREECASHFLOWTTM' as ItemName,
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
					Value_ /  nullif(lag(Value_, 4) over (
						partition by GVKey
						order by AsOfDate), 0) - 1 as Value_
from				free_cash_flow
)


, free_cash_flow2 as (
select				GVKey,
					AsOfDate,
					'FREECASHFLOWGROWTHTTM' as ItemName,
					ValueTTM / nullif(lag(ValueTTM, 4) over (
						partition by GVKey
						order by AsOfDate), 0) - 1 as Value_
from				free_cash_flow0
)


-- ######  Gross Margin  #####################################################

, gross_margin_data_0 as (

select				D1.GVKey,
					D1.ReportDate,
					D1.ValueQ as SalesQ,
					D1.ValueTTM as SalesTTM,
					D2.ValueQ as CogsQ,
					D2.ValueTTM as CogsTTM

from				sales_data_0 D1

	join			cogs_data_0 D2
		on			D1.GVKey = D2.GVKey
		and			D1.QuarterEndDate = D2.QuarterEndDate
)


, gross_margin_final_1 as (
select			GVKey,
				ReportDate as AsOfDate,
				'X_GROSSMARGINQ' as ItemName,
				(SALESQ - COGSQ) / nullif(SALESQ, 0) as Value_
from			gross_margin_data_0
)


, gross_margin_final_2 as (
select			GVKey,
				ReportDate as AsOfDate,
				'X_GROSSMARGINTTM' as ItemName,
				(SALESTTM - COGSTTM) / nullif(SALESTTM, 0) as Value_
from			gross_margin_data_0
)


-- ######  STACK AND WRITE  #####################################################

, stacked_data as (
select * from net_income_data_final_1
union
select * from net_income_data_final_2
union
select * from net_income_data_final_3
union
select * from net_income_data_final_4
union
select * from operating_income_1
union
select * from operating_income_2
union
select * from operating_income_3
union
select * from operating_income_4
union
select * from gross_margin_final_1
union
select * from gross_margin_final_2
union
select * from cash_and_marketable_securities_data1
union
select * from sales_data_final_1
union
select * from sales_data_final_2
union
select * from sales_data_final_3
union
select * from sales_data_final_4
union
select * from prof_assets_final
union
select * from free_cash_flow
union
select * from free_cash_flow0
union
select * from free_cash_flow1
union
select * from free_cash_flow2
union
select * from short_long_debt
union
select * from ebit_final_1
union
select * from ebit_final_2
union
select * from ebit_final_3
union
select * from ebit_final_4
union
select * from assets_data_final_1
union
select * from bookvalue_final
union
select * from eps_final_1
union
select * from eps_final_2
union
select * from eps_final_3
union
select * from eps_final_4
)


insert into ram.dbo.ram_compustat_accounting_derived
select * from stacked_data
