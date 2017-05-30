
set NOCOUNT on;

-- ~~~~~~ Quarterly Data Items ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- NOTE: The hard-coded items must also be harded further downstream in PIVOT

declare @items table (Item int, ItemCode varchar(10));
insert @items(Item, ItemCode) values (288, 'SALEQ'),  (184, 'NIQ'),
	(101, 'EPSFXQ'), (100, 'EPSFIQ')


-- ~~~~~~ Quarter End Dates with GVKeys ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

; with quarter_dates as (
select distinct GVKey, DATADATE from qai.dbo.CsCoIDesInd
union
select distinct GVKey, DATADATE from qai.dbo.CsICoIDesInd
)


, fye_dates as (
-- IS THIS NECESSARY?
select distinct GVKey, DATADATE as FiscalYearDate from qai.dbo.CSCoADesInd
union
select distinct GVKey, DATADATE as FiscalYearDate from qai.dbo.CSICoADesInd
)


, all_dates as (
select				D.GVKey,
					D.DATADATE,
					Y.FiscalYearDate
from				quarter_dates D
	left join		fye_dates Y
		on			D.GVKey = Y.GVKEY
		and			Y.FiscalYearDate = (select	max(a.FiscalYearDate)
										from	fye_dates a
										where	a.GVKey = D.GVKey
										and		a.FiscalYearDate <= D.DATADATE)
where				D.GVKey in (select distinct GVKey from ram.dbo.ram_earnings_report_dates)
	and				D.DATADATE >= '1990-01-01'
)


-- ~~~~~~ Quarterly Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, quarterly_data as (
select				D.GVKey,
					D.DATADATE,
					S.Item,
					S.ItemCode,
					coalesce(D1.Value_, D2.Value_, D3.DValue, D4.DValue) as Value_
from				all_dates D

	cross join		@items S

	left join		qai.dbo.CSCoIFndQ D1
		on			D.DATADATE = D1.DATADATE
		and			D.GVKey = D1.GVKey
		and			S.Item = D1.Item
		and			D1.FyrFlag = 0

	left join		qai.dbo.CSICoIFndQ D2
		on			D.DATADATE = D2.DATADATE
		and			D.GVKey = D2.GVKey
		and			S.Item = D2.Item
		and			D2.FyrFlag = 0

	-- Missing data that is derived by Compustat
	left join		(   select GVKey, DATADATE, Item, avg(DValue) as DValue from qai.dbo.CSCoIFndQ
						where Value_ is null and FyrFlag != 0
						group by GVKey, DATADATE, Item
					) D3

		on			D.DATADATE = D3.DATADATE
		and			D.GVKey = D3.GVKey
		and			S.Item = D3.Item

	left join		(   select GVKey, DATADATE, Item, avg(DValue) as DValue from qai.dbo.CSICoIFndQ
						where Value_ is null and FyrFlag != 0
						group by GVKey, DATADATE, Item
					) D4

		on			D.DATADATE = D4.DATADATE
		and			D.GVKey = D4.GVKey
		and			S.Item = D4.Item
)


, shares_adjustments as (
select distinct GVKEY, DATADATE, AJEXQ as ValueDivisor from qai.dbo.CSCoIDesInd
union
select distinct GVKEY, DATADATE, AJEXQ as ValueDivisor from qai.dbo.CSICoIDesInd
)


-- ~~~~~~ PIVOT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- HARD-CODED COLUMNS

, pivot_quarterly_data as (
select		*
from		( select GVKEY, DATADATE, Value_, ItemCode
			  from quarterly_data ) d
			pivot
			( max(Value_) for ItemCode in (SALEQ, NIQ, EPSFXQ, EPSFIQ) ) p
)


-- ~~~~~~ AGGREGATED AND HANDLED ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, aggregated_data as (
select			Q.*,

				case 
					when lag(Q.DATADATE, 7) over (
						partition by GVKey 
						order by DATADATE) is not NULL
						then 1 else 0 
				end as EightQuarterFlag,

				isnull(EPSFXQ, EPSFIQ) / isnull(A.ValueDivisor, 1) as EPS_Dil,

				sum(SALEQ) over (
					partition by GVKEY
					order by DATADATE 
					rows between 3 preceding and current row) as SALEQSum,

				sum(NIQ) over (
					partition by GVKEY
					order by DATADATE 
					rows between 3 preceding and current row) as NIQSum,

				sum(isnull(EPSFXQ, EPSFIQ) / isnull(A.ValueDivisor, 1)) over (
					partition by GVKEY
					order by DATADATE 
					rows between 3 preceding and current row) as EPS_DilSum

from			pivot_quarterly_data Q

	left join	shares_adjustments A
		on		Q.GVKEY = A.GVKEY
		and		Q.DATADATE = A.DATADATE

)



, base as (
select			GVKEY,
				DATADATE,
				EightQuarterFlag,

				-- Revenue
				SALEQ as RevenueQtr,
				lag(SALEQ, 4) over (
					partition by GVKey 
					order by DATADATE) as RevenueQtrLag,

				SALEQSum as RevenueTTM,
				lag(SALEQSum, 4) over (
					partition by GVKey 
					order by DATADATE) as RevenueTTMLag,

				-- Net Income
				NIQ as NetIncomeQtr,
				lag(NIQ, 4) over (
					partition by GVKey 
					order by DATADATE) as NetIncomeQtrLag,

				NIQSum as NetIncomeTTM,    
				lag(NIQSum, 4) over (
					partition by GVKey 
					order by DATADATE) as NetIncomeTTMLag,
        
				-- EPS
				EPS_Dil as DilEPSQtr,
				lag(EPS_Dil, 4) over (
					partition by GVKey 
					order by DATADATE) as DilEPSQtrLag,

				EPS_DilSum as DilEPSTTM,
				lag(EPS_DilSum, 4) over (
					partition by GVKey 
					order by DATADATE) as DilEPSTTMLag

from			aggregated_data
)

select			U.SecCode,
				U.ReportDate,

				B.EightQuarterFlag,
				B.RevenueQtr,
				B.RevenueQtrLag,
				B.RevenueTTM,
				B.RevenueTTMLag,
				B.NetIncomeQtr,
				B.NetIncomeQtrLag,
				B.NetIncomeTTM,
				B.NetIncomeTTMLag,
				B.DilEPSQtr,
				B.DilEPSQtrLag,
				B.DilEPSTTM,
				B.DilEPSTTMLag

from			ram.dbo.ram_earnings_report_dates U
	left join	base B
		on		B.GVKey = U.GVKey
		and		B.DATADATE = U.QuarterDate

order by U.SecCode, B.DATADATE
