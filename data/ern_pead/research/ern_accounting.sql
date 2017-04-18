
-- ~~~~~~ Quarterly/Anual Data Items ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- NOTE: The hard-coded items must also be harded further downstream in PIVOT

declare @items table (Item int, ItemCode varchar(10));
insert @items(Item, ItemCode) values (288, 'SALEQ'), 
	(65, 'COGSQ'), (37, 'ATQ'), 
	(176, 'LTQ'), (54, 'CHEQ'), 
	(81, 'DLTTQ'), (80, 'DLCQ'), 
	(196, 'OIBDPQ'), (403, 'XSGAQ');


declare @itemsA table (Item int, ItemCode varchar(10));
insert @itemsA(Item, ItemCode) values (219, 'SALE'),
	(128, 'COGS'),  (58, 'AT'),
	(460, 'LT'), (343, 'INTAN'),
	(104, 'CHE'), (183, 'DLTT'),
	(176, 'DLC'), (51, 'OIBDP'),
	(50, 'OIADP'), (189, 'DP'),
	(423, 'XSGA'), (46, 'OANCF'),
	(26, 'NI')


-- ~~~~~~ Report Dates with GVKeys ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

; with quarter_dates as (
select distinct GVKey, DATADATE from qai.dbo.CsCoIDesInd
union
select distinct GVKey, DATADATE from qai.dbo.CsICoIDesInd
)


, fye_dates as (
select distinct GVKey, DATADATE as FiscalYearDate from qai.dbo.CSCoADesInd
union
select distinct GVKey, DATADATE as FiscalYearDate from qai.dbo.CSICoADesInd
)


, report_dates as (
select				D.SecCode,
					D.QuarterDate,
					D.FilterDate,
					D.ReportDate,
					G.GVKey
from				ram.dbo.ram_earnings_report_dates D
	join			ram.dbo.ram_master_ids I
		on			D.SecCode = I.SecCode
		and			D.ReportDate between I.StartDate and I.EndDate

	join			ram.dbo.ram_idccode_to_gvkey_map G
		on			I.IdcCode = G.IdcCode
		and			D.ReportDate between G.StartDate and G.EndDate

where				D.ResearchFlag = 1
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
where				D.GVKey in (select distinct GVKey from report_dates)
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


-- ~~~~~~ Annual Data ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, stacked_annual_data as (
-- INDFMT = 9 is a weird industry format and is removed purposefully
select * from qai.dbo.CSCoAFnd1 where INDFMT = 5
union
select * from qai.dbo.CSCoAFnd2 where INDFMT = 5
)


, stacked_annual_data_inactive as (
select * from qai.dbo.CSICoAFnd1 where INDFMT = 5
union
select * from qai.dbo.CSICoAFnd2 where INDFMT = 5
)


, annual_data as (
select				D.GVKey,
					D.DATADATE,
					S.Item,
					S.ItemCode,
					coalesce(D1.DValue, D2.DValue, D3.Value_, D4.Value_) as Value_
from				all_dates D

	cross join		@itemsA S

	-- Multiple restated values that are averaged together
	left join		( select GVKey, DATADATE, Item, max(Value_) as DValue from stacked_annual_data
					  where DATAFMT = 2
					  group by GVKey, DATADATE, Item ) D1

		on			D.FiscalYearDate = D1.DATADATE
		and			D.GVKey = D1.GVKey
		and			S.Item = D1.Item

	left join		( select GVKey, DATADATE, Item, max(Value_) as DValue from stacked_annual_data_inactive
					  where DATAFMT = 2
					  group by GVKey, DATADATE, Item ) D2

		on			D.FiscalYearDate = D2.DATADATE
		and			D.GVKey = D2.GVKey
		and			S.Item = D2.Item

	left join		stacked_annual_data D3
		on			D.FiscalYearDate = D3.DATADATE
		and			D.GVKey = D3.GVKey
		and			S.Item = D3.Item
		and			D3.DATAFMT = 1

	left join		stacked_annual_data_inactive D4
		on			D.FiscalYearDate = D4.DATADATE
		and			D.GVKey = D4.GVKey
		and			S.Item = D4.Item
		and			D4.DATAFMT = 1

)


-- ~~~~~~ PIVOT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- HARD-CODED COLUMNS
, pivot_quarterly_data as (
select		*
from		( select GVKEY, DATADATE, Value_, ItemCode
			  from quarterly_data ) d
			pivot
			( max(Value_) for ItemCode in (SALEQ, COGSQ, ATQ, LTQ, CHEQ, DLTTQ, DLCQ, OIBDPQ, XSGAQ) ) p
)


, pivot_annual_data as (
select		*
from		( select GVKEY, DATADATE, Value_, ItemCode
			  from annual_data ) d
			pivot
			( max(Value_) for ItemCode in (SALE, COGS, [AT], LT, INTAN, CHE, DLTT, DLC, 
										   OIBDP, OIADP, DP, XSGA, OANCF, NI) ) p
)


-- ~~~~~~ AGGREGATED AND HANDLED ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
, aggregated_data as (
select			Q.GVKEY,
				Q.DATADATE,
				A.[AT],

				coalesce(Q.ATQ, A.[AT], 0) - coalesce(Q.LTQ, A.[LT], 0) - isnull(A.INTAN, 0) as BOOKVALUE,
			    A.OANCF / nullif(A.NI, 0) as CashFlowNetIncome,

				isnull(Q.CHEQ, A.CHE) as CASH,
				coalesce(Q.DLTTQ + Q.DLCQ, A.DLTT + A.DLC, 0) as DEBT,

				case
					when count(Q.SALEQ) over (
						partition by Q.GVKey
						order by Q.DATADATE
						rows between 3 preceding and current row) = 4 -- Null Value Check
					then sum(Q.SALEQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row)
					else A.SALE end as SALES,

				case
					when Count(Q.COGSQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row) = 4 -- Null Value Check
					then Sum(Q.COGSQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row)
					else A.COGS end as COGS,

				case
					when Count(Q.OIBDPQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row) = 4 -- Null Value Check
					then Sum(Q.OIBDPQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row)
					else A.OIBDP end as EbitdaPrimary,

				case
					when Count(Q.SALEQ - Q.COGSQ - Q.XSGAQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row) = 4 -- Null Value Check
					then Sum(Q.SALEQ - Q.COGSQ - Q.XSGAQ) over (
						partition by Q.GVKey
						order by Q.DATADATE 
						rows between 3 preceding and current row)
					else A.SALE - A.COGS - A.XSGA end as EbitdaComp2,

				A.OIADP + A.DP as EbitdaComp

from			pivot_quarterly_data Q

	left join	pivot_annual_data A
		on		Q.GVKEY = A.GVKEY
		and		Q.DATADATE = A.DATADATE

)


, aggregated_data2 as (
select				A.GVKEY,
					lead(A.DATADATE, 1) over (
						partition by GVKey
						order by DATADATE) as MergeQuarterDate,
					(A.SALES - A.COGS) / nullif(A.[AT], 0) as ProfAsset,
				    A.BOOKVALUE,
					A.DEBT,
					A.CASH,
					coalesce(A.EbitdaPrimary, 
							 A.EbitdaComp, 
							 A.EbitdaComp2, 
							 lag(coalesce(A.EbitdaPrimary, A.EbitdaComp, A.EbitdaComp2), 1) over (
								partition by A.GVKey 
						        order by A.DATADATE)) as Ebitda,
					A.CashFlowNetIncome

from				aggregated_data A

)


select				D.SecCode,
					D.ReportDate,
					A.ProfAsset,
					A.Ebitda,
					A.CashFlowNetIncome,
					P.MarketCap / A.BOOKVALUE as PriceBook,
					P.MarketCap + A.DEBT - A.CASH as EnterpriseValue

from				aggregated_data2 A

	join			report_dates D
		on			A.GVKEY = D.GVKey
		and			A.MergeQuarterDate = D.QuarterDate

	join			ram.dbo.ram_equity_pricing P
		on			D.SecCode = P.SecCode
		and			D.FilterDate = P.Date_

where				A.GVKey = 1004

