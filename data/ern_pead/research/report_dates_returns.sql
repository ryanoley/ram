SET NOCOUNT ON


; with pricing_data as (
select			*,
				Lead(coalesce(AdjVwap, AdjClose), 1) over (
					partition by SecCode
					order by Date_) as EntryVwap,
				Lead(coalesce(AdjVwap, AdjClose), 3) over (
					partition by SecCode
					order by Date_) as ExitVwapTwoDay,
				Lead(coalesce(AdjVwap, AdjClose), 4) over (
					partition by SecCode
					order by Date_) as ExitVwapThreeDay
from			ram.dbo.ram_equity_pricing
)


, pricing_data_mkt as (
select			*,
				Lead(coalesce(AdjVwap, AdjClose), 1) over (
					partition by SecCode
					order by Date_) as EntryVwapMkt,
				Lead(coalesce(AdjVwap, AdjClose), 3) over (
					partition by SecCode
					order by Date_) as ExitVwapMktTwoDay,
				Lead(coalesce(AdjVwap, AdjClose), 4) over (
					partition by SecCode
					order by Date_) as ExitVwapMktThreeDay
from			ram.dbo.ram_etf_pricing
where			SecCode = 61494
)


, report_dates0 as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


select			R.SecCode,
				R.Ticker,
				R.QuarterDate,
				R.ReportDate,
				R.FiscalQuarter,
				P.ExitVwapTwoDay / P.EntryVwap - 1 as RetTwoDay,
				P.ExitVwapThreeDay / P.EntryVwap - 1 as RetThreeDay,
				M.ExitVwapMktTwoDay / M.EntryVwapMkt - 1 as HedgeRetTwoDay,
				M.ExitVwapMktThreeDay / M.EntryVwapMkt - 1 as HedgeRetThreeDay

from			report_dates0 R
	join		pricing_data P
	on			R.SecCode = P.SecCode
	and			R.FilterDate = P.Date_

	join		pricing_data_mkt M
	on			P.Date_ = M.Date_

where			R.ResearchFlag = 1
