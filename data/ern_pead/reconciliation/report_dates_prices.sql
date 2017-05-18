SET NOCOUNT ON

-------------------------------------------------------------
-- Select table to run
if object_id('tempdb..#dates_table', 'U') is not null 
	drop table #dates_table

create table #dates_table
(
    CalendarDate datetime,
	EvalDate datetime
)

if $(trade) = 1
	insert into #dates_table select CalendarDate, Tm2 as EvalDate from ram.dbo.ram_trading_dates

if $(trade) = 2
	insert into #dates_table select CalendarDate, T0 as EvalDate from ram.dbo.ram_trading_dates


; with report_dates1 as (
select			*,
				Lag(ReportDate, 1) over (
					partition by GVKey
					order by ReportDate) as ReportDateLag,
				Lag(QuarterDate, 1) over (
					partition by GVKey
					order by QuarterDate) as QuarterDateLag 
from			ram.dbo.ram_equity_report_dates
)


, report_dates2 as (
select			*,
				case 
					when DateDiff(day, ReportDateLag, ReportDate) <= 180
					then 1 else 0 end as NormalFlag1,
				case 
					when DateDiff(day, QuarterDateLag, QuarterDate) <= 180
					then 1 else 0 end as NormalFlag2
from report_dates1
)


, report_dates3 as (
select			*,
				-- Five total announcements: current quarter plus 
				-- four required at time of trading
				sum(NormalFlag1) over (
					partition by GVKey
					order by QuarterDate
					rows between 4 preceding and current row) as NormalFlag1Sum,
				sum(NormalFlag2) over (
					partition by GVKey
					order by QuarterDate
					rows between 4 preceding and current row) as NormalFlag2Sum
from			report_dates2
)


, report_dates4 as (
select			R.GVKey,
				R.QuarterDate,
				R.ReportDate,
				R.FiscalQuarter,
				T.EvalDate
from			report_dates3 R
join			#dates_table T
	on			T.CalendarDate = R.ReportDate
where			NormalFlag1Sum = 5
	and			NormalFlag2Sum = 5
	and			year(ReportDate) = $(year)
	and			datepart(quarter, ReportDate) = $(quarter)
)


, pricing_data as (
select			*,
				Lead(Volume * Vwap, 1) over (
					partition by SecCode
					order by Date_) as EntryDolVol,
				Lead(Vwap, 1) over (
					partition by SecCode
					order by Date_) as EntryVwap,
				Lead(Vwap, 3) over (
					partition by SecCode
					order by Date_) as ExitVwapTwoDay,
				Lead(Vwap, 4) over (
					partition by SecCode
					order by Date_) as ExitVwapThreeDay

from			ram.dbo.ram_equity_pricing
where			Date_ >= dateadd(day, -180, getdate())
)


, pricing_data_mkt as (
select			*,
				Lead(Vwap, 1) over (
					partition by SecCode
					order by Date_) as EntryVwapMkt,
				Lead(Vwap, 3) over (
					partition by SecCode
					order by Date_) as ExitVwapMktTwoDay,
				Lead(Vwap, 4) over (
					partition by SecCode
					order by Date_) as ExitVwapMktThreeDay

from			ram.dbo.ram_etf_pricing
where			Date_ >= dateadd(day, -180, getdate())
	and			SecCode = 61494
)


, report_dates5 as (
select			D.*,
				P.*,
				X.Ticker,
				MKT.EntryVwapMkt,
				MKT.ExitVwapMktTwoDay,
				MKT.ExitVwapMktThreeDay,
				-- For some duplicates that make it through GVKey map
				ROW_NUMBER() over (
					partition by X.Issuer
					order by P.AvgDolVol desc, P.SecCode) as rank_val

from			pricing_data P

	join		ram.dbo.ram_idccode_to_gvkey_map M
		on		P.IdcCode = M.IdcCode
		and		P.Date_ between M.StartDate and M.EndDate

	join		report_dates4 D
		on		M.GVKey = D.GVKey
		and		P.Date_ = D.EvalDate

	join		ram.dbo.ram_master_ids X
		on		P.SecCode = X.SecCode
		and		P.Date_ between X.StartDate and X.EndDate

	join		pricing_data_mkt MKT
		on		P.Date_ = MKT.Date_

where			P.Close_ >= 15
	and			P.AvgDolVol >= 3
	and			P.MarketCap >= 200
	and			P.NormalTradingFlag = 1
	and			P.OneYearTradingFlag = 1
)


select			SecCode,
				Ticker,
				ReportDate,
				FiscalQuarter,
				EvalDate,
				EntryDolVol,
				EntryVwap,
				ExitVwapTwoDay,
				ExitVwapThreeDay,
				EntryVwapMkt,
				ExitVwapMktTwoDay,
				ExitVwapMktThreeDay
from			report_dates5
where			rank_val = 1


drop table #dates_table