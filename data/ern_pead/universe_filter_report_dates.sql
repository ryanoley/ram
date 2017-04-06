SET NOCOUNT ON

-------------------------------------------------------------
-- Create temp tables

if object_id('ram.dbo.temp_univ_report_dates', 'U') is not null 
	drop table ram.dbo.temp_univ_report_dates


create table	ram.dbo.temp_univ_report_dates (
		SecCode int,
		Ticker varchar(6),
		QuarterDate smalldatetime,
		ReportDate  smalldatetime,
		FilterDate smalldatetime,
		PreviousReportDate smalldatetime,
		FiscalQuarter int,
		ResearchFlag int,
		LiveFlag int
		primary key (SecCode, ReportDate)
)


if object_id('tempdb..#dates_table', 'U') is not null 
	drop table #dates_table

create table #dates_table
(
    CalendarDate datetime,
	FilterDate datetime
)


if $(trade) = 1
	insert into #dates_table select CalendarDate, Tm2 as FilterDate from ram.dbo.ram_trading_dates

if $(trade) = 2
	insert into #dates_table select CalendarDate, T0 as FilterDate from ram.dbo.ram_trading_dates


-------------------------------------------------------------

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
				-- Five total announcements for research,
				-- four required at time of trading
				sum(NormalFlag1) over (
					partition by GVKey
					order by QuarterDate
					rows between 4 preceding and current row) as NormalFlag1Sum,
				sum(NormalFlag2) over (
					partition by GVKey
					order by QuarterDate
					rows between 4 preceding and current row) as NormalFlag2Sum,
				sum(NormalFlag1) over (
					partition by GVKey
					order by QuarterDate
					rows between 3 preceding and current row) as NormalFlag1SumLive,
				sum(NormalFlag2) over (
					partition by GVKey
					order by QuarterDate
					rows between 3 preceding and current row) as NormalFlag2SumLive
from			report_dates2
)


, report_dates4 as (
select			R.GVKey,
				R.QuarterDate,
				R.ReportDate,
				R.FiscalQuarter,
				R.ReportDateLag as PreviousReportDate,
				T.FilterDate,
				case
					when R.NormalFlag1Sum = 5 and R.NormalFlag2Sum = 5
					then 1 else 0 end as ResearchFlag,
				case
					when R.NormalFlag1SumLive = 4 and R.NormalFlag2SumLive = 4
					then 1 else 0 end as LiveFlag
from			report_dates3 R
join			#dates_table T
	on			T.CalendarDate = R.ReportDate
where			NormalFlag1SumLive = 4
	and			NormalFlag2SumLive = 4
)


, report_dates5 as (
select			D.*,
				P.*,
				X.Ticker,
				-- For some duplicates that make it through GVKey map
				ROW_NUMBER() over (
					partition by X.Issuer, D.ReportDate
					order by P.AvgDolVol desc, P.SecCode) as rank_val

from			ram.dbo.ram_equity_pricing P

	join		ram.dbo.ram_gvkey_map M
		on		P.IdcCode = M.IdcCode
		and		P.Date_ between M.StartDate and M.EndDate

	join		report_dates4 D
		on		M.GVKey = D.GVKey
		and		P.Date_ = D.FilterDate

	join		ram.dbo.ram_master_ids X
		on		P.SecCode = X.SecCode
		and		P.Date_ between X.StartDate and X.EndDate

where			P.Close_ >= 15
	and			P.AvgDolVol >= 3
	and			P.MarketCap >= 200
	and			P.NormalTradingFlag = 1
)


insert into		ram.dbo.temp_univ_report_dates
select			SecCode,
				Ticker,
				QuarterDate,
				ReportDate,
				FilterDate,
				PreviousReportDate,
				FiscalQuarter,
				ResearchFlag,
				LiveFlag
from			report_dates5
where			rank_val = 1


drop table #dates_table


if $(trade) = 1
begin
	if object_id('ram.dbo.ram_earnings_report_dates', 'U') is not null 
		drop table ram.dbo.ram_earnings_report_dates
	exec sp_rename 'ram.dbo.temp_univ_report_dates', 'ram_earnings_report_dates'
end

if $(trade) = 2
begin
	if object_id('ram.dbo.ram_pead_report_dates', 'U') is not null 
		drop table ram.dbo.ram_pead_report_dates
	exec sp_rename 'ram.dbo.temp_univ_report_dates', 'ram_pead_report_dates'
end
