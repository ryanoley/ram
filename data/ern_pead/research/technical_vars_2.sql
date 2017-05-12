SET NOCOUNT ON


; with all_technical_vars_univ as (
select			SecCode,
				Date_,

				NormalTradingFlag,
				OneYearTradingFlag,
				Close_,
				MarketCap,
				AvgDolVol,

				AdjClose,

				Lag(AdjClose, 1) over (
					partition by SecCode
					order by Date_) as LagAdjClose

from			ram.dbo.ram_equity_pricing_research
)


, all_technical_vars_univ2 as (
select			SecCode,
				Date_,

				NormalTradingFlag,
				OneYearTradingFlag,
				Close_,
				MarketCap,
				AvgDolVol,

				AdjClose,

				stdev(log(AdjClose / LagAdjClose)) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as Vol10,

				stdev(log(AdjClose / LagAdjClose)) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) as Vol30,

				stdev(log(AdjClose / LagAdjClose)) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as Vol60,

				-- MAs
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as MA10,

				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) as MA30,

				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as MA60,

				-- STDs
				stdev(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as STD10,

				stdev(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) as STD30,

				stdev(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as STD60,

				-- For RSI Calculation
				case
					when (AdjClose - LagAdjClose) > 0 
					then (AdjClose - LagAdjClose)
					else 0
				end as UpMove,

				case
					when (AdjClose - LagAdjClose) < 0 
					then (AdjClose - LagAdjClose)
					else 0
				end as DownMove

from			all_technical_vars_univ

)


, all_technical_vars_univ3 as (
select			SecCode,
				Date_,
				AdjClose,

				NormalTradingFlag,
				OneYearTradingFlag,
				Close_,
				MarketCap,
				AvgDolVol,

				Vol10,
				Vol30,
				Vol60,

				MA10,
				MA30,
				MA60,

				STD10,
				STD30,
				STD60,

				Sum(UpMove) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as SumUp10,
				Sum(UpMove) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) as SumUp30,
				Sum(UpMove) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as SumUp60,

				Sum(DownMove) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as SumDown10,
				Sum(DownMove) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) as SumDown30,
				Sum(DownMove) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as SumDown60

from			all_technical_vars_univ2

)


, all_technical_vars_univ4 as (
select			SecCode,
				Date_,

				Vol10,
				Vol30,
				Vol60,

				(AdjClose - (MA10 - 2*STD10)) / nullif(((MA10 + 2*STD10) - (MA10 - 2*STD10)), 0) as PercentB10,
				(AdjClose - (MA30 - 2*STD30)) / nullif(((MA30 + 2*STD30) - (MA30 - 2*STD30)), 0) as PercentB30,
				(AdjClose - (MA60 - 2*STD60)) / nullif(((MA60 + 2*STD60) - (MA60 - 2*STD60)), 0) as PercentB60,

				100 * SumUp10 / nullif(SumUp10 - SumDown10, 0) as RSI10,
				100 * SumUp30 / nullif(SumUp30 - SumDown30, 0) as RSI30,
				100 * SumUp60 / nullif(SumUp60 - SumDown60, 0) as RSI60
				
from			all_technical_vars_univ3
where			NormalTradingFlag = 1
	and			OneYearTradingFlag = 1
	and			AvgDolVol >= 3
	and			Close_ >= 15
	and			MarketCap >= 200

)


, all_technical_vars_univ_ranked as (
select			SecCode,
				Date_ as FilterDate,

				Vol10,
				percent_rank() over (
					partition by Date_
					order by Vol10) as Vol10_Rank,
				Vol30,
				percent_rank() over (
					partition by Date_
					order by Vol30) as Vol30_Rank,
				Vol60,
				percent_rank() over (
					partition by Date_
					order by Vol60) as Vol60_Rank,

				PercentB10,
				percent_rank() over (
					partition by Date_
					order by PercentB10) as PercentB10_Rank,
				PercentB30,
				percent_rank() over (
					partition by Date_
					order by PercentB30) as PercentB30_Rank,
				PercentB60,
				percent_rank() over (
					partition by Date_
					order by PercentB60) as PercentB60_Rank

from			all_technical_vars_univ4

)

, report_dates0 as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


select			A.ReportDate,
				B.*
from			report_dates0 A
	join		all_technical_vars_univ_ranked B
		on		A.SecCode = B.SecCode
		and		A.FilterDate = B.FilterDate

where			A.ResearchFlag = 1
