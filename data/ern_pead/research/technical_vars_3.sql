SET NOCOUNT ON


; with all_technical_vars_univ as (
select			SecCode,
				Date_,

				NormalTradingFlag,
				OneYearTradingFlag,
				Close_,
				MarketCap,
				AvgDolVol,

				AdjClose

from			ram.dbo.ram_equity_pricing_research
)


, all_technical_vars_univ2 as (
select			SecCode,
				Date_,

				AdjClose / Max(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 62 preceding and current row) as DiscountQ,

				AdjClose / Max(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 125 preceding and current row) as DiscountS,

				AdjClose / Max(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 251 preceding and current row) as DiscountA

from			all_technical_vars_univ

where			NormalTradingFlag = 1
	and			OneYearTradingFlag = 1
	and			AvgDolVol >= 3
	and			Close_ >= 15
	and			MarketCap >= 200

)

, all_technical_vars_univ_ranked as (
select			SecCode,
				Date_ as FilterDate,

				DiscountQ,
				percent_rank() over (
					partition by Date_
					order by DiscountQ) as DiscountQ_Rank,
				DiscountS,

				percent_rank() over (
					partition by Date_
					order by DiscountS) as DiscountS_Rank,
				DiscountA,

				percent_rank() over (
					partition by Date_
					order by DiscountA) as DiscountA_Rank

from			all_technical_vars_univ2
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
