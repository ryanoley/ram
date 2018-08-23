SET NOCOUNT ON


; with all_technical_vars_univ as (
select			*,
				-- CONSTRUCT VARIABLES HERE
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 4 preceding and current row) as MA5,
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as MA10,
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 19 preceding and current row) as MA20,
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as MA60

from			ram.dbo.ram_equity_pricing_research
where			SecCode in (select distinct SecCode from ram.dbo.ram_master_ids)
)


, all_technical_vars_univ2 as (
select			SecCode,
				Date_,

				AdjClose / MA5 as PRMA5,
				AdjClose / MA10 as PRMA10,
				AdjClose / MA20 as PRMA20,
				AdjClose / MA60 as PRMA60,

				MA5 / MA10 as PRMA5_10,
				MA5 / MA20 as PRMA5_20,
				MA5 / MA60 as PRMA5_60,

				MA10 / MA20 as PRMA10_20,
				MA10 / MA60 as PRMA10_60,
				MA20 / MA60 as PRMA20_60

from			all_technical_vars_univ

-- Universe filter happens after grouped functionality from all_technical_vars_univ above
where			NormalTradingFlag = 1
	and			AvgDolVol >= 3
	and			Close_ >= 15
	and			MarketCap >= 200

)


, all_technical_vars_univ_ranked as (
select			SecCode,
				Date_ as FilterDate,

				PRMA5,
				percent_rank() over (
					partition by Date_
					order by PRMA5) as PRMA5_Rank,
				PRMA10,
				percent_rank() over (
					partition by Date_
					order by PRMA10) as PRMA10_Rank,
				PRMA20,
				percent_rank() over (
					partition by Date_
					order by PRMA20) as PRMA20_Rank,
				PRMA60,
				percent_rank() over (
					partition by Date_
					order by PRMA60) as PRMA60_Rank,

				PRMA5_10,
				percent_rank() over (
					partition by Date_
					order by PRMA5_10) as PRMA5_10_Rank,
				PRMA5_20,
				percent_rank() over (
					partition by Date_
					order by PRMA5_20) as PRMA5_20_Rank,
				PRMA5_60,
				percent_rank() over (
					partition by Date_
					order by PRMA5_60) as PRMA5_60_Rank,

				PRMA10_20,
				percent_rank() over (
					partition by Date_
					order by PRMA10_20) as PRMA10_20_Rank,
				PRMA10_60,
				percent_rank() over (
					partition by Date_
					order by PRMA10_60) as PRMA10_60_Rank,
				PRMA20_60,
				percent_rank() over (
					partition by Date_
					order by PRMA20_60) as PRMA20_60_Rank

from			all_technical_vars_univ2

)


, hedge_technical_vars1 as (
select			Date_,
				AdjClose,

				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 4 preceding and current row) as MA5,
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as MA10,
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 19 preceding and current row) as MA20,
				avg(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as MA60

from			ram.dbo.ram_etf_pricing
where			SecCode = 61494
)


, hedge_technical_vars2 as (
select			Date_ as FilterDate,

				AdjClose / MA5 as PRMA5,
				AdjClose / MA10 as PRMA10,
				AdjClose / MA20 as PRMA20,
				AdjClose / MA60 as PRMA60,

				MA5 / MA10 as PRMA5_10,
				MA5 / MA20 as PRMA5_20,
				MA5 / MA60 as PRMA5_60,

				MA10 / MA20 as PRMA10_20,
				MA10 / MA60 as PRMA10_60,
				MA20 / MA60 as PRMA20_60
from			hedge_technical_vars1
)


, report_dates0 as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


select			A.ReportDate,
				B.SecCode,

				B.PRMA5 - C.PRMA5 as PRMA5,
				B.PRMA5_Rank,
				B.PRMA10 - C.PRMA10 as PRMA10,
				B.PRMA10_Rank,
				B.PRMA20 - C.PRMA20 as PRMA20,
				B.PRMA20_Rank,
				B.PRMA60 - C.PRMA60 as PRMA60,
				B.PRMA60_Rank,

				B.PRMA5_10 - C.PRMA5_10 as PRMA5_10,
				B.PRMA5_10_Rank,
				B.PRMA5_20 - C.PRMA5_20 as PRMA5_20,
				B.PRMA5_20_Rank,
				B.PRMA5_60 - C.PRMA5_60 as PRMA5_60,
				B.PRMA5_60_Rank,

				B.PRMA10_20 - C.PRMA10_20 as PRMA10_20,
				B.PRMA10_20_Rank,
				B.PRMA10_60 - C.PRMA10_60 as PRMA10_60,
				B.PRMA10_60_Rank,
				B.PRMA20_60 - C.PRMA20_60 as PRMA20_60,
				B.PRMA20_60_Rank

from			report_dates0 A
	join		all_technical_vars_univ_ranked B
		on		A.SecCode = B.SecCode
		and		A.FilterDate = B.FilterDate
	join		hedge_technical_vars2 C
		on		A.FilterDate = C.FilterDate
where			A.ResearchFlag = 1
	and			A.AvgDolVol >= 3
	and			A.MarketCap >= 200
	and			A.Close_ >= 15