SET NOCOUNT ON


; with all_technical_vars_univ as (
select			*,

				1 - AdjClose / Max(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 62 preceding and current row) as DiscountQ,

				1 - AdjClose / Max(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 125 preceding and current row) as DiscountS,

				1 - AdjClose / Max(AdjClose) over (
					partition by SecCode
					order by Date_
					rows between 251 preceding and current row) as DiscountA

from			ram.dbo.ram_equity_pricing_research

where			SecCode in (select distinct SecCode from ram.dbo.ram_master_ids)
)


-- Counts of those at highs to adjust ranks for securities not at highs
, at_high_count as (
select			*,

				Count(Date_) over (
					partition by Date_) as AllCounts,
				
				-- Count of all those at highs
				Count(case when DiscountQ = 0 then 1 end) over (
					partition by Date_) as HighCountQ,
				Count(case when DiscountS = 0 then 1 end) over (
					partition by Date_) as HighCountS,
				Count(case when DiscountA = 0 then 1 end) over (
					partition by Date_) as HighCountA

from			all_technical_vars_univ A

)

, all_technical_vars_univ_ranked as (
select			SecCode,
				Date_ as FilterDate,

				DiscountQ,
				DiscountS,
				DiscountA,

				AllCounts,
				HighCountQ,
				HighCountS,
				HighCountA,

				-- Ranks
				Rank() over (
					partition by Date_
					order by DiscountQ) as DiscountQ_Rank,
				Rank() over (
					partition by Date_
					order by DiscountS) as DiscountS_Rank,
				Rank() over (
					partition by Date_
					order by DiscountA) as DiscountA_Rank,

				-- Percent Ranks
				percent_rank() over (
					partition by Date_
					order by DiscountQ) as DiscountQ_Rank2,

				percent_rank() over (
					partition by Date_
					order by DiscountS) as DiscountS_Rank2,

				percent_rank() over (
					partition by Date_
					order by DiscountA) as DiscountA_Rank2

from			at_high_count
where			NormalTradingFlag = 1
	and			OneYearTradingFlag = 1
	and			AvgDolVol >= 3
	and			Close_ >= 15
	and			MarketCap >= 200

)


, all_technical_vars_univ_ranked2 as (

select			SecCode,
				FilterDate,

				DiscountQ,
				DiscountS,
				DiscountA,

				DiscountQ_Rank2,
				DiscountS_Rank2,
				DiscountA_Rank2,

				-- Convert
				case 
					when DiscountQ > 0
					then Convert(int,  
						(DiscountQ_Rank - HighCountQ) / Convert(float, AllCounts - HighCountQ) * 100)
					else 0
				end as DiscountQ_Rank,

				case 
					when DiscountS > 0
					then Convert(int,  
						(DiscountS_Rank - HighCountS) / Convert(float, AllCounts - HighCountS) * 100)
					else 0
				end as DiscountS_Rank,

				case 
					when DiscountA > 0
					then Convert(int,  
						(DiscountA_Rank - HighCountA) / Convert(float, AllCounts - HighCountA) * 100)
					else 0
				end as DiscountA_Rank

from			all_technical_vars_univ_ranked

)


, report_dates0 as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


select			A.ReportDate,
				B.SecCode,
				
				DiscountQ,
				DiscountQ_Rank,
				DiscountQ_Rank2,

				DiscountS,
				DiscountS_Rank,
				DiscountS_Rank2,

				DiscountA,
				DiscountA_Rank,
				DiscountA_Rank2

from			report_dates0 A
	join		all_technical_vars_univ_ranked2 B
		on		A.SecCode = B.SecCode
		and		A.FilterDate = B.FilterDate

where			A.ResearchFlag = 1
