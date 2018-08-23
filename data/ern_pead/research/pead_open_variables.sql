SET NOCOUNT ON


; with all_technical_vars_univ as (
select			*,

				Lead(AdjOpen, 1) over (
					partition by SecCode
					order by Date_) as LeadOpen,

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

				LeadOpen / MA5 as OpenPRMA5,
				LeadOpen / MA10 as OpenPRMA10,
				LeadOpen / MA20 as OpenPRMA20,
				LeadOpen / MA60 as OpenPRMA60,

				LeadOpen / AdjClose as Overnight

from			all_technical_vars_univ

-- Universe filter happens after grouped functionality from all_technical_vars_univ above
where			NormalTradingFlag = 1
	and			OneYearTradingFlag = 1
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
					
				OpenPRMA5,
				percent_rank() over (
					partition by Date_
					order by OpenPRMA5) as Open_PRMA5_Rank,
				OpenPRMA10,
				percent_rank() over (
					partition by Date_
					order by OpenPRMA10) as Open_PRMA10_Rank,
				OpenPRMA20,
				percent_rank() over (
					partition by Date_
					order by OpenPRMA20) as Open_PRMA20_Rank,
				OpenPRMA60,
				percent_rank() over (
					partition by Date_
					order by OpenPRMA60) as Open_PRMA60_Rank,

				Overnight,
				percent_rank() over (
					partition by Date_
					order by Overnight) as Overnight_Rank

from			all_technical_vars_univ2

)


, report_dates0 as (

select *, EvalDate as FilterDate from ram.dbo.pead_event_dates_research

-- select * from ram.dbo.ram_pead_report_dates


)


select			A.ReportDate,
				B.*
from			report_dates0 A
	join		all_technical_vars_univ_ranked B
		on		A.SecCode = B.SecCode
		and		A.FilterDate = B.FilterDate
where			A.ResearchFlag = 1
	and			A.AvgDolVol >= 3
	and			A.MarketCap >= 200
	and			A.Close_ >= 15