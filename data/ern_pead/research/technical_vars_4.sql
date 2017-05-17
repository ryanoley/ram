SET NOCOUNT ON


; with report_dates as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2
)


, typical_prices1 as (
select			*,
				(AdjHigh + AdjLow + AdjClose) / 3 as TypPrice,
				(AdjHigh + AdjLow + AdjClose) / 3 * AdjVolume as MonFlowP
from			ram.dbo.ram_equity_pricing_research
where			SecCode in (select distinct SecCode from ram.dbo.ram_master_ids)
)


, typical_prices2 as (
select			*,
				lag(TypPrice, 1) over (
					partition by SecCode
					order by Date_) as LagTypPrice
from			typical_prices1
)


, money_flow as (
select			*,
				case 
					when TypPrice > LagTypPrice then MonFlowP
					else 0 
				end as MonFlow_P,
				case 
					when LagTypPrice is null then MonFlowP
					when TypPrice != LagTypPrice then MonFlowP 
					else 0 
				end as MonFlow
from			typical_prices2
)


, mfi1 as (
select			*,
				sum(MonFlow_P) over (
					partition by SecCode
					order by Date_ 
					rows between 4 preceding and current row) / 
				nullif(sum(MonFlow) over (
					partition by SecCode
					order by Date_
					rows between 4 preceding and current row), 0) * 100 as MFI_5,

				sum(MonFlow_P) over (
					partition by SecCode
					order by Date_ 
					rows between 9 preceding and current row) / 
				nullif(sum(MonFlow) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row), 0) * 100 as MFI_10,

				sum(MonFlow_P) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) / 
				nullif(sum(MonFlow) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row), 0) * 100 as MFI_30
from			money_flow
where			NormalTradingFlag = 1
	and			OneYearTradingFlag = 1
	and			AvgDolVol >= 3
	and			Close_ >= 15
	and			MarketCap >= 200
	and			SecCode in (select distinct SecCode from ram.dbo.ram_master_ids)

)


, mfi2 as (
select			SecCode,
				Date_,

				MFI_5,
				MFI_10,
				MFI_30,

				percent_rank() over (
					partition by Date_
					order by MFI_5) as MFI_5_Rank,
				percent_rank() over (
					partition by Date_
					order by MFI_10) as MFI_10_Rank,
				percent_rank() over (
					partition by Date_
					order by MFI_30) as MFI_30_Rank

from			mfi1
)


select			A.SecCode,
				A.ReportDate,
				B.MFI_5,
				B.MFI_10,
				B.MFI_30,
				B.MFI_5_Rank,
				B.MFI_10_Rank,
				B.MFI_30_Rank

from			report_dates A
	join		mfi2 B
		on		A.SecCode = B.SecCode
		and		A.FilterDate = B.Date_

where			A.ResearchFlag = 1
order by		A.SecCode, A.ReportDate
