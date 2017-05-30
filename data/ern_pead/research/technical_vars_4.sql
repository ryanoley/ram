SET NOCOUNT ON


; with report_dates as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


, prices1 as (
select			*,
				-- MFI
				(AdjHigh + AdjLow + AdjClose) / 3 as TypPrice,
				(AdjHigh + AdjLow + AdjClose) / 3 * AdjVolume as MonFlowP,
				-- Trade Vs Shares Out
				MarketCap / AdjClose * 1e6 as Shares,
				avg(AdjVolume) over (
					partition by SecCode
					order by Date_
					rows between 9 preceding and current row) as AvgVolume10,
				avg(AdjVolume) over (
					partition by SecCode
					order by Date_
					rows between 29 preceding and current row) as AvgVolume30,
				avg(AdjVolume) over (
					partition by SecCode
					order by Date_
					rows between 59 preceding and current row) as AvgVolume60

from			ram.dbo.ram_equity_pricing_research
where			SecCode in (select distinct SecCode from report_dates)

)


, prices2 as (
select			*,
				-- MFI
				lag(TypPrice, 1) over (
					partition by SecCode
					order by Date_) as LagTypPrice,
				-- Trade Vs Shares Out
				AvgVolume30 / Shares as TrdVsOut,
				lag(AvgVolume30 / Shares, 1) over (
					partition by SecCode
					order by Date_) as LagTrdVsOut,
				-- Trends in volumes
				AvgVolume10 / AvgVolume30 as VolumeMA10_30,
				AvgVolume10 / AvgVolume60 as VolumeMA10_60

from			prices1
)


, prices3 as (
select			*,
				-- MFI
				case 
					when TypPrice > LagTypPrice then MonFlowP
					else 0 
				end as MonFlow_P,
				case 
					when LagTypPrice is null then MonFlowP
					when TypPrice != LagTypPrice then MonFlowP 
					else 0 
				end as MonFlow,
				-- Trade Vs Shares Out
			    100 * (TrdVsOut - LagTrdVsOut) / LagTrdVsOut as DelTrdVsOut
from			prices2

)


, prices4 as (
select			*,
				-- MFI
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
from			prices3
where			NormalTradingFlag = 1
	and			OneYearTradingFlag = 1
	and			AvgDolVol >= 3
	and			Close_ >= 15
	and			MarketCap >= 200

)


, prices5 as (
select			SecCode,
				Date_,

				-- MFI
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
					order by MFI_30) as MFI_30_Rank,
				-- Trade Vs Shares Out
				TrdVsOut,
				DelTrdVsOut,
				percent_rank() over (
					partition by Date_
					order by TrdVsOut) as TrdVsOut_Rank,
				percent_rank() over (
					partition by Date_
					order by DelTrdVsOut) as DelTrdVsOut_Rank,
				-- Trends in Volumes
				VolumeMA10_30,
				VolumeMA10_60,
				percent_rank() over (
					partition by Date_
					order by VolumeMA10_30) as VolumeMA10_30_Rank,
				percent_rank() over (
					partition by Date_
					order by VolumeMA10_60) as VolumeMA10_60_Rank				

from			prices4
)


select			A.SecCode,
				A.ReportDate,
				-- MFI
				B.MFI_5,
				B.MFI_10,
				B.MFI_30,
				B.MFI_5_Rank,
				B.MFI_10_Rank,
				B.MFI_30_Rank,
				-- Trade Vs Shares Out
				B.TrdVsOut,
				B.DelTrdVsOut,
				B.TrdVsOut_Rank,
				B.DelTrdVsOut_Rank,
				-- Volume Trends
				B.VolumeMA10_30,
				B.VolumeMA10_60,
				B.VolumeMA10_30_Rank,
				B.VolumeMA10_60_Rank

from			report_dates A
	join		prices5 B
		on		A.SecCode = B.SecCode
		and		A.FilterDate = B.Date_

where			A.ResearchFlag = 1
order by		A.SecCode, A.ReportDate
