set NOCOUNT on;


; with report_dates as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


, dividend_yields as (
select				P.SecCode,
					P.Date_,

					-- Annual, semi-annual, and quarterly rate conversion
					case 
						when D2.PayFreqCode in ('001', '009') then D.Rate
						when D2.PayFreqCode in ('002', '00A') then D.Rate * 2
						when D2.PayFreqCode in ('004', '00D') then D.Rate * 12
						else D.Rate * 4
					end / P.AdjClose as DividendYield

from				ram.dbo.ram_equity_pricing_research P

	join			qai.prc.PrcDiv D 
		on			P.IdcCode = D.Code
		and			D.SeqCode = (
						select max(div.SeqCode) 
						from qai.prc.PrcDiv div
						where div.ExDate <= P.Date_ )
		and			D.DivType = 1											-- Cash dividend
		and			isnull(D.PayType, 0) in (0, 5, 6, 15, 16, 17, 18, 19)	-- Normal dividends

	join			qai.prc.PrcDiv2 D2
		on			D.Code = D2.Code
		and			D.SeqCode = D2.SeqCode

where				P.NormalTradingFlag = 1
	and				P.OneYearTradingFlag = 1
	and				P.AvgDolVol >= 3
	and				P.Close_ >= 15
	and				P.MarketCap >= 200
	and				P.SecCode in (select distinct SecCode from ram.dbo.ram_master_ids)
)


, dividend_yields2 as (
select				*,
					percent_rank() over (
					partition by Date_
					order by DividendYield) as DividendYield_Rank
from				dividend_yields
)


select				R.SecCode,
					R.ReportDate,
					D.DividendYield,
					D.DividendYield_Rank
from				report_dates R
	left join		dividend_yields2 D
		on			R.SecCode = D.SecCode
		and			R.FilterDate = D.Date_
where				R.ResearchFlag = 1
