
set NOCOUNT on;

; with report_dates as (
select * from ram.dbo.ram_earnings_report_dates where $(trade) = 1
union all
select * from ram.dbo.ram_pead_report_dates where $(trade) = 2

)


select				D.SecCode,
					D.ReportDate,
					coalesce(C.Value_ / P.Close_, 0) as DividendYield
from				report_dates D

	left join		ram.dbo.ram_equity_pricing_research P
		on			D.IdcCode = P.IdcCode
		and			D.FilterDate = P.Date_

	left join		ram.dbo.ram_annualized_cash_dividends C
		on			D.IdcCode = C.IdcCode
		and			C.ExDate = ( select max(ExDate) from ram.dbo.ram_annualized_cash_dividends
								 where IdcCode = D.IdcCode and ExDate <= D.FilterDate
								 and ExDate >= dateadd(day, -380, '2011-09-01') )
where				D.ResearchFlag = 1
