
use ram;

-- ######  Final Dividends Table   ################################################

if object_id('ram.dbo.ram_dividend_yield', 'U') is not null 
	drop table ram.dbo.ram_dividend_yield


create table	ram.dbo.ram_dividend_yield (
		IdcCode int,
		Date_ smalldatetime,
		Value_ float
		primary key (IdcCode, Date_)
)


IF OBJECT_ID('tempdb..#stackeddata') IS NOT NULL 
	DROP TABLE #stackeddata

create table #stackeddata
(
    Code int,
    ExDate smalldatetime,
	AnnualizedCashDividends float,
	Periodicity varchar(20)
	primary key (Code, ExDate)
)


-- ######  Final Accounting Table table   #########################################

; with cleaned_dividends_0 as (
select			D1.*
from			qai.prc.PrcDiv D1
	join		( select Code, ExDate, max(SeqCode) as SeqCode
				  from qai.prc.PrcDiv
				  group by Code, ExDate ) D2
	on			D1.Code = D2.Code
	and			D1.SeqCode = D2.SeqCode
where			D1.PayType  = 0			-- Normal Cash Dividend
	and			D1.DivType  = 1			-- Cash Dividend
	and			D1.SuppType = 0			-- Normal Cash Dividend
	and			D1.Code in (select distinct IdcCode from ram.dbo.ram_master_ids)
)


, cleaned_dividends_1 as (
select				D1.Code,
					D1.ExDate,
					D1.Rate * coalesce(P.SplitFactor, 1) as Rate,
					case 
						when D2.PayFreqCode in ('001', '009') then 'Annual'
						when D2.PayFreqCode in ('002', '00A') then 'SemiAnnual'
						when D2.PayFreqCode in ('004', '00D') then 'Monthly'
						else 'Quarterly'
					end as Periodicity
from				cleaned_dividends_0 D1

	left join		qai.prc.PrcDiv2 D2
		on			D1.Code = D2.Code
		and			D1.SeqCode = D2.SeqCode

	left join		ram.dbo.ram_equity_pricing_research P
		on			D1.Code = P.IdcCode
		and			D1.ExDate = P.Date_
)


-- ######  Annual Dividends  ###############################################

, annual_dividends_final as (
select				Code,
					ExDate,
					Rate as AnnualizedCashDividends,
					Periodicity
from				cleaned_dividends_1
where				Periodicity = 'Annual'
)


-- ######  Semi-Annual Dividends  ###############################################

, semiannual_dividends_0 as (
select				*,
					DATEDIFF (day, lag(ExDate, 1) over (
						partition by Code
						order by ExDate) , ExDate ) as LagDayDifference  

from				cleaned_dividends_1
where				Periodicity = 'SemiAnnual'
)


, semiannual_dividends_final as (
select				Code,
					ExDate,
					case
						when LagDayDifference is null then Rate * 2
						when LagDayDifference > 380 then Rate * 2
						else sum(Rate) over (
							partition by Code
							order by ExDate
							rows between 1 preceding and current row)
						end as AnnualizedCashDividends,
					Periodicity
from				semiannual_dividends_0
)


-- ######  Quarterly Dividends  ############################################

, quarterly_dividends_0 as (
select				*,
					DATEDIFF (day, lag(ExDate, 3) over (
						partition by Code
						order by ExDate) , ExDate ) as LagDayDifference  

from				cleaned_dividends_1
where				Periodicity = 'Quarterly'
)


, quarterly_dividends_final as (
select				Code,
					ExDate,
					case
						when LagDayDifference is null then Rate * 4
						when LagDayDifference > 380 then Rate * 4
						else sum(Rate) over (
							partition by Code
							order by ExDate
							rows between 3 preceding and current row)
						end as AnnualizedCashDividends,
					Periodicity
from				quarterly_dividends_0
)

-- ######  Monthly Dividends  ###############################################

, monthly_dividends_0 as (
select				*,
					DATEDIFF (day, lag(ExDate, 11) over (
						partition by Code
						order by ExDate) , ExDate ) as LagDayDifference  

from				cleaned_dividends_1
where				Periodicity = 'Monthly'
)


, monthly_dividends_final as (
select				Code,
					ExDate,
					case
						when LagDayDifference is null then Rate * 12
						when LagDayDifference > 380 then Rate * 12
						else sum(Rate) over (
							partition by Code
							order by ExDate
							rows between 11 preceding and current row)
						end as AnnualizedCashDividends,
					Periodicity
from				monthly_dividends_0
)


, stacked_dividends as (
select * from annual_dividends_final
union
select * from semiannual_dividends_final
union
select * from quarterly_dividends_final
union
select * from monthly_dividends_final
)


INSERT INTO #stackeddata
SELECT * from stacked_dividends


-- ######  FINAL  #########################################################

; with dividend_yield_data_0 as (   
select				P.Date_,
					P.IdcCode,
					D.Periodicity,
					D.ExDate,
					D.AnnualizedCashDividends / (P.Close_ * P.SplitFactor) as DividendYield
from				ram.dbo.ram_equity_pricing_research P
	left join		#stackeddata D
		on			P.IdcCode = D.Code
		and			D.ExDate = (select max(ExDate) from #stackeddata a
							    where a.Code = P.IdcCode and a.ExDate <= P.Date_)
)


insert into ram.dbo.ram_dividend_yield
select				IdcCode,
					Date_,
					case
						when Periodicity = 'Monthly' and datediff(day, ExDate, Date_) < 70 then DividendYield
						when Periodicity = 'Quarterly' and datediff(day, ExDate, Date_) < 120 then DividendYield
						when Periodicity = 'SemiAnnual' and datediff(day, ExDate, Date_) < 210 then DividendYield
						when Periodicity = 'Annual' and datediff(day, ExDate, Date_) < 390 then DividendYield
						else Null
					end as DividendYield
from				dividend_yield_data_0

