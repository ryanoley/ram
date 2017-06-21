
use ram;

-- ######  Final Dividends Table   ################################################

if object_id('ram.dbo.ram_annualized_cash_dividends', 'U') is not null 
	drop table ram.dbo.ram_annualized_cash_dividends


create table	ram.dbo.ram_annualized_cash_dividends (
		IdcCode int,
		ExDate smalldatetime,
		Value_ float
		primary key (IdcCode, ExDate)
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
					D1.Rate,
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

)


-- ######  Annual Dividends  ###############################################

, annual_dividends_final as (
select				Code,
					ExDate,
					Rate as AnnualizedCashDividends
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
							rows between 11 preceding and current row)
						end as AnnualizedCashDividends
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
						end as AnnualizedCashDividends
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
						end as AnnualizedCashDividends
from				monthly_dividends_0
)


, stacked_data as (
select * from annual_dividends_final
union
select * from semiannual_dividends_final
union
select * from quarterly_dividends_final
union
select * from monthly_dividends_final
)


insert into ram.dbo.ram_annualized_cash_dividends
select * from stacked_data
