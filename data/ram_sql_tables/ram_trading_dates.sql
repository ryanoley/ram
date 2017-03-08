/*
	Table that locates TradingDates that are offset 
	by varying days as needed for all queries.

	Convention is to name columns T(x) and Tm(X)
	to go forward and backwards from Weekday_.

	Note: To edit dates, one must change code in both
	trading_dates AND all_dates with-clauses

*/

use ram;

if object_id ('dbo.ram_trading_dates', 'U') is not null
	drop table ram_trading_dates ;
go

--Ensure Monday is 1
SET DATEFIRST 1


; with calendar_dates as (
select top 30000 DATEADD(d, incr, '1960-01-01') as CalendarDate
from (
	select	incr = row_number() over (order by object_id, column_id), *
	from (
		select		a.object_id, 
					a.column_id 
		from		sys.all_columns a 
		cross join	sys.all_columns b ) as a ) as b
)


, trading_dates as (
select		C.CalendarDate as T0,
			lag(C.CalendarDate, 3) over (order by C.CalendarDate) as Tm3,
			lag(C.CalendarDate, 2) over (order by C.CalendarDate) as Tm2,
			lag(C.CalendarDate, 1) over (order by C.CalendarDate) as Tm1,
			lead(C.CalendarDate, 1) over (order by C.CalendarDate) as T1,
			lead(C.CalendarDate, 2) over (order by C.CalendarDate) as T2,
			lead(C.CalendarDate, 3) over (order by C.CalendarDate) as T3
from		calendar_dates C
where		C.CalendarDate not in (select Date_ from qai.dbo.SDDates_v where ExchCode = 410) -- NYSE Exchange Holidays
  and		DATEPART(dw, C.CalendarDate) != 6 -- Sat
  and		DATEPART(dw, C.CalendarDate) != 7 -- Sun
)


, next_biz_date as (
select		C.CalendarDate,
			(select min(T0) from trading_dates 
			 where T0 >= C.CalendarDate) as T0
from		calendar_dates C
)


, final_dates as (
select		C.CalendarDate,
			DATEPART(dw, C.CalendarDate) as [Weekday],
			T.*
from		next_biz_date C
left join	trading_dates T
	on		T.T0 = C.T0
)

select		*
into		ram.dbo.ram_trading_dates
from		final_dates
