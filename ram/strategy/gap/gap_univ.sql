


declare @EvalDate date = '2013-01-01';
declare @StartDate date = '2012-06-01';
declare @EndDate date = '2016-01-01';
declare @N int = 1000;



with dolvol_filter as (
select 
    top (@N) IdcCode
from ram.dbo.univ_filter_data
where Date_ = (
    select T0 
    from ram.dbo.trading_dates 
    where CalendarDate = @EvalDate)
    and AvgDolVol >= 3
order by AvgDolVol desc 
)


, id_sector as (
select
    dv.IdcCode,
    info.Ticker,
    left(info.SIC, 2) as Ind
    
from dolvol_filter dv

join qai.prc.PrcInfo info
    on info.Code = dv.IdcCode
)


, prices as (
    select
    ids.Ticker,
    ids.Ind,
    
    univ.*,
    Close_ * AdjFactor as AdjClose,

	Avg(Close_ * AdjFactor) over (
	  partition by ids.IdcCode
	  order by Date_ 
	  rows between 20 preceding and 1 preceding) as AdjCloseMA20,

    Lag(High, 1) over (
	  partition by ids.IdcCode
	  order by Date_ ) as PrevHigh,

    Lag(Low, 1) over (
	  partition by ids.IdcCode
	  order by Date_ ) as PrevLow,

    Lag(Close_ * AdjFactor, 1) over (
	  partition by ids.IdcCode
	  order by Date_ ) as PrevAdjClose

from id_sector ids

join ram.dbo.univ_filter_data univ
    on univ.IdcCode = ids.IdcCode
    and univ.Date_ between @StartDate and @EndDate
)




select 
    IdcCode as ID,
    Date_ as [Date],
    Ticker,
    Open_ as [Open],
    High,
    PrevHigh,
    Low,
    PrevLow,
    Close_ as [Close],
    AdjClose,
    PrevAdjClose,
    AdjCloseMA20,
    AvgDolVol,

	Stdev((AdjClose-PrevAdjClose) / PrevAdjClose) over (
	  partition by prices.IdcCode
	  order by prices.Date_ 
	  rows between 90 preceding and 1 preceding) as StdevRet

from prices

order by prices.IdcCode, prices.Date_

