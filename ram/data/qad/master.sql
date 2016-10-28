
use ram;

IF OBJECT_ID('ram.dbo.ram_master', 'U') IS NOT NULL
  DROP TABLE ram.dbo.ram_master;


create table ram_master
	(IdcCode int,
	 Date_ smalldatetime,
	 Open_ real,
	 High real,
	 Low real,
	 Close_ real,
	 Vwap real,
	 Volume real,
	 AvgDolVol real,
	 MarketCap real,
	 CashDividend real,
	 DividendFactor real,
	 SplitFactor real,
	 NormalTradingFlag smallint
	 primary key (IdcCode, Date_)
	 )


--------------------------------------------------------------
-- Mapping Codes: Datastream data when available will be used
-- to calculate the consolidated market cap. Otherwise we will
-- use the IDC database

; with dscodes as (
select
	M.SecCode,
	M.VenCode
from qai.dbo.SecMapX M
join qai.dbo.Ds2CtryQtInfo C
	on M.VenCode = C.InfoCode
	and M.Exchange = 1
	and M.VenType = 33		-- DataStream
	and C.Region = 'US'		-- US Listed stocks only
	and C.InfoCode in (select distinct InfoCode from qai.dbo.Ds2MktVal)
	and C.DsLocalCode is not null
)


, idccodes as (
-- Map to IdcCodes
select
	I.Code,
	D.VenCode as DsInfoCode
from qai.prc.PrcInfo I
join qai.dbo.SecMapX M
	on I.Code = M.VenCode
	and M.Exchange = 1
	and M.VenType = 1
	and I.SecType = 'C'
	and ascii(I.CurrEx) in (37, 49, 65, 66, 67, 
							68, 69, 70, 72, 78, 
							81, 82, 83, 84, 85)
left join dscodes D
	on M.SecCode = D.SecCode
)


-------------------------------------------------
-- Next two CTEs are used to filter out multiple 
-- DsInfoCodes per IDC Code

, codes_temp as (
select
	Code,
	DsInfoCode = max(DsInfoCode),
	Count(*) as Count_
from idccodes
group by Code
)


, codes as (
select
	Code,
	case when Count_ > 1 then null else DsInfoCode end as DsInfoCode
from codes_temp
)


---------------------------------------------------------
-- Cleaning of data happens in many of the following CTEs.
-- This handles nans in most cases.

, pricing01 as (
select
	-- Due to full outer join, need to ensure Code, Date_
	case
		when P1.Code is null then P2.Code else P1.Code
		end as Code,
	case
		when P1.Date_ is null then P2.Date_ else P1.Date_
		end as Date_,

	-- Standard vals
	P1.Open_,
	P1.High,
	P1.Low,
	P1.Close_,
	P1.TotRet,
	P1.Volume,
	P2.Vwap

from
	qai.prc.PrcDly P1
full outer join
	qai.prc.PrcVol P2
on	
	P1.Code = P2.Code and
	P1.Date_ = P2.Date_
where (P1.Code in (select distinct Code from codes)
	or P2.Code in (select distinct Code from codes))
	and P1.Close_ != 0
)


, dividends as (
-- There are some data points that have multiple entries for a single
-- exdate. Though it isn't many observations, we decided to sum
-- them under the assumption they are real dividends.
select
	Code, 
	ExDate, 
	sum(Rate) as CashDividend
from qai.prc.PrcDiv
where PayType = 0			-- Normal Cash Dividend
	and DivType = 1			-- Cash Dividend
	and SuppType = 0		-- Normal Cash Dividend
group by Code, ExDate
)


, pricing02 as (
select
	P.*,
	Isnull(D.CashDividend, 0) as CashDividend
from pricing01 P
left join dividends D
on P.Code = D.Code
	and P.Date_ = D.ExDate
)


----------------------------------------------
-- Adjustment factor derived from dividend pricing

, pricing03 as (
select
	*,
	sum(log((1 + Isnull(CashDividend, 0) / Close_))) over 
		(partition by Code order by Date_) as CumRate
from pricing02
)


---------------------------------------------
-- Data Stream Market cap mapping. Left join

, pricing04 as (
select 
	P.*,
	M.ConsolMktVal 
from pricing03 P
join codes C
	on P.Code = C.Code
left join qai.dbo.Ds2MktVal M
	on C.DsInfoCode = M.InfoCode
	and P.Date_ = M.ValDate
)


---------------------------------------------
-- Lag pricing for error handling

, pricing05 as (
select
	P.*,
	Lag(P.Open_, 1) over (
		partition by P.Code
		order by P.Date_) as OpenLag,
	Lag(P.High, 1) over (
		partition by P.Code
		order by P.Date_) as HighLag,
	Lag(P.Low, 1) over (
		partition by P.Code
		order by P.Date_) as LowLag,
	Lag(P.Close_, 1) over (
		partition by P.Code
		order by P.Date_) as CloseLag,
	Lag(P.Volume, 1) over (
		partition by P.Code
		order by P.Date_) as VolumeLag,
	Lag(P.Vwap, 1) over (
		partition by P.Code
		order by P.Date_) as VwapLag,
	Lag(P.TotRet, 1) over (
		partition by P.Code
		order by P.Date_) as TotRetLag,
	Lag(P.Date_, 252) over (
		partition by P.Code
		order by P.Date_) as DateLag,

	A.Factor as SplitFactor,		-- Added for downstream calculations

	exp(P.CumRate) as DividendFactor

from pricing04 P
left join qai.prc.PrcAdj A
	on A.Code = P.Code
	and A.AdjType = 1
	and P.Date_ >= A.StartDate
	and P.Date_ <= A.EndDate

)


, trading_dates_filter as (
select
	Date_,
	Lag(Date_, 252) over (
		order by Date_) as DateLagC
from (select distinct T0 as Date_ from ram.dbo.trading_dates) a
)


, pricing06 as (
select
	P.*,
	D.DateLagC
from pricing05 P
join trading_dates_filter D
	on P.Date_ = D.Date_
)


, pricing07 as (
-- Merge with share counts
select
	P.*,
	S.Shares
from
	pricing06 P
left join 
	qai.prc.PrcShr S
on 
	S.Code = P.Code and
	-- Merges on max available date from Share table
	-- that is before or on current daily date
	-- IMPROVEMENT: This is where would could add in
	-- data stream functionality because there is missing
	-- share data.
	S.Date_ = (select max(S2.Date_) 
			   from qai.prc.PrcShr S2
			   where S2.Code = P.Code
			   and S2.Date_ <= P.Date_)
)


, pricing08 as (
select distinct * from pricing07
)


, pricing09 as (
select
	Code,
	Date_,
	-- Open prices
	case 
		when (Open_ > 0) then Open_
		when (OpenLag > 0) then OpenLag
		else Null
	end as Open_,
	-- High prices
	case 
		when (High > 0) then High
		when (HighLag > 0) then HighLag
		else Null
	end as High,
	-- Low prices
	case 
		when (Low > 0) then Low
		when (LowLag > 0) then LowLag
		else Null
	end as Low,
	-- Close prices
	case 
		when (Close_ > 0) then Close_
		when (Vwap > 0) then Vwap
		when (CloseLag > 0) then CloseLag
		when (VwapLag > 0) then VwapLag
		else Null
	end as Close_,
	-- Vwap prices
	case 
		when (Vwap > 0) then Vwap
		when (Close_ > 0) then Close_
		when (VwapLag > 0) then VwapLag
		when (CloseLag > 0) then CloseLag
		else Null
	end as Vwap,
	-- Volume
	case 
		when (Volume > 0) then Volume
		when (VolumeLag > 0) then VolumeLag
		else 0
	end as Volume,
	-- TotRet
	case 
		when (TotRet > 0) then TotRet
		when (TotRetLag > 0) then TotRetLag
		else Null
	end as TotRet,
	-- Others
	Shares,
	ConsolMktVal,
	DividendFactor,
	SplitFactor,
	CashDividend,
	case when DateLag = DateLagC then 1 else 0 end as NormalTradingFlag

from pricing08

)


, pricing10 as (
select
	*,
	-- Market Cap
	coalesce(ConsolMktVal, Shares * Close_ / 1e6) as MarketCap,

	-- 30 Day Average Dollar Volume
	Avg(Vwap * Volume) over (
		partition by Code 
		order by Date_
		rows between 29 preceding and current row) / 1e6 as AvgDolVol

from pricing09 a
where Date_ <= (select max(Date_) from qai.prc.PrcDly b		-- Unsure what this filter is
			    where a.Code = b.Code)
and Date_ >= '1994-12-01'
)


, pricing_final as (
select distinct
	Code as IdcCode,
	Date_,
	Open_,
	High,
	Low,
	Close_,
	Vwap,
	Volume,
	AvgDolVol,
	MarketCap,
	CashDividend,
	DividendFactor,
	SplitFactor,
	NormalTradingFlag
from pricing10 i
)


insert into ram.dbo.ram_master
	(IdcCode, 
	 Date_, 
	 Open_, 
	 High, 
	 Low, 
	 Close_,
	 Vwap, 
	 Volume, 
	 AvgDolVol, 
	 MarketCap, 
	 CashDividend, 
	 DividendFactor, 
	 SplitFactor, 
	 NormalTradingFlag)
select * from pricing_final


create index idccode_date
on ram.dbo.ram_master (IdcCode, Date_)

create index date_idccode
on ram.dbo.ram_master (Date_, IdcCode)

create index date_avgdolvol
on ram.dbo.ram_master (Date_, AvgDolVol)
