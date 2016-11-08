
use ram;

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_master_equities', 'U') is not null 
	drop table ram.dbo.ram_master_equities


create table	ram.dbo.ram_master_equities (

				SecCode int,
				DsInfoCode int,
				IdcCode int,
				Date_ smalldatetime,

				-- Raw values
				Open_ real,
				High real,
				Low real,
				Close_ real,
				Vwap real,
				Volume real,
				CashDividend real,

				AdjOpen real,
				AdjHigh real,
				AdjLow real,
				AdjClose real,
				AdjVwap real,
				AdjVolume real,

				AvgDolVol real,
				MarketCap real,
				DividendFactor real,
				SplitFactor real,
				NormalTradingFlag smallint
				primary key (SecCode, IdcCode, Date_)
)


-------------------------------------------------------------------
-------------------------------------------------------------------


; with idc_dates as (

	-- All dates from IDC tables
	select distinct Code, Date_ from qai.prc.PrcExc where Date_ >= '1993-12-01'
	union
	select distinct Code, Date_ from qai.prc.PrcDly where Date_ >= '1993-12-01'
	union
	select distinct Code, Date_ from qai.prc.PrcVol where Date_ >= '1993-12-01'

)


, shares as (

select		Code, 
			Date_ as StartDate, 
			dateadd(day, -1, Lead(Date_, 1) over (
				partition by Code 
				order by Date_)) as EndDate, 
			Shares 
from		qai.prc.PrcShr
where		Date_ >= '1992-01-01'

)


, exchanges as (

select		Code,
			StartDate, 
			coalesce(DATEADD(day, -1, lead(StartDate, 1) over (
				partition by Code 
				order by StartDate)), EndDate, getdate()) as AltEndDate,
			Exchange
from		qai.prc.PrcScChg 

)


, trading_dates_filter as (

	-- Lag dates for filtering
	select		Date_,
				Lag(Date_, 252) over (
					order by Date_) as DateLagC
	from		(select distinct T0 as Date_ from ram.dbo.trading_dates) a

)


, data_merge as (

select
				M.SecCode,
				N.VenCode as DsInfoCode,
				D.Code as IdcCode,
				D.Date_,
				DF.DateLagC,
				-- (For historical cusip:) E.Cusip,
				-- (For historical ticker:) E.Ticker,
				coalesce(P1.Open_, P2.Open_, P4.Open_) as Open_,
				coalesce(P1.High, P2.High, P4.High) as High,
				coalesce(P1.Low, P2.Low, P4.Low) as Low,
				coalesce(P1.Close_, P2.Close_, P4.Close_) as Close_,
				coalesce(Nullif(P3.Vwap, -99999), P4.Vwap) as Vwap,
				coalesce(P2.Volume, P1.Volume) as Volume,
				MC.ConsolMktVal as tempMarketCap,
				SH.Shares as tempShares,
				Isnull(DV.CashDividend, 0) as CashDividend,
				A.Factor as SplitFactor
from			idc_dates D

	-- Filter --

	-- Trading dates lag column for 'NormalTradingFlag'
	join		trading_dates_filter DF
		on		D.Date_ = DF.Date_

	-- Historical Exchange filter -- Null handling for missing EndDates
	join		exchanges E
		on		D.Code = E.Code
		and		D.Date_ between E.StartDate and E.AltEndDate
		and		E.Exchange in ('A', 'B', 'C', 'D', 'E', 'F', 'T')	-- U.S. Exchanges

	-- Security type filter
	join		qai.prc.PrcInfo F
		on		D.Code = F.Code
		and		F.SecType = 'C'

	-- Merge Security Master mapping
	join		qai.dbo.SecMapX M
		on		D.Code = M.VenCode
		and		M.Exchange = 1
		and		M.VenType = 1

	-- Join datastream codes - Rank = 1 gets rid of the handful of duplicates
	left join	qai.dbo.SecMapX N
		on		M.SecCode = N.SecCode
		and		N.Exchange = 1
		and		N.VenType = 33
		and		N.[Rank] = 1

	-- DATA --

	-- Exchange pricing is primary. Volume is reported separately
	left join	qai.prc.PrcExc P1
		on		D.Code = P1.Code
		and		D.Date_ = P1.Date_

	-- Composite pricing is secondary. Volume is reported separately
	left join	qai.prc.PrcDly P2
		on		D.Code = P2.Code
		and		D.Date_ = P2.Date_

	-- Vwap Pricing comes from this table
	left join	qai.prc.PrcVol P3
		on		D.Code = P3.Code
		and		D.Date_ = P3.Date_

	-- Adjustment factor
	left join	qai.prc.PrcAdj	A
		on		A.Code = D.Code
		and		A.AdjType = 1
		and		D.Date_ >= A.StartDate
		and		D.Date_ <= A.EndDate

	-- Ds2 Data
	left join	qai.dbo.DS2PrimQtPrc P4
		on		N.VenCode = P4.InfoCode
		and		D.Date_ = P4.MarketDate

	-- Dividends
	left join	(
				-- There are some data points that have multiple entries for a single
				-- exdate. Though it isn't many observations, we decided to sum
				-- them under the assumption they are real dividends.
				select	Code, ExDate, sum(Rate) as CashDividend
				from	qai.prc.PrcDiv
				where	PayType  = 0		-- Normal Cash Dividend
					and DivType  = 1		-- Cash Dividend
					and SuppType = 0		-- Normal Cash Dividend
				group by Code, ExDate
				) DV
		on		DV.Code = D.Code
		and		DV.ExDate = D.Date_

	-- Consolidated Market Cap
	left join	qai.dbo.Ds2MktVal MC
		on		N.VenCode = MC.InfoCode
		and		D.Date_ = MC.ValDate

	-- Shares outstanding for MarketCap
	-- IMPROVEMENT: This is where would could add in
	-- data stream functionality because there is missing
	-- share data.
	left join	shares SH
		on		D.Code = SH.Code 
		and		D.Date_ between SH.StartDate and SH.EndDate

)


, agg_data as (

select			*,

				-- Calculation for DividendFactor
				sum(log((1 + isnull(CashDividend, 0) / Close_))) over (
					partition by IdcCode 
					order by Date_) as CumRate,

				-- Average Dollar Volume
				avg(Vwap * Volume) over (
					partition by IdcCode 
					order by Date_
					rows between 29 preceding and current row) / 1e6 as AvgDolVol,

				-- Market Cap calculated here to use coalesced Close_
				coalesce(tempMarketCap, tempShares * Close_ / 1e6) as MarketCap,

				-- Date Lag for NormalTradingFlag
				Lag(Date_, 252) over (
					partition by IdcCode 
					order by Date_) as DateLag

from			data_merge

)


, final_table as (
select 			D.SecCode,
				D.DsInfoCode,
				D.IdcCode,

				D.Date_,

				D.Open_,
				D.High,
				D.Low,
				D.Close_,
				D.Vwap,
				D.Volume,
				D.CashDividend,

				-- Adjusted prices for ease in calculation downstream
				D.Open_ * D.SplitFactor * exp(D.CumRate) as AdjOpen,
				D.High * D.SplitFactor * exp(D.CumRate) as AdjHigh,
				D.Low * D.SplitFactor * exp(D.CumRate) as AdjLow,
				D.Close_ * D.SplitFactor * exp(D.CumRate) as AdjClose,
				D.Vwap * D.SplitFactor * exp(D.CumRate) as AdjVwap,
				D.Volume / D.SplitFactor as AdjVolume,

				D.AvgDolVol,
				D.MarketCap,

				exp(D.CumRate) as DividendFactor,
				D.SplitFactor,			

				-- Filter that is used in universe selection
				case when DateLag = DateLagC then 1 else 0 end as NormalTradingFlag

from			agg_data D
where			Date_ >= '1995-01-01'
)


insert into ram.dbo.ram_master_equities
select * from final_table

-- General Indexes
create index idccode_date on ram.dbo.ram_master_equities (IdcCode, Date_)
create index date_idccode on ram.dbo.ram_master_equities (Date_, IdcCode)

-- Filter Indexes
create index date_avgdolvol on ram.dbo.ram_master_equities (Date_, AvgDolVol)
create index date_marketcap on ram.dbo.ram_master_equities (Date_, MarketCap)
