
use ram;

declare @ETFS table (IdcCode int, DsInfoCode int)	
insert @ETFS values (59751, 8931), (140062, 230116)		-- SPY, VXX


-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_master_etf', 'U') is not null 
	drop table ram.dbo.ram_master_etf


create table	ram.dbo.ram_master_etf (

				SecCode int,
				DsInfoCode int,
				IdcCode int,
				Ticker varchar(10),
				Date_ smalldatetime,

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
				DividendFactor real,
				SplitFactor real

				primary key (SecCode, IdcCode, Ticker, Date_)
)


-------------------------------------------------------------------
-------------------------------------------------------------------

; with idc_dates as (

	-- All dates from IDC tables
	select distinct Code, Date_ from qai.prc.PrcExc where Code in (select IdcCode from @ETFS)
	union
	select distinct Code, Date_ from qai.prc.PrcDly where Code in (select IdcCode from @ETFS)
	union
	select distinct Code, Date_ from qai.prc.PrcVol where Code in (select IdcCode from @ETFS)

)


, data_merge as (

select
				M.SecCode,
				N.VenCode as DsInfoCode,
				D.Code as IdcCode,
				F.Ticker,
				D.Date_,

				coalesce(P1.Open_, P2.Open_, P4.Open_) as Open_,
				coalesce(P1.High, P2.High, P4.High) as High,
				coalesce(P1.Low, P2.Low, P4.Low) as Low,
				coalesce(P1.Close_, P2.Close_, P4.Close_) as Close_,
				coalesce(Nullif(P3.Vwap, -99999), P4.Vwap) as Vwap,
				coalesce(P2.Volume, P1.Volume) as Volume,
				Isnull(DV.CashDividend, 0) as CashDividend,
				A.Factor as SplitFactor

from			idc_dates D

	--  Tickers
	join		qai.prc.PrcInfo F
		on		D.Code = F.Code

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
					rows between 29 preceding and current row) / 1e6 as AvgDolVol

from			data_merge

)


, final_table as (
select 			D.SecCode,
				D.DsInfoCode,
				D.IdcCode,
				D.Ticker,

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

				exp(D.CumRate) as DividendFactor,
				D.SplitFactor

from			agg_data D
where			Date_ >= '1995-01-01'
)


insert into ram.dbo.ram_master_etf
select * from final_table

-- General Indexes
create index ticker_date on ram.dbo.ram_master_etf (Ticker, Date_)
create index date_ticker on ram.dbo.ram_master_etf (Date_, Ticker)
