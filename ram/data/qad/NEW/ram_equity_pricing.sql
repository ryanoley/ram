
use ram;

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_equity_pricing', 'U') is not null 
	drop table ram.dbo.ram_equity_pricing


create table	ram.dbo.ram_equity_pricing (
		SecCode int,
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
		primary key (SecCode, Date_)
)


-- ############################################################################################
--  TEMP TABLES

if object_id('tempdb..#idc_data', 'U') is not null 
	drop table #idc_data

if object_id('tempdb..#pricing_data', 'U') is not null 
	drop table ram.dbo.ram_equity_raw_pricing


create table #idc_data
(
    SecCode int,
	DsInfoCode int,
	IdcCode int,
    Date_ datetime,
	Open_ real,
	High real,
	Low real,
	Close_ real,
	Vwap real,
	Volume real,
	SplitFactor real,
	CashDividend real,
	tempShares real
	primary key (SecCode, Date_)
)


create table #pricing_data (
	SecCode int,
	Date_ smalldatetime,
	-- Raw values
	Open_ real,
	High real,
	Low real,
	Close_ real,
	Vwap real,
	Volume real,
	CashDividend real,
	MarketCap real,
	SplitFactor real,
	primary key (SecCode, Date_)
)



-- ############################################################################################
--  IDC Pricing

; with idc_dates as (
-- All dates from IDC tables
select distinct Code, Date_ from qai.prc.PrcExc where Date_ >= '1993-12-01'
union
select distinct Code, Date_ from qai.prc.PrcDly where Date_ >= '1993-12-01'
union
select distinct Code, Date_ from qai.prc.PrcVol where Date_ >= '1993-12-01'

)


, idc_dates2 as (
select			P.*,
				M.SecCode,
				M.DsInfoCode,
				S.Shares
from			idc_dates P

join			ram.dbo.ram_master_ids M
		on		P.Code = M.IdcCode
		and		P.Date_ between M.StartDate and M.EndDate

left join		qai.prc.PrcShr S
on 
	S.Code = P.Code and
	-- Merges on max available date from Share table
	-- that is before or on current daily date
	S.Date_ = (select max(S2.Date_) 
			   from qai.prc.PrcShr S2
			   where S2.Code = P.Code
			   and S2.Date_ <= P.Date_)
)


, idc_clean_prices1 as (
select			Code,
				Date_,
				case when Open_ > 0 then Open_ else Null end as Open_,
				case when High > 0 then High else Null end as High,
				case when Low > 0 then Low else Null end as Low,
				case when Close_ > 0 then Close_ else Null end as Close_,
				case when Volume > 0 then Volume else Null end as Volume
from			qai.prc.PrcExc

)


, idc_clean_prices2 as (
select			Code,
				Date_,
				case when Open_ > 0 then Open_ else Null end as Open_,
				case when High > 0 then High else Null end as High,
				case when Low > 0 then Low else Null end as Low,
				case when Close_ > 0 then Close_ else Null end as Close_,
				case when Volume > 0 then Volume else Null end as Volume
from			qai.prc.PrcDly

)


, idc_clean_prices3 as (
select			Code,
				Date_,
				case when Vwap > 0 then Vwap else Null end as Vwap
from			qai.prc.PrcVol

)


, idc_data_all as (
select			D.SecCode,
				D.DsInfoCode,
				D.Code as IdcCode,
				D.Date_,
				coalesce(P1.Open_, P2.Open_) as Open_,
				coalesce(P1.High, P2.High) as High,
				coalesce(P1.Low, P2.Low) as Low,
				coalesce(P1.Close_, P2.Close_) as Close_,
				P3.Vwap,
				coalesce(P2.Volume, P1.Volume) as Volume,
				A.Factor as SplitFactor,
				Isnull(DV.CashDividend, 0) as CashDividend,
				D.Shares as tempShares

from			idc_dates2 D

	left join	idc_clean_prices1 P1
		on		D.Code = P1.Code
		and		D.Date_ = P1.Date_

	left join	idc_clean_prices2 P2
		on		D.Code = P2.Code
		and		D.Date_ = P2.Date_

	left join	idc_clean_prices3 P3
		on		D.Code = P3.Code
		and		D.Date_ = P3.Date_

	left join	qai.prc.PrcAdj	A
		on		A.Code = D.Code
		and		A.AdjType = 1
		and		D.Date_ >= A.StartDate
		and		D.Date_ <= A.EndDate

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

insert into #idc_data
select * from idc_data_all

go;

-- ############################################################################################
--  Data Stream data

; with data_merge as (

select				IDC.SecCode,
					IDC.Date_,
					coalesce(IDC.Open_, P.Open_) as Open_,
					coalesce(IDC.High, P.High) as High,
					coalesce(IDC.Low, P.Low) as Low,
					coalesce(IDC.Close_, P.Close_) as Close_,
					coalesce(IDC.Vwap, P.Vwap) as Vwap,

					IDC.Volume,
					IDC.CashDividend,

					-- Market Cap calculated here to use coalesced Close_
					coalesce(MC.ConsolMktVal, IDC.tempShares * coalesce(IDC.Close_, P.Close_) / 1e6) as MarketCap,
					IDC.SplitFactor

from				#idc_data IDC

	left join		qai.dbo.DS2PrimQtPrc P
		on			IDC.DsInfoCode = P.InfoCode
		and			IDC.Date_ = P.MarketDate

	left join		qai.dbo.Ds2MktVal MC
		on			IDC.DsInfoCode = MC.InfoCode
		and			IDC.Date_ = MC.ValDate

)

insert into #pricing_data
select * from data_merge

go

-- ############################################################################################
--  Aggregated Fields

; with agg_data as (
select		*,
			-- Calculation for DividendFactor
			sum(log((1 + isnull(CashDividend, 0) / Close_))) over (
				partition by SecCode 
				order by Date_) as CumRate,
			-- Average Dollar Volume
			avg(Vwap * Volume) over (
				partition by SecCode 
				order by Date_
				rows between 29 preceding and current row) / 1e6 as AvgDolVol,
			-- Date Lag for NormalTradingFlag
			Lag(Date_, 252) over (
				partition by SecCode 
				order by Date_) as DateLag
from		#pricing_data

)


, trading_dates_filter as (
select		Date_,
			Lag(Date_, 252) over (
				order by Date_) as DateLagC
from		(select distinct T0 as Date_ from ram.dbo.trading_dates) a

)


-- ############################################################################################
--  Final Formatting

, final_table as (
select 			D.SecCode,
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
				case when DateLag = DF.DateLagC then 1 else 0 end as NormalTradingFlag

from			agg_data D
	join		trading_dates_filter DF
		on		D.Date_ = DF.Date_

where			D.Date_ >= '1995-01-01'

)

insert into ram.dbo.ram_equity_pricing
select * from final_table

