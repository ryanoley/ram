
use ram;

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_master_equities', 'U') is not null 
	drop table ram.dbo.ram_master_equities


create table	ram.dbo.ram_master_equities (

				IsrCode int,
				SecCode int,
				DsInfoCode int,
				IdcCode int,
				HistoricalCusip varchar(15),
				HistoricalTicker varchar(15),
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


-- ############################################################################################
--  IDC Data - Share data

if object_id('tempdb..#idc_shares_table', 'U') is not null 
	drop table #idc_shares_table


create table #idc_shares_table
(
	Code int,
	StartDate datetime,
	EndDate datetime,
	Shares real
)


; with shares as (

select		Code, 
			Date_ as StartDate, 
			dateadd(day, -1, Lead(Date_, 1) over (
				partition by Code 
				order by Date_)) as EndDate, 
			Shares 
from		qai.prc.PrcShr
where		Date_ >= '1992-01-01'

)

INSERT INTO
   #idc_shares_table
SELECT
    *
FROM
    shares

go

CREATE CLUSTERED INDEX IDX_Code_Date ON #idc_shares_table(Code, StartDate, EndDate)

go

-- ############################################################################################
--  IDC Data - ID Dates

if object_id('tempdb..#idc_dates_table', 'U') is not null 
	drop table #idc_dates_table

create table #idc_dates_table
(
    SecCode int,
	Code int,
	IsrCode int,
	HistoricalCusip varchar(15),
	HistoricalTicker varchar(15),
    Date_ datetime,
	DateLagC datetime,
	Open_ real,
	High real,
	Low real,
	Close_ real,
	Vwap real,
	Volume real,

	SplitFactor real,
	CashDividend real,
	tempShares real
)


; with idc_dates as (

	-- All dates from IDC tables
	select distinct Code, Date_ from qai.prc.PrcExc where Date_ >= '1993-12-01'
	union
	select distinct Code, Date_ from qai.prc.PrcDly where Date_ >= '1993-12-01'
	union
	select distinct Code, Date_ from qai.prc.PrcVol where Date_ >= '1993-12-01'

)


, exchanges as (

select		Code,
			StartDate,
			coalesce(DATEADD(day, -1, lead(StartDate, 1) over (
				partition by Code 
				order by StartDate)), EndDate, getdate()) as AltEndDate,
			Cusip,
			Ticker,
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


, idc_data as (

select			M.SecCode,
				D.Code,
				I.IsrCode,
				E.Cusip as HistoricalCusip,
				E.Ticker as HistoricalTicker,
				D.Date_,
				DF.DateLagC,
				coalesce(P1.Open_, P2.Open_) as Open_,
				coalesce(P1.High, P2.High) as High,
				coalesce(P1.Low, P2.Low) as Low,
				coalesce(P1.Close_, P2.Close_) as Close_,
				Nullif(P3.Vwap, -99999) as Vwap,
				coalesce(Nullif(P2.Volume, -99999), Nullif(P1.Volume, -99999)) as Volume,

				A.Factor as SplitFactor,
				Isnull(DV.CashDividend, 0) as CashDividend,
				SH.Shares as tempShares

from			idc_dates D

	join		qai.prc.PrcInfo F
		on		D.Code = F.Code
		and		F.SecType = 'C'
		and		F.Issue not like '%UNIT%'

	join		exchanges E
		on		D.Code = E.Code
		and		D.Date_ between E.StartDate and E.AltEndDate
		and		E.Exchange in ('A', 'B', 'C', 'D', 'E', 'F', 'T')	-- U.S. Exchanges

	-- Trading dates lag column for 'NormalTradingFlag'
	join		trading_dates_filter DF
		on		D.Date_ = DF.Date_

	-- IDC Data points
	left join	qai.prc.PrcExc P1
		on		D.Code = P1.Code
		and		D.Date_ = P1.Date_

	left join	qai.prc.PrcDly P2
		on		D.Code = P2.Code
		and		D.Date_ = P2.Date_

	left join	qai.prc.PrcVol P3
		on		D.Code = P3.Code
		and		D.Date_ = P3.Date_

	left join	qai.prc.PrcAdj	A
		on		A.Code = D.Code
		and		A.AdjType = 1
		and		D.Date_ >= A.StartDate
		and		D.Date_ <= A.EndDate

	-- Mapping of SecCode
	join		qai.dbo.SecMapX M
		on		D.Code = M.VenCode
		and		M.Exchange = 1
		and		M.VenType = 1
		and		D.Date_ between M.StartDate and coalesce(M.EndDate, getdate())

	join		qai.dbo.SecMstrX M2
		on		M.SecCode = M2.SecCode
		and		M2.Type_ = 1

	-- Get issuer code, which will identify unique companies
	join		qai.prc.PrcIss I
		on		M.SecCode = I.Code
		and		I.Type_ = 1

	-- Get SIC to filter out investment Funds. See below
	join		qai.prc.PrcIsr I2
		on		I.IsrCode = I2.IsrCode

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

	-- Shares outstanding for MarketCap
	-- IMPROVEMENT: This is where would could add in
	-- data stream functionality because there is missing
	-- share data.
	left join	#idc_shares_table SH
		on		D.Code = SH.Code 
		and		D.Date_ between SH.StartDate and SH.EndDate

where		not (I2.SIC = 0 AND M2.Name like '%FUND%')

)


INSERT INTO
   #idc_dates_table
SELECT
    *
FROM
    idc_data

go


CREATE CLUSTERED INDEX IDX_Code_Date ON #idc_dates_table(SecCode, Date_)



-- ############################################################################################
--  Data Stream data

; with ds_ids1 as (
select			M.SecCode, M.Date_, N.VenCode as DsInfoCode
from			#idc_dates_table M
	left join	qai.dbo.SecMapX N
		on		M.SecCode = N.SecCode
		and		M.Date_ between N.StartDate and N.EndDate
		and		N.Exchange = 1
		and		N.VenType = 33
)


, ds_counts as (
select			SecCode,
				Count(*) as Count_
from			ds_ids1
group by		SecCode, Date_
)


, ds_drop_ids as (
-- Drop values that don't have a DsLocalCode. This handles the incorrect data
-- points in an indirect way, so doesnt guarantee it is perfect. Worked
-- properly when developed/spot checked.
select distinct		D.SecCode, D.DsInfoCode, I.*
from				ds_ids1 D
join				qai.dbo.Ds2CtryQtInfo I
		on			D.DsInfoCode = I.InfoCode
where				SecCode in (select distinct SecCode from ds_counts where Count_ > 1)
	and				DsLocalCode is null
)


, data_merge as (

select				IDC.IsrCode,
					IDC.SecCode,
					DD.DsInfoCode,
					IDC.Code as IdcCode,

					IDC.HistoricalCusip,
					IDC.HistoricalTicker,
					IDC.Date_,
					IDC.DateLagC,

					coalesce(IDC.Open_, P.Open_) as Open_,
					coalesce(IDC.High, P.High) as High,
					coalesce(IDC.Low, P.Low) as Low,
					coalesce(IDC.Close_, P.Close_) as Close_,
					coalesce(IDC.Vwap, P.Vwap) as Vwap,

					IDC.Volume,
					IDC.SplitFactor,
					IDC.CashDividend,
					IDC.tempShares,

					MC.ConsolMktVal as tempMarketCap

from				#idc_dates_table IDC
	join			ds_ids1 DD
		on			DD.Date_ = IDC.Date_
		and			DD.SecCode = IDC.SecCode

	left join		qai.dbo.DS2PrimQtPrc P
		on			DD.DsInfoCode = P.InfoCode
		and			IDC.Date_ = P.MarketDate

	left join		qai.dbo.Ds2MktVal MC
		on			DD.DsInfoCode = MC.InfoCode
		and			IDC.Date_ = MC.ValDate
where				DD.DsInfoCode not in (select DsInfoCode from ds_drop_ids)
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
select 			D.IsrCode,
				D.SecCode,
				D.DsInfoCode,
				D.IdcCode,
				D.HistoricalCusip,
				D.HistoricalTicker,

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

drop table #idc_dates_table
drop table #idc_shares_table
