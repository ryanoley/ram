
use ram;

-------------------------------------------------------------
-- Select table to run
if object_id('tempdb..#id_table', 'U') is not null 
	drop table #id_table

create table #id_table
(
    SecCode int,
	DsInfoCode int,
	IdcCode int,
    StartDate datetime,
	EndDate datetime
)

if $(tabletype) = 1
	insert into #id_table select SecCode, DsInfoCode, IdcCode, StartDate, EndDate from ram.dbo.ram_master_ids_etf where ExchangeFlag = 1

if $(tabletype) = 2
	insert into #id_table select SecCode, DsInfoCode, IdcCode, StartDate, EndDate from ram.dbo.ram_master_ids where ExchangeFlag = 1

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.temp_pricing', 'U') is not null 
	drop table ram.dbo.temp_pricing


create table	ram.dbo.temp_pricing (
		SecCode int,
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
		NormalTradingFlag smallint,
		OneYearTradingFlag smallint
		primary key (SecCode, Date_)
)



-- ### IDC DATA ########################################################################

; with idc_dates as (
select distinct Code, Date_ from qai.prc.PrcExc where Date_ >= '1990-12-01'
	and Code in (select distinct IdcCode from #id_table)
union
select distinct Code, Date_ from qai.prc.PrcDly where Date_ >= '1990-12-01'
	and Code in (select distinct IdcCode from #id_table)
union
select distinct Code, Date_ from qai.prc.PrcVol where Date_ >= '1990-12-01'
	and Code in (select distinct IdcCode from #id_table)
)


, idc_data1 as (
select			Code,
				Date_,
				case when Open_ > 0 then Open_ else Null end as Open_,
				case when High > 0 then High else Null end as High,
				case when Low > 0 then Low else Null end as Low,
				case when Close_ > 0 then Close_ else Null end as Close_,
				case when Volume > 0 then Volume else Null end as Volume
from			qai.prc.PrcDly
where			Code in (select distinct IdcCode from #id_table)
)


, idc_data2 as (
select			Code,
				Date_,
				case when Open_ > 0 then Open_ else Null end as Open_,
				case when High > 0 then High else Null end as High,
				case when Low > 0 then Low else Null end as Low,
				case when Close_ > 0 then Close_ else Null end as Close_,
				case when Volume > 0 then Volume else Null end as Volume
from			qai.prc.PrcExc
where			Code in (select distinct IdcCode from #id_table)
)


, idc_data3 as (
select			Code,
				Date_,
				case when Vwap > 0 then Vwap else Null end as Vwap,
				case when AvgTrade > 0 then AvgTrade else Null end as AvgTrade,
				case when NumTrades > 0 then NumTrades else Null end as NumTrades
from			qai.prc.PrcVol
where			Code in (select distinct IdcCode from #id_table)
)


, clean_idc_data1 as (
select				D.Code,
					D.Date_,
					coalesce(P1.Open_, P2.Open_, P3.Vwap) as Open_,
					coalesce(P1.High, P2.High, P3.Vwap) as High,
					coalesce(P1.Low, P2.Low, P3.Vwap) as Low,
					coalesce(P1.Close_, P2.Close_, P3.Vwap) as Close_,
					P3.Vwap,
-					coalesce(P2.Volume, P1.Volume, P3.Vwap * P3.AvgTrade * P3.NumTrades, 0) as Volume,
					S.Shares as tempShares,
					A.Factor as SplitFactor,
					Isnull(DV.CashDividend, 0) as CashDividend

from				idc_dates D

	left join		idc_data1 P1
		on			D.Code = P1.Code
		and			D.Date_ = P1.Date_

	left join		idc_data2 P2
		on			D.Code = P2.Code
		and			D.Date_ = P2.Date_

	left join		idc_data3 P3
		on			D.Code = P3.Code
		and			D.Date_ = P3.Date_

	-- Shares outstanding for market cap calculation
	left join		qai.prc.PrcShr S
		on			D.Code = S.Code and
					S.Date_ = (select max(s.Date_) 
						from qai.prc.PrcShr s
						where s.Code = D.Code
						and s.Date_ <= D.Date_)

	-- Adjustment Factor
	left join	qai.prc.PrcAdj	A
		on		D.Code = A.Code
		and		A.AdjType = 2
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
		on		D.Code = DV.Code
		and		D.Date_ = DV.ExDate
)


, clean_idc_data2 as (
-- Join SecCode and DsInfoCode AND filter with Start and End Dates
select				P.*,
					M.SecCode,
					M.DsInfoCode
from				clean_idc_data1 P
	join			#id_table M
		on			P.Code = M.IdcCode
		and			P.Date_ between M.StartDate and M.EndDate
)



-- ### DATASTREAM DATA ########################################################################

, data_merge as (
select				I.SecCode,
					I.Code as IdcCode,
					I.Date_,
					coalesce(I.Open_, P.Open_) as Open_,
					coalesce(I.High, P.High) as High,
					coalesce(I.Low, P.Low) as Low,
					coalesce(I.Close_, P.Close_) as Close_,
					coalesce(I.Vwap, P.Vwap) as Vwap,

					I.Volume,
					I.CashDividend,

					-- Market Cap calculated here to use coalesced Close_
					coalesce(MC.ConsolMktVal, I.tempShares * coalesce(I.Close_, P.Close_) / 1e6) as MarketCap,
					I.SplitFactor

from				clean_idc_data2 I

	left join		qai.dbo.DS2PrimQtPrc P
		on			I.DsInfoCode = P.InfoCode
		and			I.Date_ = P.MarketDate

	left join		qai.dbo.Ds2MktVal MC
		on			I.DsInfoCode = MC.InfoCode
		and			I.Date_ = MC.ValDate
)


-- ### AGGREGATED FIELDS ########################################################################

, aggregated_data as (
select		*,
			-- Calculation for DividendFactor
			sum(log((1 + isnull(CashDividend, 0) / Close_))) over (
				partition by SecCode 
				order by Date_) as CumRate,
			-- Average Dollar Volume
			avg(isnull(Vwap, Close_) * Volume) over (
				partition by SecCode 
				order by Date_
				rows between 29 preceding and current row) / 1e6 as AvgDolVol,
			-- Date Lag for OneYearTradingFlag
			Lag(Date_, 252) over (
				partition by SecCode 
				order by Date_) as DateLag252
from		data_merge
)


, trading_dates_filter as (
select		Date_,
			Lag(Date_, 252) over (
				order by Date_) as DateLag252
from		(select distinct T0 as Date_ from ram.dbo.trading_dates) a
)


-- ############################################################################################
--  Final Formatting

, final_table as (
select 			D.SecCode,
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

				-- NormalTrading over the past 126 days.
				case 
					when 
						avg(case when D.Volume > 0 then 1.0 else 0.0 end) over (
							partition by D.SecCode order by D.Date_ rows between 125 preceding and current row) = 1
					then 1 else 0 end as NormalTradingFlag,

				case when D.DateLag252 = DF.DateLag252 then 1 else 0 end as OneYearTradingFlag

from			aggregated_data D
	join		trading_dates_filter DF
		on		D.Date_ = DF.Date_

where			D.Date_ >= '1992-01-01'

)

insert into ram.dbo.temp_pricing
select * from final_table

drop table #id_table


create nonclustered index idccode_date on ram.dbo.temp_pricing (IdcCode, Date_)


if $(tabletype) = 1
begin
	if object_id('ram.dbo.ram_etf_pricing', 'U') is not null 
		drop table ram.dbo.ram_etf_pricing
	exec sp_rename 'ram.dbo.temp_pricing', 'ram_etf_pricing'
end

if $(tabletype) = 2
begin
	if object_id('ram.dbo.ram_equity_pricing', 'U') is not null 
		drop table ram.dbo.ram_equity_pricing
	exec sp_rename 'ram.dbo.temp_pricing', 'ram_equity_pricing'
end
