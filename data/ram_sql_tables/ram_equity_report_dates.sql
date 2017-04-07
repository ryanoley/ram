use ram;

-------------------------------------------------------------
-- Create tables

if object_id('ram.dbo.ram_equity_report_dates', 'U') is not null 
	drop table ram.dbo.ram_equity_report_dates


create table	ram.dbo.ram_equity_report_dates (
		IdcCode int,
		GVKey int,
		QuarterDate smalldatetime,
		ReportDate smalldatetime,
		FiscalQuarter int,
		EarningsReturn real,
		PeadReturnLong real,
		PeadReturnShort real,
		EarningsReturnHedge real,
		PeadReturnLongHedge real,
		PeadReturnShortHedge real
		primary key (IdcCode, GVKey, QuarterDate)
)


; with all_report_dates as (
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSCoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
union
select distinct GVKey, DATADATE, RDQ, FQTR from qai.dbo.CSICoIDesInd
where RDQ is not null
	and DATACQTR is not null
	and DateDiff(day, DATADATE, RDQ) < 92
	and RDQ >= '1985-01-01'
)


, all_report_dates2 as (
select		R.GVKEY,
			R.DATADATE,
			D.T0 as RDQ,
			R.FQTR
from		all_report_dates R
	join	ram.dbo.ram_trading_dates D
	on		D.CalendarDate = R.RDQ
)


, pricing_returns as (
select		P.IdcCode,
			G.GVKey,
			Date_,
			Lag(IsNull(AdjVwap, AdjClose), 1) over (
				partition by SecCode
				order by Date_) as PriceTm1,
			Lead(IsNull(AdjVwap, AdjClose), 1) over (
				partition by SecCode
				order by Date_) as PriceT1,
			Lead(IsNull(AdjVwap, AdjClose), 3) over (
				partition by SecCode
				order by Date_) as PriceT3,
			Lead(IsNull(AdjVwap, AdjClose), 4) over (
				partition by SecCode
				order by Date_) as PriceT4

from		ram.dbo.ram_equity_pricing P
	join	ram.dbo.ram_idccode_to_gvkey_map G
	on		P.IdcCode = G.IdcCode
	and		P.Date_ between G.StartDate and G.EndDate
)


, hedge_returns as (
select		Date_,
			Lag(IsNull(AdjVwap, AdjClose), 1) over (
				partition by SecCode
				order by Date_) as PriceTm1,
			Lead(IsNull(AdjVwap, AdjClose), 1) over (
				partition by SecCode
				order by Date_) as PriceT1,
			Lead(IsNull(AdjVwap, AdjClose), 3) over (
				partition by SecCode
				order by Date_) as PriceT3,
			Lead(IsNull(AdjVwap, AdjClose), 4) over (
				partition by SecCode
				order by Date_) as PriceT4

from		ram.dbo.ram_etf_pricing
where		IdcCode = 59751

)

insert into ram.dbo.ram_equity_report_dates
select			P.IdcCode,
				D.GVKey,
				D.DATADATE,
				D.RDQ,
				D.FQTR,

				P.PriceT1 / P.PriceTm1 - 1 as EarningsReturn,
				P.PriceT4 / P.PriceT1 - 1 as PeadReturnLong,
				P.PriceT3 / P.PriceT1 - 1 as PeadReturnShort,

				H.PriceT1 / H.PriceTm1 - 1 as EarningsReturnHedge,
				H.PriceT4 / H.PriceT1 - 1 as PeadReturnLongHedge,
				H.PriceT3 / H.PriceT1 - 1 as PeadReturnShortHedge

from			all_report_dates2 D
join			pricing_returns P
	on			D.RDQ = P.Date_
	and			D.GVKEY = P.GVKey	
join			hedge_returns H
	on			D.RDQ = H.Date_
