set NOCOUNT on;

; with pivot_data as (
select		*
from		( select GVKEY, AsOfDate, Value_, ItemName
			  from ram.dbo.ram_compustat_accounting_derived ) d
			pivot
			( max(Value_) for ItemName in (X_GROSSPROFASSET, SHORTLONGDEBT, X_CASHANDSECURITIES, BOOKVALUE,
										   OPERATINGINCOMETTM, FREECASHFLOWTTM, NETINCOMETTM,
										   SALESGROWTHQ, SALESGROWTHTTM, NETINCOMEGROWTHQ, NETINCOMEGROWTHTTM,
										   FREECASHFLOWGROWTHQ, FREECASHFLOWGROWTHTTM,X_GROSSMARGINTTM) ) p
)


select				D.GVKey,
					D.ReportDate,
					A.X_GROSSPROFASSET as ProfAsset,
					P.MarketCap + A.SHORTLONGDEBT - A.X_CASHANDSECURITIES as EnterpriseValue,
					P.MarketCap / nullif(A.BOOKVALUE, 0) as PriceBook,
					A.OPERATINGINCOMETTM as OperatingIncome,
					A.FREECASHFLOWTTM / nullif(A.NETINCOMETTM, 0) as CashFlowNetIncome,
					A.SALESGROWTHQ,
					A.SALESGROWTHTTM,
					A.NETINCOMEGROWTHQ,
					A.NETINCOMEGROWTHTTM,
					A.FREECASHFLOWGROWTHQ,
					A.FREECASHFLOWGROWTHTTM,
					A.X_GROSSMARGINTTM

from				ram.dbo.ram_earnings_report_dates D

	left join		ram.dbo.ram_equity_pricing_research P
		on			D.IdcCode = P.IdcCode
		and			D.FilterDate = P.Date_
	
	left join		pivot_data A
		on			D.GVKey = A.GVKey
		and			A.AsOfDate = (select max(AsOfDate) from pivot_data
								  where GVKey = D.GVKey and AsOfDate < D.FilterDate)

where				D.ResearchFlag = 1