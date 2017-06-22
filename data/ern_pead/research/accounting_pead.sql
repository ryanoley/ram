
set NOCOUNT on;


; with pivot_data as (
select		*
from		( select GVKEY, AsOfDate, Value_, ItemName
			  from ram.dbo.ram_compustat_accounting_derived ) d
			pivot
			( max(Value_) for ItemName in (NETINCOMEQ, NETINCOMETTM,
										   SALESQ, SALESTTM, ADJEPSQ, ADJEPSTTM) ) p
)


, pivot_data_1 as (
select			*,
				Lag(NETINCOMEQ, 3) over (
					partition by GVKey
					order by AsOfDate) as NetIncomeQtrLag,
				Lag(NETINCOMETTM, 3) over (
					partition by GVKey
					order by AsOfDate) as NetIncomeTTLag,
				Lag(SALESQ, 3) over (
					partition by GVKey
					order by AsOfDate) as RevenueQtrLag,
				Lag(SALESTTM, 3) over (
					partition by GVKey
					order by AsOfDate) as RevenueTTMLag,
				Lag(ADJEPSQ, 3) over (
					partition by GVKey
					order by AsOfDate) as DilEPSQtrLag,
				Lag(ADJEPSTTM, 3) over (
					partition by GVKey
					order by AsOfDate) as DilEPSTTMLag

from			pivot_data

)


select				D.SecCode,
					D.ReportDate,

					A.ADJEPSQ as DilEPSQtr,
					A.DilEPSQtrLag,
					A.ADJEPSTTM as DilEPSTTM,
					A.DilEPSTTMLag,

					A.NETINCOMEQ as NetIncomeQtr,
					A.NetIncomeQtrLag,
					A.NETINCOMETTM as NetIncomeTTM,
					A.NetIncomeTTLag,

					A.SALESQ as RevenueQtr,
					A.RevenueQtrLag,
					A.SALESTTM as RevenueTTM,
					A.RevenueTTMLag

from				ram.dbo.ram_pead_report_dates D
	
	left join		pivot_data_1 A
		on			D.GVKey = A.GVKey
		and			A.AsOfDate = (select max(AsOfDate) from pivot_data
								  where GVKey = D.GVKey and AsOfDate < D.FilterDate)

where				D.ResearchFlag = 1
