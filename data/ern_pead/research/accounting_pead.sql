
set NOCOUNT on;


-- ######   Temp table   ###############################################

create table #stackeddata
(
    GVKey int,
    AsOfDate smalldatetime,
	ItemName varchar(40),
	Value_ float
	primary key (GVKey, ItemName, AsOfDate)
)
    

; with all_data as (
select		*,
			Lag(Value_, 3) over (
				partition by GVKey, ItemName
				order by AsOfDate) as LagValue_
from		ram.dbo.ram_compustat_accounting_derived
where		ItemName in  ('NETINCOMEQ', 'NETINCOMETTM',
						  'SALESQ', 'SALESTTM', 
						  'ADJEPSQ', 'ADJEPSTTM')
)


, stacked_data as (
select GVKey, AsOfDate, ItemName, Value_ from all_data
union
select GVKey, AsOfDate, ItemName + 'Lag' as ItemName, LagValue_ as Value_ from all_data
)

INSERT INTO #stackeddata
SELECT * from stacked_data



-- ######   Extract data from table   ##################################


; with pivot_data as (

select		*
from		( select GVKEY, AsOfDate, Value_, ItemName
			  from #stackeddata ) d
			pivot
			( max(Value_) for ItemName in (NETINCOMEQ, NETINCOMETTM,
										   SALESQ, SALESTTM, ADJEPSQ, ADJEPSTTM,
										   NETINCOMEQLag, NETINCOMETTMLag,
										   SALESQLag, SALESTTMLag, ADJEPSQLag, ADJEPSTTMLag) ) p
)


select				D.SecCode,
					D.ReportDate,

					A.ADJEPSQ as DilEPSQtr,
					A.ADJEPSQLag as DilEPSQtrLag,
					A.ADJEPSTTM as DilEPSTTM,
					A.ADJEPSTTMLag as DilEPSTTMLag,

					A.NETINCOMEQ as NetIncomeQtr,
					A.NETINCOMEQLag as NetIncomeQtrLag,
					A.NETINCOMETTM as NetIncomeTTM,
					A.NETINCOMETTMLag as NetIncomeTTMLag,

					A.SALESQ as RevenueQtr,
					A.SALESQLag as RevenueQtrLag,
					A.SALESTTM as RevenueTTM,
					A.SALESTTMLag as RevenueTTMLag

from				ram.dbo.ram_pead_report_dates D
	
	left join		pivot_data A
		on			D.GVKey = A.GVKey
		and			A.AsOfDate = (select max(AsOfDate) from pivot_data
								  where GVKey = D.GVKey and AsOfDate < D.FilterDate)

where				D.ResearchFlag = 1
